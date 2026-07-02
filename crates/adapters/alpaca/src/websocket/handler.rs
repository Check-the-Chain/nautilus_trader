// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Inner WebSocket feed handler running on a dedicated tokio task.

use std::{
    collections::VecDeque,
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use dashmap::DashMap;
use nautilus_core::{AtomicTime, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::data::TradeTick;
use nautilus_network::{
    RECONNECTED,
    retry::{RetryManager, create_websocket_retry_manager},
    websocket::{AuthTracker, WebSocketClient},
};
use tokio_tungstenite::tungstenite::Message;
use ustr::Ustr;

use super::{
    error::{AlpacaWsError, create_alpaca_ws_timeout_error, should_retry_alpaca_ws_error},
    messages::{
        AlpacaInstrumentInfo, AlpacaStreamError, AlpacaStreamMessage, AlpacaWsChannel,
        AlpacaWsEvent, AlpacaWsSubscription, NautilusWsMessage,
    },
    parse::{parse_ws_bar, parse_ws_events, parse_ws_quote_tick, parse_ws_trade_tick},
};

/// Success control message text confirming authentication.
const MSG_AUTHENTICATED: &str = "authenticated";
/// Success control message text confirming the transport handshake.
const MSG_CONNECTED: &str = "connected";
/// Authorization envelope status confirming authentication.
const STATUS_AUTHORIZED: &str = "authorized";

/// Venue error codes that terminate an in-flight authentication attempt:
/// 401 not authenticated, 402 auth failed, 404 auth timeout.
const AUTH_FAILURE_CODES: [u16; 3] = [401, 402, 404];
/// Venue error code meaning a redundant (already satisfied) authentication.
const AUTH_ALREADY_AUTHENTICATED: u16 = 403;

/// Which stream protocol the handler speaks.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum FeedKind {
    /// Market data stream (`"T"`-tagged JSON arrays).
    MarketData,
    /// Trade-updates stream (`{"stream","data"}` envelopes).
    TradeUpdates,
}

/// Commands sent from the outer clients to the inner feed handler.
pub enum HandlerCommand {
    /// Hand the live `WebSocketClient` to the handler after the outer client
    /// completes the network connect.
    SetClient(WebSocketClient),
    /// Drain the queue and shut the handler down.
    Disconnect,
    /// Send an authentication payload.
    Authenticate {
        /// Serialized auth message (contains the API secret).
        payload: String,
    },
    /// Subscribe `symbols` on a market data `channel`.
    Subscribe {
        channel: AlpacaWsChannel,
        symbols: Vec<Ustr>,
    },
    /// Unsubscribe `symbols` from a market data `channel`.
    Unsubscribe {
        channel: AlpacaWsChannel,
        symbols: Vec<Ustr>,
    },
    /// Send a pre-serialized payload (e.g. the trade-updates listen frame).
    Send {
        /// Serialized message.
        payload: String,
    },
}

impl Debug for HandlerCommand {
    /// Custom `Debug` that redacts the `Authenticate` payload, which embeds
    /// the API secret key.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SetClient(_) => f.write_str("SetClient(<WebSocketClient>)"),
            Self::Disconnect => f.write_str("Disconnect"),
            Self::Authenticate { .. } => f
                .debug_struct(stringify!(Authenticate))
                .field("payload", &"<redacted>")
                .finish(),
            Self::Subscribe { channel, symbols } => f
                .debug_struct(stringify!(Subscribe))
                .field("channel", channel)
                .field("symbols", symbols)
                .finish(),
            Self::Unsubscribe { channel, symbols } => f
                .debug_struct(stringify!(Unsubscribe))
                .field("channel", channel)
                .field("symbols", symbols)
                .finish(),
            Self::Send { payload } => f
                .debug_struct(stringify!(Send))
                .field("payload", payload)
                .finish(),
        }
    }
}

/// Inner feed handler. Owns the [`WebSocketClient`] exclusively and routes
/// raw frames into typed [`NautilusWsMessage`] values in a single parse pass.
pub(super) struct FeedHandler {
    clock: &'static AtomicTime,
    kind: FeedKind,
    signal: Arc<AtomicBool>,
    inner: Option<WebSocketClient>,
    cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
    raw_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
    out_tx: tokio::sync::mpsc::UnboundedSender<NautilusWsMessage>,
    auth_tracker: AuthTracker,
    instruments: Arc<DashMap<Ustr, AlpacaInstrumentInfo>>,
    retry_manager: RetryManager<AlpacaWsError>,
    pending_messages: VecDeque<NautilusWsMessage>,
}

impl FeedHandler {
    pub(super) fn new(
        kind: FeedKind,
        signal: Arc<AtomicBool>,
        cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
        raw_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
        out_tx: tokio::sync::mpsc::UnboundedSender<NautilusWsMessage>,
        auth_tracker: AuthTracker,
        instruments: Arc<DashMap<Ustr, AlpacaInstrumentInfo>>,
    ) -> Self {
        Self {
            clock: get_atomic_clock_realtime(),
            kind,
            signal,
            inner: None,
            cmd_rx,
            raw_rx,
            out_tx,
            auth_tracker,
            instruments,
            retry_manager: create_websocket_retry_manager(),
            pending_messages: VecDeque::new(),
        }
    }

    pub(super) fn send(&self, msg: NautilusWsMessage) -> Result<(), String> {
        self.out_tx
            .send(msg)
            .map_err(|e| format!("Failed to send message: {e}"))
    }

    pub(super) fn is_stopped(&self) -> bool {
        self.signal.load(Ordering::Relaxed)
    }

    pub(super) async fn next(&mut self) -> Option<NautilusWsMessage> {
        if let Some(msg) = self.pending_messages.pop_front() {
            return Some(msg);
        }

        loop {
            tokio::select! {
                Some(cmd) = self.cmd_rx.recv() => {
                    match cmd {
                        HandlerCommand::SetClient(client) => {
                            log::debug!("Setting WebSocket client in Alpaca handler");
                            self.inner = Some(client);
                        }
                        HandlerCommand::Disconnect => {
                            log::debug!("Alpaca handler received disconnect");
                            if let Some(ref client) = self.inner {
                                client.disconnect().await;
                            }
                            self.signal.store(true, Ordering::SeqCst);
                            return None;
                        }
                        HandlerCommand::Authenticate { payload } => {
                            if let Err(e) = self.send_with_retry(payload).await {
                                log::error!("Error sending Alpaca auth message: {e}");
                                self.auth_tracker.fail(e.to_string());
                            }
                        }
                        HandlerCommand::Subscribe { channel, symbols } => {
                            self.dispatch_subscription(
                                AlpacaWsSubscription::subscribe(channel, symbols),
                            )
                            .await;
                        }
                        HandlerCommand::Unsubscribe { channel, symbols } => {
                            self.dispatch_subscription(
                                AlpacaWsSubscription::unsubscribe(channel, symbols),
                            )
                            .await;
                        }
                        HandlerCommand::Send { payload } => {
                            if let Err(e) = self.send_with_retry(payload).await {
                                log::error!("Error sending Alpaca message: {e}");
                            }
                        }
                    }
                }
                () = tokio::time::sleep(Duration::from_millis(100)) => {
                    if self.signal.load(Ordering::Acquire) {
                        return None;
                    }
                }
                Some(raw_msg) = self.raw_rx.recv() => {
                    match raw_msg {
                        Message::Text(text) => {
                            if text == RECONNECTED {
                                log::debug!("Received Alpaca WebSocket RECONNECTED sentinel");
                                self.auth_tracker.invalidate();
                                return Some(NautilusWsMessage::Reconnected);
                            }
                            if let Some(first) = self.route_payload(text.as_bytes()) {
                                return Some(first);
                            }
                        }
                        // The paper trade-updates endpoint sends binary frames
                        // carrying the same JSON payloads.
                        Message::Binary(data) => {
                            if let Some(first) = self.route_payload(&data) {
                                return Some(first);
                            }
                        }
                        Message::Ping(data) => {
                            if let Some(ref client) = self.inner
                                && let Err(e) = client.send_pong(data.to_vec()).await
                            {
                                log::error!("Error sending Alpaca pong: {e}");
                            }
                        }
                        Message::Close(frame) => {
                            log::debug!("Received Alpaca WebSocket close frame: {frame:?}");
                            return None;
                        }
                        _ => {}
                    }
                }
                else => {
                    log::debug!(
                        "Alpaca handler shutting down: stream ended or command channel closed"
                    );
                    return None;
                }
            }
        }
    }

    fn route_payload(&mut self, raw: &[u8]) -> Option<NautilusWsMessage> {
        let messages = match self.kind {
            FeedKind::MarketData => {
                let ts_init = self.clock.get_time_ns();
                match parse_ws_events(raw) {
                    Ok(events) => self.handle_market_events(&events, ts_init),
                    Err(e) => {
                        log::warn!("Failed to parse Alpaca market data frame: {e}");
                        Vec::new()
                    }
                }
            }
            FeedKind::TradeUpdates => self.handle_stream_payload(raw),
        };
        self.dispatch_results(messages)
    }

    fn dispatch_results(
        &mut self,
        mut messages: Vec<NautilusWsMessage>,
    ) -> Option<NautilusWsMessage> {
        if messages.is_empty() {
            return None;
        }
        let first = messages.remove(0);
        for extra in messages {
            self.pending_messages.push_back(extra);
        }
        Some(first)
    }

    /// Converts one parsed market data payload into output messages.
    ///
    /// Trades within a payload batch into a single
    /// [`NautilusWsMessage::Trades`] emitted ahead of any other converted
    /// values, preserving the venue's intra-payload ordering guarantees for
    /// consumers that only care about prints.
    fn handle_market_events(
        &self,
        events: &[AlpacaWsEvent<'_>],
        ts_init: UnixNanos,
    ) -> Vec<NautilusWsMessage> {
        let mut messages: Vec<NautilusWsMessage> = Vec::new();
        let mut trades: Vec<TradeTick> = Vec::new();

        for event in events {
            match event {
                AlpacaWsEvent::Trade(trade) => {
                    let Some(info) = self.instruments.get(&trade.symbol) else {
                        log::debug!("No instrument mapped for Alpaca symbol {}", trade.symbol);
                        continue;
                    };
                    match parse_ws_trade_tick(trade, info.value(), ts_init) {
                        Ok(tick) => trades.push(tick),
                        Err(e) => log::warn!("Failed to parse Alpaca trade: {e}"),
                    }
                }
                AlpacaWsEvent::Quote(quote) => {
                    let Some(info) = self.instruments.get(&quote.symbol) else {
                        log::debug!("No instrument mapped for Alpaca symbol {}", quote.symbol);
                        continue;
                    };
                    match parse_ws_quote_tick(quote, info.value(), ts_init) {
                        Ok(tick) => messages.push(NautilusWsMessage::Quote(tick)),
                        Err(e) => log::warn!("Failed to parse Alpaca quote: {e}"),
                    }
                }
                AlpacaWsEvent::MinuteBar(bar) => {
                    let Some(info) = self.instruments.get(&bar.symbol) else {
                        log::debug!("No instrument mapped for Alpaca symbol {}", bar.symbol);
                        continue;
                    };
                    match parse_ws_bar(bar, info.value(), ts_init) {
                        Ok(bar) => messages.push(NautilusWsMessage::Bar(bar)),
                        Err(e) => log::warn!("Failed to parse Alpaca bar: {e}"),
                    }
                }
                // Daily bars are cumulative intraday snapshots and updated
                // bars revise already-emitted minutes; neither maps onto an
                // immutable Nautilus `Bar` (see `parse_ws_bar` docs).
                AlpacaWsEvent::DailyBar(bar) | AlpacaWsEvent::UpdatedBar(bar) => {
                    log::trace!("Skipping Alpaca daily/updated bar for {}", bar.symbol);
                }
                AlpacaWsEvent::Success(success) => match success.msg.as_str() {
                    MSG_CONNECTED => log::debug!("Alpaca WebSocket handshake complete"),
                    MSG_AUTHENTICATED => {
                        log::debug!("Alpaca WebSocket authenticated");
                        self.auth_tracker.succeed();
                        messages.push(NautilusWsMessage::Authenticated);
                    }
                    other => log::debug!("Alpaca WebSocket success message: {other}"),
                },
                AlpacaWsEvent::Error(error) => {
                    log::warn!(
                        "Alpaca WebSocket error frame: code={:?} msg={}",
                        error.code,
                        error.msg,
                    );
                    match error.code {
                        Some(code) if AUTH_FAILURE_CODES.contains(&code) => {
                            self.auth_tracker.fail(error.msg.clone());
                        }
                        Some(AUTH_ALREADY_AUTHENTICATED) => self.auth_tracker.succeed(),
                        _ => {}
                    }
                    messages.push(NautilusWsMessage::Error {
                        code: error.code,
                        msg: error.msg.clone(),
                    });
                }
                AlpacaWsEvent::Subscription(ack) => {
                    messages.push(NautilusWsMessage::SubscriptionAck(ack.clone()));
                }
                AlpacaWsEvent::Unknown => {
                    log::debug!("Ignoring unrecognized Alpaca market data message");
                }
            }
        }

        if !trades.is_empty() {
            messages.insert(0, NautilusWsMessage::Trades(trades));
        }
        messages
    }

    /// Converts one trade-updates envelope into output messages.
    fn handle_stream_payload(&self, raw: &[u8]) -> Vec<NautilusWsMessage> {
        match serde_json::from_slice::<AlpacaStreamMessage>(raw) {
            Ok(AlpacaStreamMessage::Authorization(auth)) => {
                if auth.status == STATUS_AUTHORIZED {
                    log::debug!("Alpaca trade-updates stream authorized");
                    self.auth_tracker.succeed();
                    vec![NautilusWsMessage::Authenticated]
                } else {
                    let msg = format!("trade-updates authorization failed: {}", auth.status);
                    log::error!("{msg}");
                    self.auth_tracker.fail(msg.clone());
                    vec![NautilusWsMessage::Error { code: None, msg }]
                }
            }
            Ok(AlpacaStreamMessage::Listening(data)) => {
                log::debug!("Alpaca trade-updates listening: {:?}", data.streams);
                Vec::new()
            }
            Ok(AlpacaStreamMessage::TradeUpdate(update)) => {
                vec![NautilusWsMessage::TradeUpdate(update)]
            }
            Err(_) => {
                if let Ok(error) = serde_json::from_slice::<AlpacaStreamError>(raw) {
                    log::error!(
                        "Alpaca trade-updates stream error: {}",
                        error.data.error_message,
                    );
                    vec![NautilusWsMessage::Error {
                        code: None,
                        msg: error.data.error_message,
                    }]
                } else {
                    log::warn!(
                        "Unparsed Alpaca trade-updates frame: {}",
                        String::from_utf8_lossy(raw),
                    );
                    Vec::new()
                }
            }
        }
    }

    async fn dispatch_subscription(&self, request: AlpacaWsSubscription) {
        match serde_json::to_string(&request) {
            Ok(payload) => {
                log::debug!("Sending Alpaca subscription request: {payload}");
                if let Err(e) = self.send_with_retry(payload).await {
                    log::error!("Error sending Alpaca subscription request: {e}");
                }
            }
            Err(e) => log::error!("Error serializing Alpaca subscription request: {e}"),
        }
    }

    async fn send_with_retry(&self, payload: String) -> Result<(), AlpacaWsError> {
        if let Some(client) = &self.inner {
            self.retry_manager
                .execute_with_retry(
                    "websocket_send",
                    || {
                        let payload = payload.clone();
                        async move {
                            client
                                .send_text(payload, None)
                                .await
                                .map_err(AlpacaWsError::Transport)
                        }
                    },
                    should_retry_alpaca_ws_error,
                    create_alpaca_ws_timeout_error,
                )
                .await
        } else {
            Err(AlpacaWsError::Client(
                "no active WebSocket client".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::{InstrumentId, Symbol};
    use rstest::rstest;

    use super::*;
    use crate::common::{
        consts::ALPACA_VENUE, enums::AlpacaTradeUpdateEvent, testing::load_test_json,
    };

    fn test_handler(kind: FeedKind) -> FeedHandler {
        let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let (_raw_tx, raw_rx) = tokio::sync::mpsc::unbounded_channel();
        let (out_tx, _out_rx) = tokio::sync::mpsc::unbounded_channel();
        let instruments = Arc::new(DashMap::new());

        for symbol in ["AAPL", "MSFT"] {
            let key = Ustr::from(symbol);
            instruments.insert(
                key,
                AlpacaInstrumentInfo {
                    instrument_id: InstrumentId::new(Symbol::new(symbol), *ALPACA_VENUE),
                    price_precision: 2,
                    size_precision: 0,
                },
            );
        }

        FeedHandler::new(
            kind,
            Arc::new(AtomicBool::new(false)),
            cmd_rx,
            raw_rx,
            out_tx,
            AuthTracker::new(),
            instruments,
        )
    }

    #[rstest]
    fn test_route_market_data_batch_orders_trades_first() {
        let mut handler = test_handler(FeedKind::MarketData);
        let json = load_test_json("ws_market_data_batch.json");

        let first = handler.route_payload(json.as_bytes()).expect("messages");

        match first {
            NautilusWsMessage::Trades(trades) => assert_eq!(trades.len(), 2),
            other => panic!("expected batched trades first, got {other:?}"),
        }
        // Two quotes and one bar remain queued.
        assert_eq!(handler.pending_messages.len(), 3);
        assert!(matches!(
            handler.pending_messages[0],
            NautilusWsMessage::Quote(_)
        ));
        assert!(matches!(
            handler.pending_messages[2],
            NautilusWsMessage::Bar(_)
        ));
    }

    #[rstest]
    fn test_route_authenticated_success_updates_tracker() {
        let mut handler = test_handler(FeedKind::MarketData);
        let json = load_test_json("ws_authenticated.json");

        let first = handler.route_payload(json.as_bytes()).expect("message");

        assert!(matches!(first, NautilusWsMessage::Authenticated));
        assert!(handler.auth_tracker.is_authenticated());
    }

    #[rstest]
    fn test_route_connected_success_is_silent() {
        let mut handler = test_handler(FeedKind::MarketData);
        let json = load_test_json("ws_connected.json");

        assert!(handler.route_payload(json.as_bytes()).is_none());
        assert!(!handler.auth_tracker.is_authenticated());
    }

    #[rstest]
    fn test_route_error_frame_maps_code() {
        let mut handler = test_handler(FeedKind::MarketData);
        let json = load_test_json("ws_error_406.json");

        let first = handler.route_payload(json.as_bytes()).expect("message");

        assert!(matches!(
            first,
            NautilusWsMessage::Error {
                code: Some(406),
                ..
            }
        ));
    }

    #[rstest]
    fn test_route_auth_failure_error_fails_tracker() {
        let mut handler = test_handler(FeedKind::MarketData);
        let json = r#"[{"T":"error","code":402,"msg":"auth failed"}]"#;

        let first = handler.route_payload(json.as_bytes()).expect("message");

        assert!(matches!(first, NautilusWsMessage::Error { .. }));
        assert!(!handler.auth_tracker.is_authenticated());
    }

    #[rstest]
    fn test_route_subscription_ack() {
        let mut handler = test_handler(FeedKind::MarketData);
        let json = load_test_json("ws_subscription_ack.json");

        let first = handler.route_payload(json.as_bytes()).expect("message");

        match first {
            NautilusWsMessage::SubscriptionAck(ack) => {
                assert_eq!(ack.trades, vec![Ustr::from("AAPL")]);
            }
            other => panic!("expected subscription ack, got {other:?}"),
        }
    }

    #[rstest]
    fn test_route_unknown_symbol_is_skipped() {
        let mut handler = test_handler(FeedKind::MarketData);
        let json = r#"[{"T":"t","S":"ZZZZ","i":1,"x":"V","p":1.0,"s":1,"c":[],"z":"C","t":"2026-01-05T14:30:00Z"}]"#;

        assert!(handler.route_payload(json.as_bytes()).is_none());
    }

    #[rstest]
    fn test_route_trade_updates_authorization() {
        let mut handler = test_handler(FeedKind::TradeUpdates);
        let json = load_test_json("ws_trade_updates_authorization.json");

        let first = handler.route_payload(json.as_bytes()).expect("message");

        assert!(matches!(first, NautilusWsMessage::Authenticated));
        assert!(handler.auth_tracker.is_authenticated());
    }

    #[rstest]
    fn test_route_trade_updates_fill_event() {
        let mut handler = test_handler(FeedKind::TradeUpdates);
        let json = load_test_json("ws_trade_updates_fill.json");

        let first = handler.route_payload(json.as_bytes()).expect("message");

        match first {
            NautilusWsMessage::TradeUpdate(update) => {
                assert_eq!(update.event, AlpacaTradeUpdateEvent::Fill);
                assert_eq!(update.price.as_deref(), Some("189.05"));
                assert_eq!(update.order.symbol.as_deref(), Some("AAPL"));
            }
            other => panic!("expected trade update, got {other:?}"),
        }
    }

    #[rstest]
    fn test_route_trade_updates_stream_error() {
        let mut handler = test_handler(FeedKind::TradeUpdates);
        let json = r#"{"action":"error","data":{"error_message":"internal error"}}"#;

        let first = handler.route_payload(json.as_bytes()).expect("message");

        assert!(
            matches!(first, NautilusWsMessage::Error { code: None, msg } if msg == "internal error")
        );
    }
}
