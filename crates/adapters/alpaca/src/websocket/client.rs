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

//! Outer WebSocket clients orchestrating connection lifecycle and subscriptions.

use std::{
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::Duration,
};

use ahash::AHashSet;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use nautilus_common::live::get_runtime;
use nautilus_model::identifiers::InstrumentId;
use nautilus_network::{
    mode::ConnectionMode,
    websocket::{
        AUTHENTICATION_TIMEOUT_SECS, AuthTracker, TransportBackend, WebSocketClient,
        WebSocketConfig, channel_message_handler,
    },
};
use ustr::Ustr;

use crate::{
    common::{
        consts::{
            HEARTBEAT_INTERVAL, RECONNECT_BACKOFF_FACTOR, RECONNECT_DELAY_INITIAL,
            RECONNECT_DELAY_MAX, RECONNECT_JITTER, RECONNECT_TIMEOUT,
        },
        credential::Credential,
        enums::{AlpacaDataFeed, AlpacaEnvironment},
        urls::{alpaca_stocks_stream_ws_url, alpaca_trade_updates_ws_url},
    },
    websocket::{
        error::AlpacaWsError,
        handler::{FeedHandler, FeedKind, HandlerCommand},
        messages::{
            AlpacaInstrumentInfo, AlpacaSubscriptionAck, AlpacaWsAuth, AlpacaWsChannel,
            AlpacaWsListen, NautilusWsMessage,
        },
    },
};

const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Outer Alpaca market data WebSocket client.
///
/// Orchestrates the connection lifecycle, authentication, and subscription
/// bookkeeping for the equities market data stream. The inner feed handler
/// runs on a dedicated tokio task and exclusively owns the underlying
/// [`WebSocketClient`]; this outer type communicates with it through a
/// command channel and consumes typed [`NautilusWsMessage`] events over an
/// unbounded mpsc.
///
/// On reconnect the spawned task re-authenticates and replays the full
/// tracked subscription set; the venue's subscription acknowledgements
/// (which always carry the complete server-side set) reconcile the tracked
/// state afterwards.
pub struct AlpacaWebSocketClient {
    url: String,
    credential: Credential,
    auth_timeout_secs: u64,
    auth_tracker: AuthTracker,
    signal: Arc<AtomicBool>,
    connection_mode: Arc<ArcSwap<AtomicU8>>,
    cmd_tx: Arc<tokio::sync::RwLock<tokio::sync::mpsc::UnboundedSender<HandlerCommand>>>,
    out_rx: Option<tokio::sync::mpsc::UnboundedReceiver<NautilusWsMessage>>,
    subscriptions: Arc<DashMap<AlpacaWsChannel, AHashSet<Ustr>>>,
    instruments: Arc<DashMap<Ustr, AlpacaInstrumentInfo>>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
    transport_backend: TransportBackend,
    proxy_url: Option<String>,
}

impl Debug for AlpacaWebSocketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AlpacaWebSocketClient))
            .field("url", &self.url)
            .field("is_active", &self.is_active())
            .field("subscription_count", &self.subscription_count())
            .field("instruments_len", &self.instruments.len())
            .field("transport_backend", &self.transport_backend)
            .field("proxy_url", &self.proxy_url)
            .finish_non_exhaustive()
    }
}

impl Clone for AlpacaWebSocketClient {
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            credential: self.credential.clone(),
            auth_timeout_secs: self.auth_timeout_secs,
            auth_tracker: self.auth_tracker.clone(),
            signal: Arc::clone(&self.signal),
            connection_mode: Arc::clone(&self.connection_mode),
            cmd_tx: Arc::clone(&self.cmd_tx),
            out_rx: None,
            subscriptions: Arc::clone(&self.subscriptions),
            instruments: Arc::clone(&self.instruments),
            task_handle: None,
            transport_backend: self.transport_backend,
            proxy_url: self.proxy_url.clone(),
        }
    }
}

impl AlpacaWebSocketClient {
    /// Creates a new client without connecting.
    ///
    /// `url` overrides the resolved feed URL when supplied.
    #[must_use]
    pub fn new(
        url: Option<String>,
        data_feed: AlpacaDataFeed,
        credential: Credential,
        transport_backend: TransportBackend,
        proxy_url: Option<String>,
    ) -> Self {
        let url = url.unwrap_or_else(|| alpaca_stocks_stream_ws_url(data_feed).to_string());
        let (placeholder_tx, _) = tokio::sync::mpsc::unbounded_channel();

        Self {
            url,
            credential,
            auth_timeout_secs: AUTHENTICATION_TIMEOUT_SECS,
            auth_tracker: AuthTracker::new(),
            signal: Arc::new(AtomicBool::new(false)),
            connection_mode: Arc::new(ArcSwap::new(Arc::new(AtomicU8::new(
                ConnectionMode::Closed as u8,
            )))),
            cmd_tx: Arc::new(tokio::sync::RwLock::new(placeholder_tx)),
            out_rx: None,
            subscriptions: Arc::new(DashMap::new()),
            instruments: Arc::new(DashMap::new()),
            task_handle: None,
            transport_backend,
            proxy_url,
        }
    }

    /// Returns the resolved WebSocket URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns `true` when the underlying connection is active.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.connection_mode.load().load(Ordering::Relaxed) == ConnectionMode::Active as u8
    }

    /// Returns the count of tracked symbol subscriptions across channels.
    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.subscriptions
            .iter()
            .map(|entry| entry.value().len())
            .sum()
    }

    /// Waits until the underlying connection reports active, or returns an
    /// error after `timeout_secs`.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaWsError::Timeout`] if the connection does not reach
    /// the active state within `timeout_secs`.
    pub async fn wait_until_active(&self, timeout_secs: f64) -> Result<(), AlpacaWsError> {
        let timeout = Duration::from_secs_f64(timeout_secs);

        tokio::time::timeout(timeout, async {
            while !self.is_active() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| {
            AlpacaWsError::Timeout(format!(
                "WebSocket connection timeout after {timeout_secs} seconds"
            ))
        })
    }

    /// Replaces the instrument map used to build ticks from wire messages.
    pub fn initialize_instruments(&self, instruments: Vec<AlpacaInstrumentInfo>) {
        self.instruments.clear();
        for info in instruments {
            self.instruments
                .insert(info.instrument_id.symbol.inner(), info);
        }
        log::debug!(
            "Alpaca instrument map initialized with {} instruments",
            self.instruments.len()
        );
    }

    /// Inserts or replaces a single instrument mapping.
    pub fn add_instrument(&self, info: AlpacaInstrumentInfo) {
        self.instruments
            .insert(info.instrument_id.symbol.inner(), info);
    }

    /// Establishes the WebSocket connection, spawns the feed-handler task,
    /// and authenticates.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying [`WebSocketClient::connect`] fails,
    /// the credential secret is invalid, or authentication fails or times
    /// out.
    pub async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_active() {
            log::warn!("Alpaca WebSocket already connected");
            return Ok(());
        }

        let auth_payload = self.build_auth_payload()?;
        self.signal.store(false, Ordering::Release);

        let (message_handler, raw_rx) = channel_message_handler();
        let cfg = WebSocketConfig {
            url: self.url.clone(),
            headers: vec![],
            heartbeat: Some(HEARTBEAT_INTERVAL.as_secs()),
            heartbeat_msg: None,
            reconnect_timeout_ms: Some(RECONNECT_TIMEOUT.as_millis() as u64),
            reconnect_delay_initial_ms: Some(RECONNECT_DELAY_INITIAL.as_millis() as u64),
            reconnect_delay_max_ms: Some(RECONNECT_DELAY_MAX.as_millis() as u64),
            reconnect_backoff_factor: Some(RECONNECT_BACKOFF_FACTOR),
            reconnect_jitter_ms: Some(RECONNECT_JITTER.as_millis() as u64),
            reconnect_max_attempts: None,
            idle_timeout_ms: None,
            backend: self.transport_backend,
            proxy_url: self.proxy_url.clone(),
        };
        let client =
            WebSocketClient::connect(cfg, Some(message_handler), None, None, vec![], None).await?;

        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel::<HandlerCommand>();
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel::<NautilusWsMessage>();
        let connection_mode_atomic = client.connection_mode_atomic();

        // Queue SetClient onto the new command channel BEFORE publishing it,
        // so a clone cannot race a Subscribe ahead of it.
        if let Err(e) = cmd_tx.send(HandlerCommand::SetClient(client)) {
            anyhow::bail!("Failed to send SetClient command: {e}");
        }

        *self.cmd_tx.write().await = cmd_tx.clone();
        self.out_rx = Some(out_rx);
        self.connection_mode.store(connection_mode_atomic);

        log::debug!("Alpaca WebSocket connected: {}", self.url);

        let signal = Arc::clone(&self.signal);
        let auth_tracker = self.auth_tracker.clone();
        let instruments = Arc::clone(&self.instruments);
        let subscriptions = Arc::clone(&self.subscriptions);
        let cmd_tx_for_reconnect = cmd_tx.clone();

        let task = get_runtime().spawn(async move {
            let mut handler = FeedHandler::new(
                FeedKind::MarketData,
                signal,
                cmd_rx,
                raw_rx,
                out_tx,
                auth_tracker,
                instruments,
            );

            let restore_subscriptions = || {
                // Re-authenticate first: the venue processes messages on one
                // connection in order, so queuing subscribes right behind the
                // auth frame is race-free.
                if let Err(e) = cmd_tx_for_reconnect.send(HandlerCommand::Authenticate {
                    payload: auth_payload.clone(),
                }) {
                    log::error!("Failed to resend Alpaca auth command: {e}");
                    return;
                }

                for entry in subscriptions.iter() {
                    let symbols: Vec<Ustr> = entry.value().iter().copied().collect();
                    if symbols.is_empty() {
                        continue;
                    }
                    log::debug!(
                        "Restoring {} Alpaca {} subscriptions after reconnect",
                        symbols.len(),
                        entry.key(),
                    );
                    if let Err(e) = cmd_tx_for_reconnect.send(HandlerCommand::Subscribe {
                        channel: *entry.key(),
                        symbols,
                    }) {
                        log::error!("Failed to resend Alpaca subscribe command: {e}");
                    }
                }
            };

            loop {
                match handler.next().await {
                    Some(NautilusWsMessage::Reconnected) => {
                        log::debug!("Alpaca WebSocket reconnected");
                        restore_subscriptions();

                        if handler.send(NautilusWsMessage::Reconnected).is_err() {
                            log_forward_failure(handler.is_stopped());
                            break;
                        }
                    }
                    Some(NautilusWsMessage::SubscriptionAck(ack)) => {
                        // Acks carry the FULL server-side set; adopt it as truth.
                        reconcile_subscriptions(&subscriptions, &ack);

                        if handler
                            .send(NautilusWsMessage::SubscriptionAck(ack))
                            .is_err()
                        {
                            log_forward_failure(handler.is_stopped());
                            break;
                        }
                    }
                    Some(msg) => {
                        if handler.send(msg).is_err() {
                            log_forward_failure(handler.is_stopped());
                            break;
                        }
                    }
                    None => {
                        if handler.is_stopped() {
                            log::debug!("Alpaca handler stop signal observed, exiting loop");
                        } else {
                            log::warn!("Alpaca WebSocket stream ended unexpectedly");
                        }
                        break;
                    }
                }
            }
            log::debug!("Alpaca handler task completed");
        });
        self.task_handle = Some(task);

        self.authenticate().await
    }

    /// Disconnects gracefully: signals shutdown, drains the handler, then
    /// awaits the task handle with a timeout.
    ///
    /// # Errors
    ///
    /// This function currently completes best-effort shutdown and returns `Ok(())`.
    pub async fn disconnect(&mut self) -> Result<(), AlpacaWsError> {
        log::debug!("Disconnecting Alpaca WebSocket");

        if let Err(e) = self.cmd_tx.read().await.send(HandlerCommand::Disconnect) {
            log::debug!("Failed to send Alpaca disconnect command: {e}");
        }
        self.signal.store(true, Ordering::Release);

        if let Some(handle) = self.task_handle.take() {
            await_task_shutdown(handle).await;
        }

        self.connection_mode
            .store(Arc::new(AtomicU8::new(ConnectionMode::Closed as u8)));
        self.auth_tracker.invalidate();
        Ok(())
    }

    /// Receives the next message from the handler, or `None` if the receiver
    /// has been taken or the handler has shut down.
    pub async fn next_event(&mut self) -> Option<NautilusWsMessage> {
        if let Some(rx) = self.out_rx.as_mut() {
            rx.recv().await
        } else {
            None
        }
    }

    /// Takes ownership of the output receiver, leaving `None` behind.
    ///
    /// Used by the data client to consume the stream on its own task.
    #[must_use]
    pub fn take_receiver(
        &mut self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<NautilusWsMessage>> {
        self.out_rx.take()
    }

    /// Subscribe to trade prints for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be queued.
    pub async fn subscribe_trades(&self, instrument_id: InstrumentId) -> Result<(), AlpacaWsError> {
        self.subscribe(AlpacaWsChannel::Trades, instrument_id).await
    }

    /// Unsubscribe from trade prints.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be queued.
    pub async fn unsubscribe_trades(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), AlpacaWsError> {
        self.unsubscribe(AlpacaWsChannel::Trades, instrument_id)
            .await
    }

    /// Subscribe to NBBO quotes for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be queued.
    pub async fn subscribe_quotes(&self, instrument_id: InstrumentId) -> Result<(), AlpacaWsError> {
        self.subscribe(AlpacaWsChannel::Quotes, instrument_id).await
    }

    /// Unsubscribe from NBBO quotes.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be queued.
    pub async fn unsubscribe_quotes(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<(), AlpacaWsError> {
        self.unsubscribe(AlpacaWsChannel::Quotes, instrument_id)
            .await
    }

    /// Subscribe to 1-minute bars for an instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be queued.
    pub async fn subscribe_bars(&self, instrument_id: InstrumentId) -> Result<(), AlpacaWsError> {
        self.subscribe(AlpacaWsChannel::Bars, instrument_id).await
    }

    /// Unsubscribe from 1-minute bars.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be queued.
    pub async fn unsubscribe_bars(&self, instrument_id: InstrumentId) -> Result<(), AlpacaWsError> {
        self.unsubscribe(AlpacaWsChannel::Bars, instrument_id).await
    }

    async fn subscribe(
        &self,
        channel: AlpacaWsChannel,
        instrument_id: InstrumentId,
    ) -> Result<(), AlpacaWsError> {
        let symbol = instrument_id.symbol.inner();
        self.subscriptions
            .entry(channel)
            .or_default()
            .insert(symbol);

        if let Err(e) = self
            .send_cmd(HandlerCommand::Subscribe {
                channel,
                symbols: vec![symbol],
            })
            .await
        {
            if let Some(mut entry) = self.subscriptions.get_mut(&channel) {
                entry.value_mut().remove(&symbol);
            }
            return Err(e);
        }
        Ok(())
    }

    async fn unsubscribe(
        &self,
        channel: AlpacaWsChannel,
        instrument_id: InstrumentId,
    ) -> Result<(), AlpacaWsError> {
        let symbol = instrument_id.symbol.inner();
        self.send_cmd(HandlerCommand::Unsubscribe {
            channel,
            symbols: vec![symbol],
        })
        .await?;

        if let Some(mut entry) = self.subscriptions.get_mut(&channel) {
            entry.value_mut().remove(&symbol);
        }
        Ok(())
    }

    async fn authenticate(&self) -> anyhow::Result<()> {
        let payload = self.build_auth_payload()?;
        let rx = self.auth_tracker.begin();

        self.cmd_tx
            .read()
            .await
            .send(HandlerCommand::Authenticate { payload })
            .map_err(|e| anyhow::anyhow!("Failed to send authenticate command: {e}"))?;

        self.auth_tracker
            .wait_for_result::<AlpacaWsError>(Duration::from_secs(self.auth_timeout_secs), rx)
            .await
            .map_err(|e| anyhow::anyhow!("Alpaca WebSocket authentication failed: {e}"))?;

        log::info!("Alpaca WebSocket authenticated");
        Ok(())
    }

    fn build_auth_payload(&self) -> anyhow::Result<String> {
        let auth = AlpacaWsAuth::new(self.credential.api_key(), self.credential.api_secret()?);
        serde_json::to_string(&auth)
            .map_err(|e| anyhow::anyhow!("Failed to serialize auth message: {e}"))
    }

    async fn send_cmd(&self, cmd: HandlerCommand) -> Result<(), AlpacaWsError> {
        self.cmd_tx
            .read()
            .await
            .send(cmd)
            .map_err(|e| AlpacaWsError::Client(format!("handler unavailable: {e}")))
    }
}

/// Outer Alpaca trade-updates WebSocket client.
///
/// Streams order lifecycle events from the Trading API stream endpoint.
/// After authenticating it issues an absolute `listen` request for the
/// `trade_updates` stream; on reconnect both are replayed.
pub struct AlpacaTradeUpdatesWebSocketClient {
    url: String,
    credential: Credential,
    auth_timeout_secs: u64,
    auth_tracker: AuthTracker,
    signal: Arc<AtomicBool>,
    connection_mode: Arc<ArcSwap<AtomicU8>>,
    cmd_tx: Arc<tokio::sync::RwLock<tokio::sync::mpsc::UnboundedSender<HandlerCommand>>>,
    out_rx: Option<tokio::sync::mpsc::UnboundedReceiver<NautilusWsMessage>>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
    transport_backend: TransportBackend,
    proxy_url: Option<String>,
}

impl Debug for AlpacaTradeUpdatesWebSocketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AlpacaTradeUpdatesWebSocketClient))
            .field("url", &self.url)
            .field("is_active", &self.is_active())
            .field("transport_backend", &self.transport_backend)
            .field("proxy_url", &self.proxy_url)
            .finish_non_exhaustive()
    }
}

impl AlpacaTradeUpdatesWebSocketClient {
    /// Creates a new client without connecting.
    ///
    /// `url` overrides the resolved environment URL when supplied.
    #[must_use]
    pub fn new(
        url: Option<String>,
        environment: AlpacaEnvironment,
        credential: Credential,
        transport_backend: TransportBackend,
        proxy_url: Option<String>,
    ) -> Self {
        let url = url.unwrap_or_else(|| alpaca_trade_updates_ws_url(environment).to_string());
        let (placeholder_tx, _) = tokio::sync::mpsc::unbounded_channel();

        Self {
            url,
            credential,
            auth_timeout_secs: AUTHENTICATION_TIMEOUT_SECS,
            auth_tracker: AuthTracker::new(),
            signal: Arc::new(AtomicBool::new(false)),
            connection_mode: Arc::new(ArcSwap::new(Arc::new(AtomicU8::new(
                ConnectionMode::Closed as u8,
            )))),
            cmd_tx: Arc::new(tokio::sync::RwLock::new(placeholder_tx)),
            out_rx: None,
            task_handle: None,
            transport_backend,
            proxy_url,
        }
    }

    /// Returns the resolved WebSocket URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns `true` when the underlying connection is active.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.connection_mode.load().load(Ordering::Relaxed) == ConnectionMode::Active as u8
    }

    /// Waits until the underlying connection reports active, or returns an
    /// error after `timeout_secs`.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaWsError::Timeout`] if the connection does not reach
    /// the active state within `timeout_secs`.
    pub async fn wait_until_active(&self, timeout_secs: f64) -> Result<(), AlpacaWsError> {
        let timeout = Duration::from_secs_f64(timeout_secs);

        tokio::time::timeout(timeout, async {
            while !self.is_active() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| {
            AlpacaWsError::Timeout(format!(
                "WebSocket connection timeout after {timeout_secs} seconds"
            ))
        })
    }

    /// Establishes the WebSocket connection, authenticates, and issues the
    /// `trade_updates` listen request.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying [`WebSocketClient::connect`] fails,
    /// the credential secret is invalid, or authentication fails or times
    /// out.
    ///
    /// # Panics
    ///
    /// Panics if the listen request fails to serialize, which cannot happen
    /// for the static `trade_updates` payload.
    pub async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_active() {
            log::warn!("Alpaca trade-updates WebSocket already connected");
            return Ok(());
        }

        let auth_payload = self.build_auth_payload()?;
        let listen_payload = serde_json::to_string(&AlpacaWsListen::trade_updates())
            .expect("static listen payload serializes");
        self.signal.store(false, Ordering::Release);

        let (message_handler, raw_rx) = channel_message_handler();
        let cfg = WebSocketConfig {
            url: self.url.clone(),
            headers: vec![],
            heartbeat: Some(HEARTBEAT_INTERVAL.as_secs()),
            heartbeat_msg: None,
            reconnect_timeout_ms: Some(RECONNECT_TIMEOUT.as_millis() as u64),
            reconnect_delay_initial_ms: Some(RECONNECT_DELAY_INITIAL.as_millis() as u64),
            reconnect_delay_max_ms: Some(RECONNECT_DELAY_MAX.as_millis() as u64),
            reconnect_backoff_factor: Some(RECONNECT_BACKOFF_FACTOR),
            reconnect_jitter_ms: Some(RECONNECT_JITTER.as_millis() as u64),
            reconnect_max_attempts: None,
            idle_timeout_ms: None,
            backend: self.transport_backend,
            proxy_url: self.proxy_url.clone(),
        };
        let client =
            WebSocketClient::connect(cfg, Some(message_handler), None, None, vec![], None).await?;

        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel::<HandlerCommand>();
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel::<NautilusWsMessage>();
        let connection_mode_atomic = client.connection_mode_atomic();

        if let Err(e) = cmd_tx.send(HandlerCommand::SetClient(client)) {
            anyhow::bail!("Failed to send SetClient command: {e}");
        }

        *self.cmd_tx.write().await = cmd_tx.clone();
        self.out_rx = Some(out_rx);
        self.connection_mode.store(connection_mode_atomic);

        log::debug!("Alpaca trade-updates WebSocket connected: {}", self.url);

        let signal = Arc::clone(&self.signal);
        let auth_tracker = self.auth_tracker.clone();
        let cmd_tx_for_reconnect = cmd_tx.clone();
        let restore_listen_payload = listen_payload.clone();

        let task = get_runtime().spawn(async move {
            let mut handler = FeedHandler::new(
                FeedKind::TradeUpdates,
                signal,
                cmd_rx,
                raw_rx,
                out_tx,
                auth_tracker,
                Arc::new(DashMap::new()),
            );

            let restore_stream = || {
                if let Err(e) = cmd_tx_for_reconnect.send(HandlerCommand::Authenticate {
                    payload: auth_payload.clone(),
                }) {
                    log::error!("Failed to resend Alpaca auth command: {e}");
                    return;
                }
                if let Err(e) = cmd_tx_for_reconnect.send(HandlerCommand::Send {
                    payload: restore_listen_payload.clone(),
                }) {
                    log::error!("Failed to resend Alpaca listen command: {e}");
                }
            };

            loop {
                match handler.next().await {
                    Some(NautilusWsMessage::Reconnected) => {
                        log::debug!("Alpaca trade-updates WebSocket reconnected");
                        restore_stream();

                        if handler.send(NautilusWsMessage::Reconnected).is_err() {
                            log_forward_failure(handler.is_stopped());
                            break;
                        }
                    }
                    Some(msg) => {
                        if handler.send(msg).is_err() {
                            log_forward_failure(handler.is_stopped());
                            break;
                        }
                    }
                    None => {
                        if handler.is_stopped() {
                            log::debug!(
                                "Alpaca trade-updates handler stop signal observed, exiting loop"
                            );
                        } else {
                            log::warn!("Alpaca trade-updates stream ended unexpectedly");
                        }
                        break;
                    }
                }
            }
            log::debug!("Alpaca trade-updates handler task completed");
        });
        self.task_handle = Some(task);

        self.authenticate().await?;

        self.send_cmd(HandlerCommand::Send {
            payload: listen_payload,
        })
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send listen command: {e}"))?;

        Ok(())
    }

    /// Disconnects gracefully: signals shutdown, drains the handler, then
    /// awaits the task handle with a timeout.
    ///
    /// # Errors
    ///
    /// This function currently completes best-effort shutdown and returns `Ok(())`.
    pub async fn disconnect(&mut self) -> Result<(), AlpacaWsError> {
        log::debug!("Disconnecting Alpaca trade-updates WebSocket");

        if let Err(e) = self.cmd_tx.read().await.send(HandlerCommand::Disconnect) {
            log::debug!("Failed to send Alpaca disconnect command: {e}");
        }
        self.signal.store(true, Ordering::Release);

        if let Some(handle) = self.task_handle.take() {
            await_task_shutdown(handle).await;
        }

        self.connection_mode
            .store(Arc::new(AtomicU8::new(ConnectionMode::Closed as u8)));
        self.auth_tracker.invalidate();
        Ok(())
    }

    /// Receives the next message from the handler, or `None` if the receiver
    /// has been taken or the handler has shut down.
    pub async fn next_event(&mut self) -> Option<NautilusWsMessage> {
        if let Some(rx) = self.out_rx.as_mut() {
            rx.recv().await
        } else {
            None
        }
    }

    /// Takes ownership of the output receiver, leaving `None` behind.
    #[must_use]
    pub fn take_receiver(
        &mut self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<NautilusWsMessage>> {
        self.out_rx.take()
    }

    async fn authenticate(&self) -> anyhow::Result<()> {
        let payload = self.build_auth_payload()?;
        let rx = self.auth_tracker.begin();

        self.cmd_tx
            .read()
            .await
            .send(HandlerCommand::Authenticate { payload })
            .map_err(|e| anyhow::anyhow!("Failed to send authenticate command: {e}"))?;

        self.auth_tracker
            .wait_for_result::<AlpacaWsError>(Duration::from_secs(self.auth_timeout_secs), rx)
            .await
            .map_err(|e| anyhow::anyhow!("Alpaca trade-updates authentication failed: {e}"))?;

        log::info!("Alpaca trade-updates WebSocket authenticated");
        Ok(())
    }

    fn build_auth_payload(&self) -> anyhow::Result<String> {
        let auth = AlpacaWsAuth::new(self.credential.api_key(), self.credential.api_secret()?);
        serde_json::to_string(&auth)
            .map_err(|e| anyhow::anyhow!("Failed to serialize auth message: {e}"))
    }

    async fn send_cmd(&self, cmd: HandlerCommand) -> Result<(), AlpacaWsError> {
        self.cmd_tx
            .read()
            .await
            .send(cmd)
            .map_err(|e| AlpacaWsError::Client(format!("handler unavailable: {e}")))
    }
}

/// Replaces the tracked subscription sets with the server-side sets from a
/// subscription acknowledgement.
fn reconcile_subscriptions(
    subscriptions: &DashMap<AlpacaWsChannel, AHashSet<Ustr>>,
    ack: &AlpacaSubscriptionAck,
) {
    let channels: [(AlpacaWsChannel, &Vec<Ustr>); 5] = [
        (AlpacaWsChannel::Trades, &ack.trades),
        (AlpacaWsChannel::Quotes, &ack.quotes),
        (AlpacaWsChannel::Bars, &ack.bars),
        (AlpacaWsChannel::DailyBars, &ack.daily_bars),
        (AlpacaWsChannel::UpdatedBars, &ack.updated_bars),
    ];
    for (channel, symbols) in channels {
        subscriptions.insert(channel, symbols.iter().copied().collect());
    }
}

fn log_forward_failure(stopped: bool) {
    if stopped {
        log::debug!("Failed to forward Alpaca message (receiver dropped)");
    } else {
        log::error!("Failed to forward Alpaca message (receiver dropped)");
    }
}

async fn await_task_shutdown(handle: tokio::task::JoinHandle<()>) {
    let abort_handle = handle.abort_handle();
    tokio::select! {
        result = handle => match result {
            Ok(()) => log::debug!("Alpaca handler task completed"),
            Err(e) if e.is_cancelled() => log::debug!("Alpaca handler task cancelled"),
            Err(e) => log::error!("Alpaca handler task error: {e:?}"),
        },
        () = tokio::time::sleep(DISCONNECT_TIMEOUT) => {
            log::warn!("Timeout waiting for Alpaca handler task, aborting");
            abort_handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn test_credential() -> Credential {
        Credential::new("key-id".to_string(), "secret-key".to_string())
    }

    #[rstest]
    fn test_market_data_client_default_url() {
        let client = AlpacaWebSocketClient::new(
            None,
            AlpacaDataFeed::Iex,
            test_credential(),
            TransportBackend::default(),
            None,
        );
        assert_eq!(client.url(), "wss://stream.data.alpaca.markets/v2/iex");
        assert!(!client.is_active());
        assert_eq!(client.subscription_count(), 0);
    }

    #[rstest]
    fn test_market_data_client_url_override() {
        let client = AlpacaWebSocketClient::new(
            Some("ws://localhost:9000/v2/test".to_string()),
            AlpacaDataFeed::Sip,
            test_credential(),
            TransportBackend::default(),
            None,
        );
        assert_eq!(client.url(), "ws://localhost:9000/v2/test");
    }

    #[rstest]
    fn test_trade_updates_client_default_urls() {
        let paper = AlpacaTradeUpdatesWebSocketClient::new(
            None,
            AlpacaEnvironment::Paper,
            test_credential(),
            TransportBackend::default(),
            None,
        );
        assert_eq!(paper.url(), "wss://paper-api.alpaca.markets/stream");

        let live = AlpacaTradeUpdatesWebSocketClient::new(
            None,
            AlpacaEnvironment::Live,
            test_credential(),
            TransportBackend::default(),
            None,
        );
        assert_eq!(live.url(), "wss://api.alpaca.markets/stream");
    }

    #[rstest]
    fn test_reconcile_subscriptions_adopts_full_set() {
        let subscriptions: DashMap<AlpacaWsChannel, AHashSet<Ustr>> = DashMap::new();
        subscriptions.insert(
            AlpacaWsChannel::Trades,
            [Ustr::from("MSFT")].into_iter().collect(),
        );

        let ack = AlpacaSubscriptionAck {
            trades: vec![Ustr::from("AAPL")],
            quotes: vec![Ustr::from("AMD"), Ustr::from("CLDR")],
            ..Default::default()
        };
        reconcile_subscriptions(&subscriptions, &ack);

        let trades = subscriptions.get(&AlpacaWsChannel::Trades).unwrap();
        assert_eq!(trades.len(), 1);
        assert!(trades.contains(&Ustr::from("AAPL")));
        let quotes = subscriptions.get(&AlpacaWsChannel::Quotes).unwrap();
        assert_eq!(quotes.len(), 2);
        assert!(
            subscriptions
                .get(&AlpacaWsChannel::Bars)
                .unwrap()
                .is_empty()
        );
    }

    #[rstest]
    fn test_debug_does_not_leak_credentials() {
        let client = AlpacaWebSocketClient::new(
            None,
            AlpacaDataFeed::Iex,
            test_credential(),
            TransportBackend::default(),
            None,
        );
        let dbg_out = format!("{client:?}");
        assert!(!dbg_out.contains("secret-key"));
        assert!(!dbg_out.contains("key-id"));
    }
}
