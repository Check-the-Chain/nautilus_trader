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

//! Wire frames and handler-output message types for Alpaca streams.
//!
//! Market data arrives as JSON arrays of messages tagged by a `"T"` field;
//! the inbound types here deserialize in a single typed pass with borrowed
//! timestamps so the hot path performs no DOM traversal or re-serialization.
//! The trade-updates stream wraps events in a `{"stream", "data"}` envelope.

use std::fmt::Debug;

use nautilus_model::{
    data::{Bar, QuoteTick, TradeTick},
    identifiers::InstrumentId,
};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumIter, EnumString};
use ustr::Ustr;

use crate::common::enums::AlpacaTradeUpdateEvent;

/// Instrument metadata required to build Nautilus values from wire messages.
#[derive(Copy, Clone, Debug)]
pub struct AlpacaInstrumentInfo {
    /// Nautilus instrument identifier.
    pub instrument_id: InstrumentId,
    /// Price precision (decimal places).
    pub price_precision: u8,
    /// Size precision (decimal places).
    pub size_precision: u8,
}

/// Market data stream channel.
#[derive(Copy, Clone, Debug, Display, PartialEq, Eq, Hash, AsRefStr, EnumIter, EnumString)]
#[strum(serialize_all = "camelCase")]
pub enum AlpacaWsChannel {
    /// Trade prints.
    Trades,
    /// NBBO quotes.
    Quotes,
    /// Minute bars.
    Bars,
    /// Daily bars (emitted each minute after market open).
    DailyBars,
    /// Corrected minute bars for late trades.
    UpdatedBars,
}

/// Inbound message produced by the Alpaca feed handlers and consumed by the
/// data and execution clients.
#[derive(Debug, Clone)]
pub enum NautilusWsMessage {
    /// Batched trade ticks parsed from a market data payload.
    Trades(Vec<TradeTick>),
    /// A single NBBO quote tick.
    Quote(QuoteTick),
    /// A completed external bar.
    Bar(Bar),
    /// An order lifecycle event from the trade-updates stream.
    TradeUpdate(Box<AlpacaTradeUpdateMsg>),
    /// Subscription acknowledgement carrying the full server-side set.
    SubscriptionAck(AlpacaSubscriptionAck),
    /// Authentication confirmed by the venue.
    Authenticated,
    /// Venue error frame.
    Error { code: Option<u16>, msg: String },
    /// Transport reconnected; subscriptions are being restored.
    Reconnected,
}

// ================================================================================================
// Outbound messages
// ================================================================================================

/// Authentication request for both stream protocols.
#[derive(Clone, Serialize)]
pub struct AlpacaWsAuth {
    /// Action discriminator (always `"auth"`).
    pub action: &'static str,
    /// API key ID.
    pub key: String,
    /// API secret key.
    pub secret: String,
}

impl AlpacaWsAuth {
    /// Creates a new authentication message.
    #[must_use]
    pub fn new(key: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            action: "auth",
            key: key.into(),
            secret: secret.into(),
        }
    }
}

impl Debug for AlpacaWsAuth {
    /// Custom `Debug` that redacts the credential fields.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AlpacaWsAuth))
            .field("action", &self.action)
            .field("key", &"<redacted>")
            .field("secret", &"<redacted>")
            .finish()
    }
}

/// Subscribe or unsubscribe request for the market data stream.
///
/// Subscribe messages are additive and unsubscribe messages subtractive, so
/// callers send only symbol deltas.
#[derive(Clone, Debug, Serialize)]
pub struct AlpacaWsSubscription {
    /// Action discriminator (`"subscribe"` or `"unsubscribe"`).
    pub action: &'static str,
    /// Trade symbols.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub trades: Vec<Ustr>,
    /// Quote symbols.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub quotes: Vec<Ustr>,
    /// Minute bar symbols.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub bars: Vec<Ustr>,
    /// Daily bar symbols.
    #[serde(rename = "dailyBars", skip_serializing_if = "Vec::is_empty")]
    pub daily_bars: Vec<Ustr>,
    /// Updated bar symbols.
    #[serde(rename = "updatedBars", skip_serializing_if = "Vec::is_empty")]
    pub updated_bars: Vec<Ustr>,
}

impl AlpacaWsSubscription {
    /// Creates a subscribe request for `symbols` on a single `channel`.
    #[must_use]
    pub fn subscribe(channel: AlpacaWsChannel, symbols: Vec<Ustr>) -> Self {
        Self::for_channel("subscribe", channel, symbols)
    }

    /// Creates an unsubscribe request for `symbols` on a single `channel`.
    #[must_use]
    pub fn unsubscribe(channel: AlpacaWsChannel, symbols: Vec<Ustr>) -> Self {
        Self::for_channel("unsubscribe", channel, symbols)
    }

    fn for_channel(action: &'static str, channel: AlpacaWsChannel, symbols: Vec<Ustr>) -> Self {
        let mut request = Self {
            action,
            trades: Vec::new(),
            quotes: Vec::new(),
            bars: Vec::new(),
            daily_bars: Vec::new(),
            updated_bars: Vec::new(),
        };
        match channel {
            AlpacaWsChannel::Trades => request.trades = symbols,
            AlpacaWsChannel::Quotes => request.quotes = symbols,
            AlpacaWsChannel::Bars => request.bars = symbols,
            AlpacaWsChannel::DailyBars => request.daily_bars = symbols,
            AlpacaWsChannel::UpdatedBars => request.updated_bars = symbols,
        }
        request
    }
}

/// Listen request for the trade-updates stream.
///
/// Listen messages are absolute: the venue replaces the active stream set
/// with the supplied list.
#[derive(Clone, Debug, Serialize)]
pub struct AlpacaWsListen {
    /// Action discriminator (always `"listen"`).
    pub action: &'static str,
    /// Requested streams.
    pub data: AlpacaWsListenData,
}

/// Stream list payload for [`AlpacaWsListen`].
#[derive(Clone, Debug, Serialize)]
pub struct AlpacaWsListenData {
    /// Stream names (e.g. `"trade_updates"`).
    pub streams: Vec<&'static str>,
}

/// Trade-updates stream name.
pub const TRADE_UPDATES_STREAM: &str = "trade_updates";

impl AlpacaWsListen {
    /// Creates a listen request for the `trade_updates` stream.
    #[must_use]
    pub fn trade_updates() -> Self {
        Self {
            action: "listen",
            data: AlpacaWsListenData {
                streams: vec![TRADE_UPDATES_STREAM],
            },
        }
    }
}

// ================================================================================================
// Inbound market data messages
// ================================================================================================

/// A single message on the market data stream, tagged by the `"T"` field.
///
/// Payloads always arrive as JSON arrays of these messages; control messages
/// (`success`, `error`, `subscription`) arrive alone while data messages may
/// be batched.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "T", bound(deserialize = "'de: 'a"))]
pub enum AlpacaWsEvent<'a> {
    /// Trade print.
    #[serde(rename = "t")]
    Trade(AlpacaWsTrade<'a>),
    /// NBBO quote.
    #[serde(rename = "q")]
    Quote(AlpacaWsQuote<'a>),
    /// Completed minute bar.
    #[serde(rename = "b")]
    MinuteBar(AlpacaWsBar<'a>),
    /// Cumulative daily bar (emitted each minute after market open).
    #[serde(rename = "d")]
    DailyBar(AlpacaWsBar<'a>),
    /// Corrected minute bar issued when late trades arrive.
    #[serde(rename = "u")]
    UpdatedBar(AlpacaWsBar<'a>),
    /// Subscription acknowledgement carrying the full current set.
    #[serde(rename = "subscription")]
    Subscription(AlpacaSubscriptionAck),
    /// Success control message (`"connected"` / `"authenticated"`).
    #[serde(rename = "success")]
    Success(AlpacaWsSuccess),
    /// Error control message.
    #[serde(rename = "error")]
    Error(AlpacaWsControlError),
    /// Unrecognized message type; content is ignored.
    #[serde(other)]
    Unknown,
}

/// Trade message (`"T":"t"`).
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaWsTrade<'a> {
    /// Symbol.
    #[serde(rename = "S")]
    pub symbol: Ustr,
    /// Trade ID.
    #[serde(rename = "i")]
    pub trade_id: u64,
    /// Exchange code.
    #[serde(rename = "x")]
    pub exchange: Ustr,
    /// Trade price.
    #[serde(rename = "p")]
    pub price: f64,
    /// Trade size in shares.
    #[serde(rename = "s")]
    pub size: u64,
    /// Trade condition codes.
    #[serde(rename = "c", default)]
    pub conditions: Vec<Ustr>,
    /// Tape (`A`/`B`/`C`).
    #[serde(rename = "z", default)]
    pub tape: Option<Ustr>,
    /// RFC 3339 timestamp with nanosecond precision.
    #[serde(rename = "t", borrow)]
    pub timestamp: &'a str,
}

/// Quote message (`"T":"q"`).
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaWsQuote<'a> {
    /// Symbol.
    #[serde(rename = "S")]
    pub symbol: Ustr,
    /// Ask exchange code.
    #[serde(rename = "ax")]
    pub ask_exchange: Ustr,
    /// Ask price.
    #[serde(rename = "ap")]
    pub ask_price: f64,
    /// Ask size in shares.
    #[serde(rename = "as")]
    pub ask_size: u64,
    /// Bid exchange code.
    #[serde(rename = "bx")]
    pub bid_exchange: Ustr,
    /// Bid price.
    #[serde(rename = "bp")]
    pub bid_price: f64,
    /// Bid size in shares.
    #[serde(rename = "bs")]
    pub bid_size: u64,
    /// Quote condition codes.
    #[serde(rename = "c", default)]
    pub conditions: Vec<Ustr>,
    /// Tape (`A`/`B`/`C`).
    #[serde(rename = "z", default)]
    pub tape: Option<Ustr>,
    /// RFC 3339 timestamp with nanosecond precision.
    #[serde(rename = "t", borrow)]
    pub timestamp: &'a str,
}

/// Bar message (`"T":"b"` / `"d"` / `"u"`).
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaWsBar<'a> {
    /// Symbol.
    #[serde(rename = "S")]
    pub symbol: Ustr,
    /// Open price.
    #[serde(rename = "o")]
    pub open: f64,
    /// High price.
    #[serde(rename = "h")]
    pub high: f64,
    /// Low price.
    #[serde(rename = "l")]
    pub low: f64,
    /// Close price.
    #[serde(rename = "c")]
    pub close: f64,
    /// Volume in shares.
    #[serde(rename = "v")]
    pub volume: u64,
    /// Trade count.
    #[serde(rename = "n", default)]
    pub trade_count: Option<u64>,
    /// Volume-weighted average price.
    #[serde(rename = "vw", default)]
    pub vwap: Option<f64>,
    /// RFC 3339 bar-open timestamp.
    #[serde(rename = "t", borrow)]
    pub timestamp: &'a str,
}

/// Subscription acknowledgement (`"T":"subscription"`).
///
/// Always lists the FULL current subscription set per channel, so consumers
/// reconcile tracked state from it rather than applying deltas.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct AlpacaSubscriptionAck {
    /// Subscribed trade symbols.
    #[serde(default)]
    pub trades: Vec<Ustr>,
    /// Subscribed quote symbols.
    #[serde(default)]
    pub quotes: Vec<Ustr>,
    /// Subscribed minute bar symbols.
    #[serde(default)]
    pub bars: Vec<Ustr>,
    /// Subscribed daily bar symbols.
    #[serde(rename = "dailyBars", default)]
    pub daily_bars: Vec<Ustr>,
    /// Subscribed updated bar symbols.
    #[serde(rename = "updatedBars", default)]
    pub updated_bars: Vec<Ustr>,
}

/// Success control message (`"T":"success"`).
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaWsSuccess {
    /// Status text (`"connected"` or `"authenticated"`).
    pub msg: String,
}

/// Error control message (`"T":"error"`).
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaWsControlError {
    /// Venue error code.
    #[serde(default)]
    pub code: Option<u16>,
    /// Error message.
    pub msg: String,
}

// ================================================================================================
// Inbound trade-updates messages
// ================================================================================================

/// Envelope for messages on the trade-updates stream, adjacently tagged by
/// `"stream"` with the payload under `"data"`.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "stream", content = "data")]
pub enum AlpacaStreamMessage {
    /// Authentication acknowledgement.
    #[serde(rename = "authorization")]
    Authorization(AlpacaAuthorizationData),
    /// Listen acknowledgement carrying the active stream set.
    #[serde(rename = "listening")]
    Listening(AlpacaListeningData),
    /// Order lifecycle event.
    #[serde(rename = "trade_updates")]
    TradeUpdate(Box<AlpacaTradeUpdateMsg>),
}

/// Authorization acknowledgement payload.
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaAuthorizationData {
    /// `"authorized"` or `"unauthorized"`.
    pub status: String,
    /// Echoed action (`"authenticate"`).
    #[serde(default)]
    pub action: Option<String>,
}

/// Listening acknowledgement payload.
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaListeningData {
    /// Active stream names.
    pub streams: Vec<String>,
}

/// In-stream failure frame sent before the venue closes the connection.
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaStreamError {
    /// Action discriminator (always `"error"`).
    pub action: String,
    /// Error payload.
    pub data: AlpacaStreamErrorData,
}

/// Error payload for [`AlpacaStreamError`].
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaStreamErrorData {
    /// Failure description.
    #[serde(default)]
    pub error_message: String,
}

/// Order lifecycle event from the trade-updates stream.
///
/// Monetary and quantity fields are decimal strings per the Trading API
/// convention. Fill events carry `price` / `qty` for the individual
/// execution and `position_qty` for the signed post-event position.
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaTradeUpdateMsg {
    /// Event type.
    pub event: AlpacaTradeUpdateEvent,
    /// Event timestamp (fills, cancels, replaces).
    #[serde(default)]
    pub timestamp: Option<String>,
    /// Per-share execution price for this fill.
    #[serde(default)]
    pub price: Option<String>,
    /// Shares executed in this fill.
    #[serde(default)]
    pub qty: Option<String>,
    /// Signed total position after the event.
    #[serde(default)]
    pub position_qty: Option<String>,
    /// Execution ID (fills).
    #[serde(default)]
    pub execution_id: Option<String>,
    /// Event ID (ULID).
    #[serde(default)]
    pub event_id: Option<String>,
    /// Event time.
    #[serde(default)]
    pub at: Option<String>,
    /// The full order object as of this event.
    pub order: AlpacaWsOrder,
}

/// Order object embedded in trade-update events.
///
/// Mirrors the Trading API order schema; almost every field is nullable in
/// practice regardless of the documented `required` set, so all fields are
/// optional except the identifiers and status.
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaWsOrder {
    /// Venue order ID (UUID).
    pub id: String,
    /// Client order ID.
    pub client_order_id: String,
    /// Order status.
    pub status: crate::common::enums::AlpacaOrderStatus,
    /// Symbol.
    #[serde(default)]
    pub symbol: Option<String>,
    /// Asset class.
    #[serde(default)]
    pub asset_class: Option<String>,
    /// Order side.
    #[serde(default)]
    pub side: Option<crate::common::enums::AlpacaOrderSide>,
    /// Order type.
    #[serde(rename = "type", default)]
    pub order_type: Option<crate::common::enums::AlpacaOrderType>,
    /// Time in force.
    #[serde(default)]
    pub time_in_force: Option<crate::common::enums::AlpacaTimeInForce>,
    /// Order quantity (decimal string).
    #[serde(default)]
    pub qty: Option<String>,
    /// Notional amount (decimal string), mutually exclusive with `qty`.
    #[serde(default)]
    pub notional: Option<String>,
    /// Cumulative filled quantity (decimal string).
    #[serde(default)]
    pub filled_qty: Option<String>,
    /// Average fill price (decimal string).
    #[serde(default)]
    pub filled_avg_price: Option<String>,
    /// Limit price (decimal string).
    #[serde(default)]
    pub limit_price: Option<String>,
    /// Stop price (decimal string).
    #[serde(default)]
    pub stop_price: Option<String>,
    /// Trailing amount (decimal string).
    #[serde(default)]
    pub trail_price: Option<String>,
    /// Trailing percent (decimal string).
    #[serde(default)]
    pub trail_percent: Option<String>,
    /// Order class.
    #[serde(default)]
    pub order_class: Option<crate::common::enums::AlpacaOrderClass>,
    /// Extended hours flag.
    #[serde(default)]
    pub extended_hours: Option<bool>,
    /// Creation timestamp.
    #[serde(default)]
    pub created_at: Option<String>,
    /// Last update timestamp.
    #[serde(default)]
    pub updated_at: Option<String>,
    /// Submission timestamp.
    #[serde(default)]
    pub submitted_at: Option<String>,
    /// Fill timestamp.
    #[serde(default)]
    pub filled_at: Option<String>,
    /// Cancel timestamp.
    #[serde(default)]
    pub canceled_at: Option<String>,
    /// Expiry timestamp.
    #[serde(default)]
    pub expired_at: Option<String>,
    /// Cancel request timestamp.
    #[serde(default)]
    pub cancel_requested_at: Option<String>,
    /// ID of the order this one replaces.
    #[serde(default)]
    pub replaces: Option<String>,
    /// ID of the order replacing this one.
    #[serde(default)]
    pub replaced_by: Option<String>,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_auth_message_serialization() {
        let auth = AlpacaWsAuth::new("key-id", "secret-key");
        let json = serde_json::to_string(&auth).unwrap();
        assert_eq!(
            json,
            r#"{"action":"auth","key":"key-id","secret":"secret-key"}"#
        );
    }

    #[rstest]
    fn test_auth_message_debug_redacts_credentials() {
        let auth = AlpacaWsAuth::new("key-id", "secret-key");
        let dbg_out = format!("{auth:?}");
        assert!(!dbg_out.contains("key-id"));
        assert!(!dbg_out.contains("secret-key"));
    }

    #[rstest]
    fn test_subscribe_message_skips_empty_channels() {
        let request = AlpacaWsSubscription::subscribe(
            AlpacaWsChannel::Trades,
            vec![Ustr::from("AAPL"), Ustr::from("MSFT")],
        );
        let json = serde_json::to_string(&request).unwrap();
        assert_eq!(json, r#"{"action":"subscribe","trades":["AAPL","MSFT"]}"#);
    }

    #[rstest]
    fn test_unsubscribe_message_daily_bars_rename() {
        let request =
            AlpacaWsSubscription::unsubscribe(AlpacaWsChannel::DailyBars, vec![Ustr::from("SPY")]);
        let json = serde_json::to_string(&request).unwrap();
        assert_eq!(json, r#"{"action":"unsubscribe","dailyBars":["SPY"]}"#);
    }

    #[rstest]
    fn test_listen_message_serialization() {
        let listen = AlpacaWsListen::trade_updates();
        let json = serde_json::to_string(&listen).unwrap();
        assert_eq!(
            json,
            r#"{"action":"listen","data":{"streams":["trade_updates"]}}"#
        );
    }

    #[rstest]
    fn test_deserialize_trade_event() {
        let json = r#"[{"T":"t","S":"AAPL","i":96921,"x":"V","p":189.05,"s":100,"c":["@"],"z":"C","t":"2026-01-05T14:30:00.123456789Z"}]"#;
        let events: Vec<AlpacaWsEvent> = serde_json::from_str(json).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            AlpacaWsEvent::Trade(trade) => {
                assert_eq!(trade.symbol, Ustr::from("AAPL"));
                assert_eq!(trade.trade_id, 96921);
                assert_eq!(trade.price, 189.05);
                assert_eq!(trade.size, 100);
                assert_eq!(trade.timestamp, "2026-01-05T14:30:00.123456789Z");
            }
            other => panic!("expected trade, got {other:?}"),
        }
    }

    #[rstest]
    fn test_deserialize_quote_event() {
        let json = r#"[{"T":"q","S":"AMD","bx":"U","bp":87.66,"bs":1,"ax":"Q","ap":87.68,"as":4,"c":["R"],"z":"C","t":"2026-01-05T14:30:01.000000001Z"}]"#;
        let events: Vec<AlpacaWsEvent> = serde_json::from_str(json).unwrap();
        match &events[0] {
            AlpacaWsEvent::Quote(quote) => {
                assert_eq!(quote.symbol, Ustr::from("AMD"));
                assert_eq!(quote.bid_price, 87.66);
                assert_eq!(quote.ask_price, 87.68);
                assert_eq!(quote.bid_size, 1);
                assert_eq!(quote.ask_size, 4);
            }
            other => panic!("expected quote, got {other:?}"),
        }
    }

    #[rstest]
    #[case(r#"[{"T":"b","S":"SPY","o":388.985,"h":389.13,"l":388.975,"c":389.12,"v":49378,"n":416,"vw":389.052107,"t":"2026-01-05T14:30:00Z"}]"#)]
    #[case(r#"[{"T":"d","S":"SPY","o":388.985,"h":389.13,"l":388.975,"c":389.12,"v":49378,"t":"2026-01-05T05:00:00Z"}]"#)]
    #[case(r#"[{"T":"u","S":"SPY","o":388.985,"h":389.13,"l":388.975,"c":389.12,"v":49378,"n":416,"vw":389.052107,"t":"2026-01-05T14:30:00Z"}]"#)]
    fn test_deserialize_bar_events(#[case] json: &str) {
        let events: Vec<AlpacaWsEvent> = serde_json::from_str(json).unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            AlpacaWsEvent::MinuteBar(_) | AlpacaWsEvent::DailyBar(_) | AlpacaWsEvent::UpdatedBar(_)
        ));
    }

    #[rstest]
    fn test_deserialize_control_messages() {
        let json = r#"[{"T":"success","msg":"authenticated"}]"#;
        let events: Vec<AlpacaWsEvent> = serde_json::from_str(json).unwrap();
        assert!(
            matches!(&events[0], AlpacaWsEvent::Success(success) if success.msg == "authenticated")
        );

        let json = r#"[{"T":"error","code":406,"msg":"connection limit exceeded"}]"#;
        let events: Vec<AlpacaWsEvent> = serde_json::from_str(json).unwrap();
        assert!(matches!(
            &events[0],
            AlpacaWsEvent::Error(error) if error.code == Some(406)
        ));
    }

    #[rstest]
    fn test_deserialize_subscription_ack_full_set() {
        let json = r#"[{"T":"subscription","trades":["AAPL"],"quotes":["AMD","CLDR"],"bars":["*"],"updatedBars":[],"dailyBars":["SPY"]}]"#;
        let events: Vec<AlpacaWsEvent> = serde_json::from_str(json).unwrap();
        match &events[0] {
            AlpacaWsEvent::Subscription(ack) => {
                assert_eq!(ack.trades, vec![Ustr::from("AAPL")]);
                assert_eq!(ack.quotes.len(), 2);
                assert_eq!(ack.daily_bars, vec![Ustr::from("SPY")]);
                assert!(ack.updated_bars.is_empty());
            }
            other => panic!("expected subscription ack, got {other:?}"),
        }
    }

    #[rstest]
    fn test_deserialize_unknown_tag_is_tolerated() {
        let json = r#"[{"T":"x","S":"AAPL","i":1,"x":"V","p":1.0,"s":1,"a":"C","t":"2026-01-05T14:30:00Z","z":"C"}]"#;
        let events: Vec<AlpacaWsEvent> = serde_json::from_str(json).unwrap();
        assert!(matches!(events[0], AlpacaWsEvent::Unknown));
    }

    #[rstest]
    fn test_deserialize_authorization_envelope() {
        let json =
            r#"{"stream":"authorization","data":{"status":"authorized","action":"authenticate"}}"#;
        let msg: AlpacaStreamMessage = serde_json::from_str(json).unwrap();
        assert!(
            matches!(msg, AlpacaStreamMessage::Authorization(auth) if auth.status == "authorized")
        );
    }

    #[rstest]
    fn test_deserialize_listening_envelope() {
        let json = r#"{"stream":"listening","data":{"streams":["trade_updates"]}}"#;
        let msg: AlpacaStreamMessage = serde_json::from_str(json).unwrap();
        assert!(
            matches!(msg, AlpacaStreamMessage::Listening(data) if data.streams == vec!["trade_updates"])
        );
    }

    #[rstest]
    fn test_deserialize_stream_error() {
        let json = r#"{"action":"error","data":{"error_message":"internal error"}}"#;
        let error: AlpacaStreamError = serde_json::from_str(json).unwrap();
        assert_eq!(error.action, "error");
        assert_eq!(error.data.error_message, "internal error");
    }
}
