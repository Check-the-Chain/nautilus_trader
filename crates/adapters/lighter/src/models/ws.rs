use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize};

/// Deserialize a field that the exchange sends in multiple formats:
///
/// 1. JSON `null` or missing → empty `Vec`
/// 2. JSON array: `[{...}, ...]` → `Vec<T>`
/// 3. Map of arrays: `{"1": [{...}], ...}` → flatten into `Vec<T>`
/// 4. Map of single objects: `{"1": {...}, ...}` → collect values into `Vec<T>`
///
/// The `subscribed/account_all` message sends `assets` and `shares` as maps,
/// while `update/account_all` sends them as flat arrays.  Some messages send
/// `null` when the list is empty.
fn vec_or_map<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum VecOrMap<T> {
        Vec(Vec<T>),
        MapOfVecs(HashMap<String, Vec<T>>),
        MapOfSingles(HashMap<String, T>),
    }

    // Wrap in Option so JSON `null` → None → empty vec.
    let opt: Option<VecOrMap<T>> = Option::deserialize(deserializer)?;
    match opt {
        None => Ok(Vec::new()),
        Some(VecOrMap::Vec(v)) => Ok(v),
        Some(VecOrMap::MapOfVecs(m)) => Ok(m.into_values().flatten().collect()),
        Some(VecOrMap::MapOfSingles(m)) => Ok(m.into_values().collect()),
    }
}

/// Deserialize a `HashMap<String, Vec<T>>` that the exchange may also send as
/// `HashMap<String, T>` (single objects instead of arrays), wrapping each value
/// in a one-element `Vec`.
///
/// Handles: outer `null` → empty map, individual values that are `null` → empty vec.
fn map_vec_or_single<'de, D, T>(deserializer: D) -> Result<HashMap<String, Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum MaybeVecOrSingle<T> {
        Vec(Vec<T>),
        Single(T),
        Null(()),
    }

    // Wrap in Option so JSON `null` → None → empty map.
    let opt: Option<HashMap<String, MaybeVecOrSingle<T>>> = Option::deserialize(deserializer)?;
    match opt {
        None => Ok(HashMap::new()),
        Some(raw) => Ok(raw
            .into_iter()
            .filter_map(|(k, v)| match v {
                MaybeVecOrSingle::Vec(vec) => Some((k, vec)),
                MaybeVecOrSingle::Single(item) => Some((k, vec![item])),
                MaybeVecOrSingle::Null(()) => None,
            })
            .collect()),
    }
}

/// Deserialize a `HashMap<String, T>` with `null` or missing treated as empty.
fn map_or_null<'de, D, T>(deserializer: D) -> Result<HashMap<String, T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    let opt: Option<HashMap<String, T>> = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

fn looks_like_market_stats_payload(value: &serde_json::Value) -> bool {
    let Some(obj) = value.as_object() else {
        return false;
    };
    obj.contains_key("market_id")
        || obj.contains_key("symbol")
        || obj.contains_key("mark_price")
        || obj.contains_key("index_price")
        || obj.contains_key("last_trade_price")
        || obj.contains_key("current_funding_rate")
}

/// Deserialize `market_stats` payloads that can arrive as either:
/// 1) a single market object, or
/// 2) a map keyed by market_id with one or more market objects.
///
/// For map payloads, this returns the first decoded market entry.
fn market_or_market_stats<'de, D>(deserializer: D) -> Result<Option<PerpsMarketStats>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    let Some(value) = raw else {
        return Ok(None);
    };

    if looks_like_market_stats_payload(&value) {
        let market =
            serde_json::from_value::<PerpsMarketStats>(value).map_err(serde::de::Error::custom)?;
        return Ok(Some(market));
    }

    if let Some(map) = value.as_object() {
        for (market_key, entry) in map {
            if !entry.is_object() {
                continue;
            }
            let mut market = serde_json::from_value::<PerpsMarketStats>(entry.clone())
                .map_err(serde::de::Error::custom)?;
            if market.market_id.is_none() {
                market.market_id = market_key.parse::<i64>().ok();
            }
            return Ok(Some(market));
        }
    }

    Ok(None)
}

pub use super::asset::Asset;
use super::market::PerpsMarketStats;
use super::order::Order;
use super::order_book::PriceLevel;
pub use super::trade::Trade;

/// Deserialize an `Option<f64>` from either a JSON string (`"1.23"`) or a
/// JSON number (`1.23`).  Missing / `null` → `None`.
fn f64_from_string_or_number<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::{self, Visitor};
    use std::fmt;

    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = Option<f64>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a numeric string or number")
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            if v.is_empty() {
                return Ok(None);
            }
            v.parse::<f64>()
                .map(Some)
                .map_err(|_| E::custom(format!("invalid numeric string: {v}")))
        }
        fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }
        fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
            Ok(Some(v))
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
            Ok(Some(v as f64))
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
            Ok(Some(v as f64))
        }
        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
    }
    deserializer.deserialize_any(V)
}

/// A single price/size level from the BBO ticker channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsTickerLevel {
    #[serde(default, deserialize_with = "f64_from_string_or_number")]
    pub price: Option<f64>,
    #[serde(default, deserialize_with = "f64_from_string_or_number")]
    pub size: Option<f64>,
}

/// Ticker payload: symbol + best ask (`a`) + best bid (`b`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsTickerData {
    pub s: String,
    pub a: WsTickerLevel,
    pub b: WsTickerLevel,
}

/// Top-level WS message for `subscribed/ticker` and `update/ticker`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsTickerUpdate {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub ticker: Option<WsTickerData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default)]
    pub channel: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsOrderBookMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub channel: String,
    pub offset: i64,
    pub order_book: WsOrderBook,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsOrderBook {
    pub code: i64,
    pub asks: Vec<PriceLevel>,
    pub bids: Vec<PriceLevel>,
    pub offset: i64,
    pub nonce: i64,
    pub begin_nonce: i64,
}

/// Local normalized state used by the sdk orderbook handler.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WsOrderBookState {
    #[serde(default)]
    pub asks: Vec<PriceLevel>,
    #[serde(default)]
    pub bids: Vec<PriceLevel>,
}

impl From<WsOrderBook> for WsOrderBookState {
    fn from(value: WsOrderBook) -> Self {
        Self {
            asks: value.asks,
            bids: value.bids,
        }
    }
}

impl From<&WsOrderBook> for WsOrderBookState {
    fn from(value: &WsOrderBook) -> Self {
        Self {
            asks: value.asks.clone(),
            bids: value.bids.clone(),
        }
    }
}

/// Strict docs-typed websocket `Transaction`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub hash: String,
    #[serde(rename = "type")]
    pub tx_type: i64,
    pub info: String,
    pub event_info: String,
    pub status: i64,
    pub transaction_index: i64,
    pub l1_address: String,
    pub account_index: i64,
    pub nonce: i64,
    pub expire_at: i64,
    pub block_height: i64,
    pub queued_at: i64,
    pub executed_at: i64,
    pub sequence_index: i64,
    pub parent_hash: String,
    pub api_key_index: i64,
    pub transaction_time: i64,
    pub committed_at: i64,
    pub verified_at: i64,
}

/// Strict docs-typed websocket `Position`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Position {
    pub market_id: i64,
    pub symbol: String,
    pub initial_margin_fraction: String,
    pub open_order_count: i64,
    pub pending_order_count: i64,
    pub position_tied_order_count: i64,
    pub sign: i64,
    pub position: String,
    pub avg_entry_price: String,
    pub position_value: String,
    pub unrealized_pnl: String,
    pub realized_pnl: String,
    pub liquidation_price: String,
    /// Not present in `subscribed/account_all` payloads.
    #[serde(default)]
    pub total_funding_paid_out: String,
    pub margin_mode: i64,
    pub allocated_margin: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PositionWithDiscount {
    pub market_id: i64,
    pub symbol: String,
    pub initial_margin_fraction: String,
    pub open_order_count: i64,
    pub pending_order_count: i64,
    pub position_tied_order_count: i64,
    pub sign: i64,
    pub position: String,
    pub avg_entry_price: String,
    pub position_value: String,
    pub unrealized_pnl: String,
    pub realized_pnl: String,
    pub liquidation_price: String,
    /// May be omitted when empty in some WS payloads.
    #[serde(default)]
    pub total_funding_paid_out: String,
    pub margin_mode: i64,
    pub allocated_margin: String,
    #[serde(default)]
    pub total_discount: String,
}

impl PositionWithDiscount {
    /// Return the position size with the correct sign applied.
    ///
    /// Positive = long, negative = short, zero = flat.
    pub fn signed_position(&self) -> f64 {
        let qty = self.position.parse::<f64>().unwrap_or(0.0);
        if qty == 0.0 {
            return 0.0;
        }
        if self.sign < 0 {
            -qty.abs()
        } else if self.sign > 0 {
            qty.abs()
        } else {
            0.0
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionFunding {
    pub timestamp: i64,
    pub market_id: i64,
    pub funding_id: i64,
    pub change: String,
    pub rate: String,
    pub position_size: String,
    pub position_side: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PoolShares {
    pub public_pool_index: i64,
    pub shares_amount: i64,
    pub entry_usdc: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsAccountOrdersUpdate {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub account: i64,
    pub channel: String,
    pub orders: HashMap<String, Vec<Order>>,
    pub nonce: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsAccountMarketUpdate {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub account: i64,
    pub channel: String,
    pub assets: Vec<Asset>,
    pub funding_history: HashMap<String, PositionFunding>,
    pub orders: Vec<Order>,
    pub position: Vec<Position>,
    pub trades: Vec<Trade>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsAccountAllUpdate {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub account: i64,
    #[serde(default, deserialize_with = "vec_or_map")]
    pub assets: Vec<Asset>,
    pub channel: String,
    #[serde(default)]
    pub daily_trades_count: f64,
    #[serde(default)]
    pub daily_volume: f64,
    #[serde(default)]
    pub weekly_trades_count: f64,
    #[serde(default)]
    pub weekly_volume: f64,
    #[serde(default)]
    pub monthly_trades_count: f64,
    #[serde(default)]
    pub monthly_volume: f64,
    #[serde(default)]
    pub total_trades_count: f64,
    #[serde(default)]
    pub total_volume: f64,
    #[serde(default, deserialize_with = "map_vec_or_single")]
    pub funding_histories: HashMap<String, Vec<PositionFunding>>,
    #[serde(default, deserialize_with = "map_vec_or_single")]
    pub positions: HashMap<String, Vec<Position>>,
    #[serde(default, deserialize_with = "vec_or_map")]
    pub shares: Vec<PoolShares>,
    #[serde(default, deserialize_with = "map_vec_or_single")]
    pub trades: HashMap<String, Vec<Trade>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsAccountAllAssetsUpdate {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub channel: String,
    pub assets: HashMap<String, Vec<Asset>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsAccountAllTradesUpdate {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub channel: String,
    pub trades: HashMap<String, Vec<Trade>>,
    pub total_volume: f64,
    pub monthly_volume: f64,
    pub weekly_volume: f64,
    pub daily_volume: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsAccountAllPositionsUpdate {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub channel: String,
    #[serde(default, deserialize_with = "map_or_null")]
    pub positions: HashMap<String, PositionWithDiscount>,
    #[serde(default, deserialize_with = "vec_or_map")]
    pub shares: Vec<PoolShares>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsAccountAllOrdersUpdate {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub channel: String,
    pub orders: HashMap<String, Vec<Order>>,
}

// Backward-compatible alias for previous name.
pub type WsAccountUpdate = WsAccountAllUpdate;

/// WebSocket `market_stats` update (channel: `market_stats/{market_id}`
/// or `market_stats/all`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsMarketStatsUpdate {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub channel: String,
    #[serde(
        default,
        alias = "market_stats",
        deserialize_with = "market_or_market_stats"
    )]
    pub market: Option<PerpsMarketStats>,
}

/// Nested stats block inside a `user_stats` update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserStatsBlock {
    #[serde(default)]
    pub collateral: Option<String>,
    #[serde(default)]
    pub portfolio_value: Option<String>,
    #[serde(default)]
    pub leverage: Option<String>,
    #[serde(default)]
    pub available_balance: Option<String>,
    #[serde(default)]
    pub margin_usage: Option<String>,
    #[serde(default)]
    pub buying_power: Option<String>,
    #[serde(default)]
    pub account_trading_mode: Option<i64>,
}

/// WebSocket `user_stats` update (channel: `user_stats/{account_id}`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsUserStatsUpdate {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub channel: String,
    pub stats: UserStatsWithSub,
}

/// Top-level stats object containing optional cross/total sub-stats.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserStatsWithSub {
    #[serde(flatten)]
    pub top: UserStatsBlock,
    #[serde(default)]
    pub cross_stats: Option<UserStatsBlock>,
    #[serde(default)]
    pub total_stats: Option<UserStatsBlock>,
}

#[cfg(test)]
mod tests {
    use super::{
        WsAccountAllOrdersUpdate, WsAccountAllPositionsUpdate, WsAccountAllTradesUpdate,
        WsAccountAllUpdate, WsAccountOrdersUpdate, WsMarketStatsUpdate, WsOrderBookMessage,
        WsUserStatsUpdate,
    };

    #[test]
    fn parses_order_book_update_payload() {
        let raw = r#"{
            "type":"update/order_book",
            "channel":"order_book/89",
            "offset":282472,
            "timestamp":1736739654,
            "order_book":{
                "code":0,
                "offset":282472,
                "nonce":281220,
                "begin_nonce":281220,
                "asks":[{"price":"80.56","size":"0.4"}],
                "bids":[{"price":"80.55","size":"0.9"}]
            }
        }"#;

        let msg: WsOrderBookMessage = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.msg_type, "update/order_book");
        assert_eq!(msg.channel, "order_book/89");
        assert_eq!(msg.offset, 282472);
        assert_eq!(msg.timestamp, 1736739654);
        assert_eq!(msg.order_book.nonce, 281220);
    }

    #[test]
    fn parses_account_all_payload() {
        let raw = r#"{
            "type":"update/account_all",
            "account":54255,
            "assets":[{"symbol":"USDC","asset_id":1,"balance":"100.12","locked_balance":"0"}],
            "channel":"account_all/54255",
            "daily_trades_count":111,
            "daily_volume":120000,
            "weekly_trades_count":211,
            "weekly_volume":220000,
            "monthly_trades_count":311,
            "monthly_volume":320000,
            "total_trades_count":411,
            "total_volume":420000,
            "funding_histories":{},
            "positions":{"89":[{"market_id":89,"symbol":"ETH-USDC","initial_margin_fraction":"100","open_order_count":0,"pending_order_count":0,"position_tied_order_count":0,"sign":1,"position":"0.53","avg_entry_price":"76.82","position_value":"40.7146","unrealized_pnl":"1.1","realized_pnl":"0.0","liquidation_price":"70.0","total_funding_paid_out":"0.0","margin_mode":0,"allocated_margin":"0.0"}]},
            "shares":[],
            "trades":{}
        }"#;

        let msg: WsAccountAllUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.account, 54255);
        assert_eq!(msg.assets.len(), 1);
        assert_eq!(msg.positions.get("89").unwrap().len(), 1);
    }

    /// The exchange may send `null` for collection fields (assets, shares,
    /// trades, positions, funding_histories) when they are empty.  The custom
    /// deserializers must treat `null` as an empty collection.
    #[test]
    fn parses_account_all_with_null_fields() {
        let raw = r#"{
            "type":"update/account_all",
            "account":713480,
            "assets":null,
            "channel":"account_all:713480",
            "daily_trades_count":0,
            "daily_volume":0,
            "weekly_trades_count":0,
            "weekly_volume":0,
            "monthly_trades_count":0,
            "monthly_volume":0,
            "total_trades_count":0,
            "total_volume":0,
            "funding_histories":null,
            "positions":null,
            "shares":null,
            "trades":null
        }"#;

        let msg: WsAccountAllUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.account, 713480);
        assert!(msg.assets.is_empty());
        assert!(msg.shares.is_empty());
        assert!(msg.trades.is_empty());
        assert!(msg.positions.is_empty());
        assert!(msg.funding_histories.is_empty());
    }

    /// Fields may be entirely absent from the payload (not just null).
    #[test]
    fn parses_account_all_with_missing_fields() {
        let raw = r#"{
            "type":"update/account_all",
            "account":713480,
            "channel":"account_all:713480"
        }"#;

        let msg: WsAccountAllUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.account, 713480);
        assert!(msg.assets.is_empty());
        assert!(msg.trades.is_empty());
        assert_eq!(msg.daily_volume, 0.0);
    }

    /// The `subscribed/account_all` message sends `assets` and `shares` as maps
    /// (keyed by id) instead of arrays. Verify the custom deserializer handles it.
    #[test]
    fn parses_account_all_subscribed_map_format() {
        let raw = r#"{
            "type":"subscribed/account_all",
            "account":713480,
            "assets":{"1":[{"symbol":"USDC","asset_id":1,"balance":"500.00","locked_balance":"10"}]},
            "channel":"account_all:713480",
            "daily_trades_count":5,
            "daily_volume":1000,
            "weekly_trades_count":20,
            "weekly_volume":5000,
            "monthly_trades_count":80,
            "monthly_volume":20000,
            "total_trades_count":200,
            "total_volume":100000,
            "funding_histories":{},
            "positions":{"89":[{"market_id":89,"symbol":"BTC-USDC","initial_margin_fraction":"100","open_order_count":2,"pending_order_count":0,"position_tied_order_count":0,"sign":1,"position":"0.01","avg_entry_price":"95000.00","position_value":"950.00","unrealized_pnl":"5.0","realized_pnl":"0.0","liquidation_price":"80000.0","total_funding_paid_out":"0.0","margin_mode":0,"allocated_margin":"0.0"}]},
            "shares":{},
            "trades":{}
        }"#;

        let msg: WsAccountAllUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.msg_type, "subscribed/account_all");
        assert_eq!(msg.account, 713480);
        assert_eq!(msg.assets.len(), 1);
        assert_eq!(msg.assets[0].symbol, "USDC");
        assert_eq!(msg.positions.get("89").unwrap().len(), 1);
        assert!(msg.shares.is_empty());
    }

    /// The exchange may also send map values as single objects instead of arrays.
    #[test]
    fn parses_account_all_subscribed_single_object_map() {
        let raw = r#"{
            "type":"subscribed/account_all",
            "account":713480,
            "assets":{"1":{"symbol":"USDC","asset_id":1,"balance":"500.00","locked_balance":"10"}},
            "channel":"account_all:713480",
            "daily_trades_count":5,
            "daily_volume":1000,
            "weekly_trades_count":20,
            "weekly_volume":5000,
            "monthly_trades_count":80,
            "monthly_volume":20000,
            "total_trades_count":200,
            "total_volume":100000,
            "funding_histories":{},
            "positions":{"89":[{"market_id":89,"symbol":"BTC-USDC","initial_margin_fraction":"100","open_order_count":2,"pending_order_count":0,"position_tied_order_count":0,"sign":1,"position":"0.01","avg_entry_price":"95000.00","position_value":"950.00","unrealized_pnl":"5.0","realized_pnl":"0.0","liquidation_price":"80000.0","total_funding_paid_out":"0.0","margin_mode":0,"allocated_margin":"0.0"}]},
            "shares":{},
            "trades":{}
        }"#;

        let msg: WsAccountAllUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.assets.len(), 1);
        assert_eq!(msg.assets[0].symbol, "USDC");
    }

    #[test]
    fn parses_account_orders_update() {
        let raw = r#"{
            "type":"update/account_orders",
            "channel":"account_orders/89/54255",
            "account":54255,
            "nonce":160,
            "orders":{
                "89":[
                    {
                        "order_index":19039,
                        "client_order_index":123,
                        "order_id":"19039",
                        "client_order_id":"123",
                        "market_index":89,
                        "owner_account_index":54255,
                        "initial_base_amount":"0.4",
                        "price":"80.56",
                        "nonce":2,
                        "remaining_base_amount":"0.4",
                        "is_ask":true,
                        "base_size":4000,
                        "base_price":8056,
                        "filled_base_amount":"0",
                        "filled_quote_amount":"0",
                        "side":"ask",
                        "type":"limit",
                        "time_in_force":"gtc",
                        "reduce_only":false,
                        "trigger_price":"0",
                        "order_expiry":0,
                        "status":"open",
                        "trigger_status":"none",
                        "trigger_time":0,
                        "parent_order_index":0,
                        "parent_order_id":"",
                        "to_trigger_order_id_0":"",
                        "to_trigger_order_id_1":"",
                        "to_cancel_order_id_0":"",
                        "block_height":1,
                        "timestamp":1736739654,
                        "created_at":1736739654,
                        "updated_at":1736739654,
                        "transaction_time":1736739654
                    }
                ]
            }
        }"#;

        let msg: WsAccountOrdersUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.account, 54255);
        assert_eq!(msg.nonce, 160);
        assert_eq!(msg.orders.get("89").unwrap().len(), 1);
        assert_eq!(msg.orders.get("89").unwrap()[0].price, "80.56");
    }

    #[test]
    fn parses_account_all_trades_update() {
        let raw = r#"{
            "type":"update/account_all_trades",
            "channel":"account_all_trades/54255",
            "trades":{},
            "total_volume":120000.0,
            "monthly_volume":42000.0,
            "weekly_volume":10000.0,
            "daily_volume":5000.0
        }"#;

        let msg: WsAccountAllTradesUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.msg_type, "update/account_all_trades");
        assert_eq!(msg.daily_volume, 5000.0);
    }

    #[test]
    fn parses_account_all_positions_update() {
        let raw = r#"{
            "type":"update/account_all_positions",
            "channel":"account_all_positions/54255",
            "positions":{
                "89":{
                    "market_id":89,
                    "symbol":"ETH-USDC",
                    "initial_margin_fraction":"100",
                    "open_order_count":0,
                    "pending_order_count":0,
                    "position_tied_order_count":0,
                    "sign":1,
                    "position":"0.53",
                    "avg_entry_price":"76.82",
                    "position_value":"40.7146",
                    "unrealized_pnl":"1.1",
                    "realized_pnl":"0.0",
                    "liquidation_price":"70.0",
                    "total_funding_paid_out":"0.0",
                    "margin_mode":0,
                    "allocated_margin":"0.0",
                    "total_discount":"0"
                }
            },
            "shares":[
                {
                    "public_pool_index":1,
                    "shares_amount":120,
                    "entry_usdc":"101.5"
                }
            ]
        }"#;

        let msg: WsAccountAllPositionsUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.positions.get("89").unwrap().total_discount, "0");
        assert_eq!(msg.shares.len(), 1);
        assert_eq!(msg.shares[0].public_pool_index, 1);
    }

    #[test]
    fn parses_account_all_positions_with_map_shares() {
        let raw = r#"{
            "type":"update/account_all_positions",
            "channel":"account_all_positions/54255",
            "positions":{
                "89":{
                    "market_id":89,
                    "symbol":"ETH-USDC",
                    "initial_margin_fraction":"100",
                    "open_order_count":0,
                    "pending_order_count":0,
                    "position_tied_order_count":0,
                    "sign":1,
                    "position":"0.53",
                    "avg_entry_price":"76.82",
                    "position_value":"40.7146",
                    "unrealized_pnl":"1.1",
                    "realized_pnl":"0.0",
                    "liquidation_price":"70.0",
                    "total_funding_paid_out":"0.0",
                    "margin_mode":0,
                    "allocated_margin":"0.0",
                    "total_discount":"0"
                }
            },
            "shares":{
                "1":{
                    "public_pool_index":1,
                    "shares_amount":120,
                    "entry_usdc":"101.5"
                }
            }
        }"#;

        let msg: WsAccountAllPositionsUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.shares.len(), 1);
        assert_eq!(msg.shares[0].public_pool_index, 1);
    }

    #[test]
    fn parses_account_all_positions_without_total_funding_paid_out() {
        let raw = r#"{
            "type":"update/account_all_positions",
            "channel":"account_all_positions/54255",
            "positions":{
                "89":{
                    "market_id":89,
                    "symbol":"ETH-USDC",
                    "initial_margin_fraction":"100",
                    "open_order_count":0,
                    "pending_order_count":0,
                    "position_tied_order_count":0,
                    "sign":1,
                    "position":"0.53",
                    "avg_entry_price":"76.82",
                    "position_value":"40.7146",
                    "unrealized_pnl":"1.1",
                    "realized_pnl":"0.0",
                    "liquidation_price":"70.0",
                    "margin_mode":0,
                    "allocated_margin":"0.0",
                    "total_discount":"0"
                }
            }
        }"#;

        let msg: WsAccountAllPositionsUpdate = serde_json::from_str(raw).unwrap();
        let pos = msg.positions.get("89").unwrap();
        assert_eq!(pos.total_funding_paid_out, "");
        assert_eq!(pos.total_discount, "0");
    }

    #[test]
    fn parses_account_all_positions_with_missing_nested_fields() {
        let raw = r#"{
            "type":"update/account_all_positions",
            "channel":"account_all_positions/54255",
            "positions":{
                "89":{
                    "market_id":89
                }
            },
            "shares":[
                {
                    "public_pool_index":1
                }
            ]
        }"#;

        let msg: WsAccountAllPositionsUpdate = serde_json::from_str(raw).unwrap();
        let pos = msg.positions.get("89").unwrap();
        assert_eq!(pos.market_id, 89);
        assert_eq!(pos.symbol, "");
        assert_eq!(pos.position, "");
        assert_eq!(pos.margin_mode, 0);
        assert_eq!(msg.shares.len(), 1);
        assert_eq!(msg.shares[0].public_pool_index, 1);
        assert_eq!(msg.shares[0].shares_amount, 0);
        assert_eq!(msg.shares[0].entry_usdc, "");
    }

    #[test]
    fn parses_account_all_positions_with_null_or_missing_collections() {
        let raw_null = r#"{
            "type":"update/account_all_positions",
            "channel":"account_all_positions/54255",
            "positions":null,
            "shares":null
        }"#;
        let msg_null: WsAccountAllPositionsUpdate = serde_json::from_str(raw_null).unwrap();
        assert!(msg_null.positions.is_empty());
        assert!(msg_null.shares.is_empty());

        let raw_missing = r#"{
            "type":"update/account_all_positions",
            "channel":"account_all_positions/54255"
        }"#;
        let msg_missing: WsAccountAllPositionsUpdate = serde_json::from_str(raw_missing).unwrap();
        assert!(msg_missing.positions.is_empty());
        assert!(msg_missing.shares.is_empty());
    }

    #[test]
    fn parses_account_all_orders_update() {
        let raw = r#"{
            "type":"update/account_all_orders",
            "channel":"account_all_orders/54255",
            "orders":{"89":[]}
        }"#;

        let msg: WsAccountAllOrdersUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.msg_type, "update/account_all_orders");
        assert!(msg.orders.contains_key("89"));
    }

    /// Verify that `WsAccountAllUpdate` can parse a payload containing actual
    /// trade data. Previously the `Trade` struct had too many required fields,
    /// causing the entire `account_all` message to fail deserialization once
    /// the account had any trade history — which silently killed all risk data.
    #[test]
    fn parses_account_all_with_trades() {
        let raw = r#"{
            "type":"update/account_all",
            "account":54255,
            "assets":[{"symbol":"USDC","asset_id":1,"balance":"100.12","locked_balance":"0"}],
            "channel":"account_all/54255",
            "daily_trades_count":3,
            "daily_volume":5000,
            "weekly_trades_count":10,
            "weekly_volume":20000,
            "monthly_trades_count":30,
            "monthly_volume":60000,
            "total_trades_count":100,
            "total_volume":200000,
            "funding_histories":{},
            "positions":{"89":[{"market_id":89,"symbol":"ETH-USDC","initial_margin_fraction":"100","open_order_count":1,"pending_order_count":0,"position_tied_order_count":0,"sign":1,"position":"0.53","avg_entry_price":"76.82","position_value":"40.7146","unrealized_pnl":"1.1","realized_pnl":"0.0","liquidation_price":"70.0","total_funding_paid_out":"0.0","margin_mode":0,"allocated_margin":"0.0"}]},
            "shares":[],
            "trades":{"89":[
                {
                    "trade_id":50001,
                    "tx_hash":"0xabc123",
                    "type":"trade",
                    "market_id":89,
                    "size":"0.53",
                    "price":"76.82",
                    "usd_amount":"40.71",
                    "ask_id":19040,
                    "bid_id":19039,
                    "ask_client_id":100,
                    "bid_client_id":200,
                    "ask_account_id":54255,
                    "bid_account_id":99999,
                    "is_maker_ask":true,
                    "block_height":12345,
                    "timestamp":1736739654,
                    "taker_fee":10,
                    "taker_position_size_before":"0",
                    "taker_entry_quote_before":"0",
                    "taker_initial_margin_fraction_before":100,
                    "taker_position_sign_changed":false,
                    "maker_fee":5,
                    "maker_position_size_before":"0",
                    "maker_entry_quote_before":"0",
                    "maker_initial_margin_fraction_before":100,
                    "maker_position_sign_changed":false,
                    "transaction_time":1736739654,
                    "ask_account_pnl":"0.0",
                    "bid_account_pnl":"0.0"
                }
            ]}
        }"#;

        let msg: WsAccountAllUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.account, 54255);
        let trades = msg.trades.get("89").unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].trade_id, 50001);
        assert_eq!(trades[0].price, "76.82");
        assert!(trades[0].is_maker_ask);
    }

    /// The WS may send a minimal trade payload without all the REST fields.
    /// This must not break deserialization of the parent message.
    #[test]
    fn parses_account_all_with_minimal_trades() {
        let raw = r#"{
            "type":"update/account_all",
            "account":54255,
            "assets":[{"symbol":"USDC","asset_id":1,"balance":"100.12","locked_balance":"0"}],
            "channel":"account_all/54255",
            "daily_trades_count":1,
            "daily_volume":1000,
            "weekly_trades_count":1,
            "weekly_volume":1000,
            "monthly_trades_count":1,
            "monthly_volume":1000,
            "total_trades_count":1,
            "total_volume":1000,
            "funding_histories":{},
            "positions":{},
            "shares":[],
            "trades":{"89":[
                {
                    "trade_id":50002,
                    "market_id":89,
                    "size":"1.0",
                    "price":"100.00",
                    "is_maker_ask":false,
                    "timestamp":1736740000
                }
            ]}
        }"#;

        let msg: WsAccountAllUpdate = serde_json::from_str(raw).unwrap();
        let trades = msg.trades.get("89").unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].trade_id, 50002);
        assert_eq!(trades[0].tx_hash, "");
        assert_eq!(trades[0].trade_type, "");
        assert_eq!(trades[0].usd_amount, "");
    }

    /// Verify that `WsAccountAllOrdersUpdate` can parse actual order objects.
    #[test]
    fn parses_account_all_orders_with_data() {
        let raw = r#"{
            "type":"update/account_all_orders",
            "channel":"account_all_orders/54255",
            "orders":{"89":[
                {
                    "order_index":19039,
                    "market_index":89,
                    "initial_base_amount":"0.4",
                    "price":"80.56",
                    "remaining_base_amount":"0.4",
                    "is_ask":true,
                    "filled_base_amount":"0",
                    "side":"ask",
                    "type":"limit",
                    "time_in_force":"good-till-time",
                    "reduce_only":false,
                    "status":"open",
                    "timestamp":1736739654,
                    "created_at":1736739654
                }
            ]}
        }"#;

        let msg: WsAccountAllOrdersUpdate = serde_json::from_str(raw).unwrap();
        let orders = msg.orders.get("89").unwrap();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].order_index, 19039);
        assert_eq!(orders[0].price, "80.56");
        assert_eq!(orders[0].status, "open");
        // Fields not in the payload should default:
        assert_eq!(orders[0].trigger_price, "");
        assert_eq!(orders[0].block_height, 0);
    }

    #[test]
    fn parses_user_stats_update() {
        let raw = r#"{
            "type":"update/user_stats",
            "channel":"user_stats:54255",
            "stats":{
                "collateral":"1000.50",
                "portfolio_value":"1200.00",
                "leverage":"1.20",
                "available_balance":"800.00",
                "margin_usage":"0.17",
                "buying_power":"4000.00",
                "account_trading_mode":0,
                "cross_stats":{
                    "collateral":"900.00",
                    "portfolio_value":"1100.00",
                    "leverage":"1.10",
                    "available_balance":"700.00",
                    "margin_usage":"0.15",
                    "buying_power":"3500.00"
                },
                "total_stats":{
                    "collateral":"1000.50",
                    "portfolio_value":"1200.00",
                    "leverage":"1.20",
                    "available_balance":"800.00",
                    "margin_usage":"0.17",
                    "buying_power":"4000.00"
                }
            }
        }"#;

        let msg: WsUserStatsUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.msg_type, "update/user_stats");
        assert_eq!(msg.stats.top.collateral.as_deref(), Some("1000.50"));
        assert_eq!(msg.stats.top.portfolio_value.as_deref(), Some("1200.00"));
        assert_eq!(msg.stats.top.leverage.as_deref(), Some("1.20"));
        assert_eq!(msg.stats.top.available_balance.as_deref(), Some("800.00"));
        assert_eq!(msg.stats.top.margin_usage.as_deref(), Some("0.17"));
        assert_eq!(msg.stats.top.buying_power.as_deref(), Some("4000.00"));
        assert_eq!(msg.stats.top.account_trading_mode, Some(0));
        assert!(msg.stats.cross_stats.is_some());
        assert_eq!(
            msg.stats
                .cross_stats
                .as_ref()
                .unwrap()
                .collateral
                .as_deref(),
            Some("900.00")
        );
        assert!(msg.stats.total_stats.is_some());
    }

    #[test]
    fn parses_market_stats_update() {
        let raw = r#"{
            "type":"update/market_stats",
            "channel":"market_stats:91",
            "market":{
                "market_id":"91",
                "symbol":"BTC-USDC",
                "last_trade_price":"108331.1",
                "mark_price":"108330.8",
                "index_price":"108328.7",
                "open_interest":"1751497.89",
                "next_funding_time":1736834400,
                "funding_rate":"0.0000038",
                "funding_countdown":22994,
                "volume_24h":"223388779.12"
            }
        }"#;

        let msg: WsMarketStatsUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.msg_type, "update/market_stats");
        assert_eq!(msg.channel, "market_stats:91");
        let market = msg.market.unwrap();
        assert_eq!(market.market_id, Some(91));
        assert_eq!(market.symbol.as_deref(), Some("BTC-USDC"));
        assert_eq!(market.last_trade_price, Some(108331.1));
        assert_eq!(market.mark_price, Some(108330.8));
        assert_eq!(market.next_funding_time, Some(1736834400));
        assert_eq!(market.funding_countdown, Some(22994));
    }

    #[test]
    fn parses_market_stats_all_docs_shape() {
        let raw = r#"{
            "type":"update/market_stats",
            "channel":"market_stats/all",
            "market_stats":{
                "market_id":"91",
                "symbol":"BTC-USDC",
                "last_trade_price":"108331.1",
                "mark_price":"108330.8",
                "index_price":"108328.7",
                "open_interest":"1751497.89",
                "funding_timestamp":1736834400,
                "current_funding_rate":"0.0000038",
                "daily_quote_token_volume":"223388779.12",
                "daily_price_high":"109220.3",
                "daily_price_low":"107500.2",
                "daily_price_change":"0.91"
            }
        }"#;

        let msg: WsMarketStatsUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.msg_type, "update/market_stats");
        assert_eq!(msg.channel, "market_stats/all");
        let market = msg.market.unwrap();
        assert_eq!(market.market_id, Some(91));
        assert_eq!(market.symbol.as_deref(), Some("BTC-USDC"));
        assert_eq!(market.mark_price, Some(108330.8));
        assert_eq!(market.next_funding_time, Some(1736834400));
        assert_eq!(market.current_funding_rate, Some(0.0000038));
        assert_eq!(market.volume_24h, Some(223388779.12));
        assert_eq!(market.high_24h, Some(109220.3));
        assert_eq!(market.low_24h, Some(107500.2));
        assert_eq!(market.change_24h, Some(0.91));
    }

    #[test]
    fn parses_market_stats_all_map_shape() {
        let raw = r#"{
            "type":"update/market_stats",
            "channel":"market_stats:all",
            "market_stats":{
                "2":{
                    "symbol":"SOL",
                    "mark_price":"85.395",
                    "current_funding_rate":"-0.0011"
                }
            }
        }"#;

        let msg: WsMarketStatsUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(msg.msg_type, "update/market_stats");
        assert_eq!(msg.channel, "market_stats:all");
        let market = msg.market.unwrap();
        assert_eq!(market.market_id, Some(2));
        assert_eq!(market.symbol.as_deref(), Some("SOL"));
        assert_eq!(market.mark_price, Some(85.395));
        assert_eq!(market.current_funding_rate, Some(-0.0011));
    }
}
