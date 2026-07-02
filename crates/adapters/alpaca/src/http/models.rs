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

//! Wire models mirrored from Alpaca Trading and Market Data REST payloads.
//!
//! Trading API numerics arrive as decimal strings; Market Data API prices and
//! sizes arrive as JSON numbers. Alpaca's OpenAPI `required` arrays are
//! unreliable (see the docs for `Order`), so almost every field is optional
//! and models deserialize leniently, tolerating unknown fields.

use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use ustr::Ustr;

use crate::common::enums::{
    AlpacaAssetClass, AlpacaAssetStatus, AlpacaOrderClass, AlpacaOrderSide, AlpacaOrderStatus,
    AlpacaOrderType, AlpacaPositionSide, AlpacaTimeInForce,
};

/// Deserializes an optional field treating an empty string as `None`.
///
/// Alpaca serializes some absent enum values as `""` (e.g. `side` and
/// `asset_class` on multi-leg parent orders).
pub(crate) fn empty_string_as_none<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(serde_json::Value::String(s)) if s.is_empty() => Ok(None),
        Some(v) => T::deserialize(v)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

/// Alpaca asset record from `GET /v2/assets`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AlpacaAsset {
    /// Asset UUID.
    pub id: String,
    /// Asset class.
    pub class: AlpacaAssetClass,
    /// Primary listing exchange (e.g. `NASDAQ`).
    pub exchange: String,
    /// Ticker symbol (e.g. `AAPL`).
    pub symbol: Ustr,
    /// Full asset name.
    pub name: Option<String>,
    /// Asset status.
    pub status: AlpacaAssetStatus,
    /// Whether the asset is tradable on Alpaca.
    pub tradable: bool,
    /// Whether the asset is marginable.
    pub marginable: bool,
    /// Whether the asset is shortable.
    pub shortable: bool,
    /// Whether the asset supports fractional quantities.
    pub fractionable: bool,
    /// Deprecated easy-to-borrow flag (sunset 2026-09-22; use `borrow_status`).
    pub easy_to_borrow: Option<bool>,
    /// Borrow status (`easy_to_borrow` / `hard_to_borrow`); US equities only.
    pub borrow_status: Option<String>,
    /// CUSIP identifier; requires support enablement.
    pub cusip: Option<String>,
    /// Long margin requirement percentage (decimal string).
    pub margin_requirement_long: Option<String>,
    /// Short margin requirement percentage (decimal string).
    pub margin_requirement_short: Option<String>,
    /// Minimum order size (decimal string; crypto only).
    pub min_order_size: Option<String>,
    /// Minimum trade increment (decimal string; crypto only).
    pub min_trade_increment: Option<String>,
    /// Price increment (decimal string; crypto only).
    pub price_increment: Option<String>,
    /// Asset attributes (e.g. `has_options`, `fractional_eh_enabled`).
    #[serde(default)]
    pub attributes: Vec<String>,
}

/// Alpaca account record from `GET /v2/account`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AlpacaAccount {
    /// Account UUID.
    pub id: String,
    /// Account number.
    pub account_number: Option<String>,
    /// Account status (e.g. `ACTIVE`).
    pub status: String,
    /// Account currency (e.g. `USD`).
    pub currency: Option<String>,
    /// Cash balance (decimal string).
    pub cash: Option<String>,
    /// Total equity (decimal string).
    pub equity: Option<String>,
    /// Equity as of the prior trading day (decimal string).
    pub last_equity: Option<String>,
    /// Current buying power (decimal string).
    pub buying_power: Option<String>,
    /// Reg T buying power (decimal string).
    pub regt_buying_power: Option<String>,
    /// Non-marginable buying power (decimal string).
    pub non_marginable_buying_power: Option<String>,
    /// Buying power multiplier (`"1"`, `"2"`, or `"4"`).
    pub multiplier: Option<String>,
    /// Initial margin requirement (decimal string).
    pub initial_margin: Option<String>,
    /// Maintenance margin requirement (decimal string).
    pub maintenance_margin: Option<String>,
    /// Long positions market value (decimal string).
    pub long_market_value: Option<String>,
    /// Short positions market value (decimal string).
    pub short_market_value: Option<String>,
    /// Whether shorting is enabled.
    pub shorting_enabled: Option<bool>,
    /// Whether trading is blocked.
    pub trading_blocked: Option<bool>,
    /// Whether transfers are blocked.
    pub transfers_blocked: Option<bool>,
    /// Whether the account is blocked.
    pub account_blocked: Option<bool>,
    /// Whether the user suspended trading.
    pub trade_suspended_by_user: Option<bool>,
    /// Account creation timestamp (RFC 3339).
    pub created_at: Option<String>,
}

/// Alpaca order record from the Trading API.
///
/// Also embedded in trade-updates stream events. Almost every field is
/// optional: the venue omits or nulls fields depending on order class and
/// lifecycle state.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AlpacaOrder {
    /// Order UUID.
    pub id: String,
    /// Client-assigned order identifier.
    pub client_order_id: Option<String>,
    /// Order creation timestamp (RFC 3339).
    pub created_at: Option<String>,
    /// Last update timestamp (RFC 3339).
    pub updated_at: Option<String>,
    /// Submission timestamp (RFC 3339).
    pub submitted_at: Option<String>,
    /// Fill timestamp (RFC 3339).
    pub filled_at: Option<String>,
    /// Expiry timestamp (RFC 3339).
    pub expired_at: Option<String>,
    /// Expiration timestamp for day orders (RFC 3339).
    pub expires_at: Option<String>,
    /// Cancellation timestamp (RFC 3339).
    pub canceled_at: Option<String>,
    /// Failure timestamp (RFC 3339).
    pub failed_at: Option<String>,
    /// Replacement timestamp (RFC 3339).
    pub replaced_at: Option<String>,
    /// Cancel request timestamp (trade-updates payloads only).
    pub cancel_requested_at: Option<String>,
    /// UUID of the order that replaced this one.
    pub replaced_by: Option<String>,
    /// UUID of the order this one replaces.
    pub replaces: Option<String>,
    /// Asset UUID.
    pub asset_id: Option<String>,
    /// Ticker symbol (empty for multi-leg parents).
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub symbol: Option<Ustr>,
    /// Asset class (empty for multi-leg parents).
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub asset_class: Option<AlpacaAssetClass>,
    /// Order quantity (decimal string; null for notional orders).
    pub qty: Option<String>,
    /// Notional dollar amount (decimal string; null for quantity orders).
    pub notional: Option<String>,
    /// Filled quantity (decimal string, `"0"` initially).
    pub filled_qty: Option<String>,
    /// Average fill price (decimal string).
    pub filled_avg_price: Option<String>,
    /// Order class (`""` is simple).
    pub order_class: Option<AlpacaOrderClass>,
    /// Order type.
    #[serde(rename = "type")]
    pub order_type: Option<AlpacaOrderType>,
    /// Order side (empty for multi-leg parents).
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub side: Option<AlpacaOrderSide>,
    /// Time in force.
    pub time_in_force: Option<AlpacaTimeInForce>,
    /// Limit price (decimal string).
    pub limit_price: Option<String>,
    /// Stop price (decimal string).
    pub stop_price: Option<String>,
    /// Order status.
    pub status: AlpacaOrderStatus,
    /// Whether the order is eligible for extended hours.
    pub extended_hours: Option<bool>,
    /// Nested legs for multi-leg or bracket orders.
    pub legs: Option<Vec<Self>>,
    /// Trailing stop offset in percent (decimal string).
    pub trail_percent: Option<String>,
    /// Trailing stop offset in price (decimal string).
    pub trail_price: Option<String>,
    /// High-water mark for trailing stops (decimal string).
    pub hwm: Option<String>,
    /// Position intent (e.g. `buy_to_open`).
    pub position_intent: Option<String>,
}

/// Alpaca open position record from `GET /v2/positions`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AlpacaPosition {
    /// Asset UUID.
    pub asset_id: String,
    /// Ticker symbol.
    pub symbol: Ustr,
    /// Primary listing exchange.
    pub exchange: String,
    /// Asset class.
    pub asset_class: AlpacaAssetClass,
    /// Average entry price (decimal string).
    pub avg_entry_price: String,
    /// Position quantity (decimal string; signed).
    pub qty: String,
    /// Quantity available for orders (decimal string).
    pub qty_available: Option<String>,
    /// Position side.
    pub side: AlpacaPositionSide,
    /// Current market value (decimal string).
    pub market_value: Option<String>,
    /// Total cost basis (decimal string).
    pub cost_basis: Option<String>,
    /// Unrealized profit/loss (decimal string).
    pub unrealized_pl: Option<String>,
    /// Unrealized profit/loss percent factor (decimal string).
    pub unrealized_plpc: Option<String>,
    /// Unrealized intraday profit/loss (decimal string).
    pub unrealized_intraday_pl: Option<String>,
    /// Unrealized intraday profit/loss percent factor (decimal string).
    pub unrealized_intraday_plpc: Option<String>,
    /// Current asset price (decimal string).
    pub current_price: Option<String>,
    /// Prior trading day close price (decimal string).
    pub lastday_price: Option<String>,
    /// Percent change from prior close (decimal string).
    pub change_today: Option<String>,
    /// Whether the asset is marginable.
    pub asset_marginable: Option<bool>,
}

/// Result row from `DELETE /v2/orders` (cancel all; HTTP 207 multi-status).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AlpacaCancelOrderStatus {
    /// Order UUID.
    pub id: String,
    /// Per-order HTTP status code for the cancel attempt.
    pub status: u16,
}

/// Historical bar from `GET /v2/stocks/bars`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AlpacaBar {
    /// Bar timestamp (RFC 3339; start of the aggregation window).
    pub t: String,
    /// Open price.
    pub o: f64,
    /// High price.
    pub h: f64,
    /// Low price.
    pub l: f64,
    /// Close price.
    pub c: f64,
    /// Volume.
    pub v: u64,
    /// Trade count.
    pub n: Option<u64>,
    /// Volume-weighted average price.
    pub vw: Option<f64>,
}

/// Historical trade from `GET /v2/stocks/trades`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AlpacaHistoricalTrade {
    /// Trade timestamp (RFC 3339, nanosecond precision).
    pub t: String,
    /// Trade ID (unique per symbol per day).
    pub i: u64,
    /// Exchange code.
    pub x: String,
    /// Trade price.
    pub p: f64,
    /// Trade size in shares.
    pub s: u64,
    /// Trade conditions.
    #[serde(default)]
    pub c: Vec<String>,
    /// Tape (`A`/`B`/`C`, `N` overnight, `O` OTC).
    pub z: Option<String>,
    /// Update flag (`canceled` / `incorrect` / `corrected`; absent = valid).
    pub u: Option<String>,
}

/// Historical quote from `GET /v2/stocks/quotes`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AlpacaHistoricalQuote {
    /// Quote timestamp (RFC 3339, nanosecond precision).
    pub t: String,
    /// Bid exchange code.
    pub bx: String,
    /// Bid price (0 means no active bid).
    pub bp: f64,
    /// Bid size in shares.
    pub bs: u64,
    /// Ask exchange code.
    pub ax: String,
    /// Ask price (0 means no active ask).
    pub ap: f64,
    /// Ask size in shares.
    #[serde(rename = "as")]
    pub ask_size: u64,
    /// Quote conditions.
    #[serde(default)]
    pub c: Vec<String>,
    /// Tape.
    pub z: Option<String>,
}

/// Paginated response envelope from `GET /v2/stocks/bars`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AlpacaBarsResponse {
    /// Bars keyed by symbol.
    #[serde(default)]
    pub bars: HashMap<String, Vec<AlpacaBar>>,
    /// Response currency (defaults to USD).
    pub currency: Option<String>,
    /// Pagination cursor; `None` on the final page.
    pub next_page_token: Option<String>,
}

/// Paginated response envelope from `GET /v2/stocks/trades`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AlpacaTradesResponse {
    /// Trades keyed by symbol.
    #[serde(default)]
    pub trades: HashMap<String, Vec<AlpacaHistoricalTrade>>,
    /// Response currency (defaults to USD).
    pub currency: Option<String>,
    /// Pagination cursor; `None` on the final page.
    pub next_page_token: Option<String>,
}

/// Paginated response envelope from `GET /v2/stocks/quotes`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AlpacaQuotesResponse {
    /// Quotes keyed by symbol.
    #[serde(default)]
    pub quotes: HashMap<String, Vec<AlpacaHistoricalQuote>>,
    /// Response currency (defaults to USD).
    pub currency: Option<String>,
    /// Pagination cursor; `None` on the final page.
    pub next_page_token: Option<String>,
}

/// Account activity from `GET /v2/account/activities`.
///
/// The endpoint returns a heterogeneous array: trade activities
/// (`activity_type: "FILL"`) carry execution fields, while non-trade
/// activities (`"FEE"`, `"DIV"`, ...) carry monetary fields. All fields
/// beyond the discriminators are optional so both shapes deserialize.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AlpacaAccountActivity {
    /// Activity identifier (`{date}::{uuid}` format).
    pub id: String,
    /// Activity type (e.g. `FILL`, `FEE`, `DIV`, `TRANS`).
    pub activity_type: String,
    /// Execution timestamp (trade activities).
    pub transaction_time: Option<String>,
    /// Fill subtype: `fill` or `partial_fill` (trade activities).
    #[serde(rename = "type")]
    pub fill_type: Option<String>,
    /// Execution price per share (trade activities).
    pub price: Option<String>,
    /// Executed quantity (trade activities) or activity quantity.
    pub qty: Option<String>,
    /// Order side (trade activities).
    pub side: Option<String>,
    /// Symbol the activity relates to.
    pub symbol: Option<String>,
    /// Quantity remaining on the order (trade activities).
    pub leaves_qty: Option<String>,
    /// Cumulative filled quantity (trade activities).
    pub cum_qty: Option<String>,
    /// Related order identifier (trade activities).
    pub order_id: Option<String>,
    /// Activity date (non-trade activities).
    pub date: Option<String>,
    /// Net monetary amount, negative for debits (non-trade activities).
    pub net_amount: Option<String>,
    /// Per-share amount (non-trade activities).
    pub per_share_amount: Option<String>,
    /// Human-readable description (non-trade activities).
    pub description: Option<String>,
    /// Activity status: `executed`, `correct`, or `canceled`.
    pub status: Option<String>,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::common::testing::load_test_json;

    #[rstest]
    fn test_deserialize_assets() {
        let json = load_test_json("http_get_assets.json");
        let assets: Vec<AlpacaAsset> = serde_json::from_str(&json).unwrap();

        assert_eq!(assets.len(), 2);
        assert_eq!(assets[0].symbol, Ustr::from("AAPL"));
        assert_eq!(assets[0].class, AlpacaAssetClass::UsEquity);
        assert_eq!(assets[0].status, AlpacaAssetStatus::Active);
        assert!(assets[0].fractionable);
        assert!(assets[0].attributes.contains(&"has_options".to_string()));
        assert_eq!(assets[1].symbol, Ustr::from("BRK.A"));
        assert!(!assets[1].fractionable);
    }

    #[rstest]
    fn test_deserialize_account() {
        let json = load_test_json("http_get_account.json");
        let account: AlpacaAccount = serde_json::from_str(&json).unwrap();

        assert_eq!(account.id, "e6a5b6cd-1f27-4b3e-9c31-1a2c5d3f7c11");
        assert_eq!(account.status, "ACTIVE");
        assert_eq!(account.currency.as_deref(), Some("USD"));
        assert_eq!(account.cash.as_deref(), Some("100000.00"));
        assert_eq!(account.multiplier.as_deref(), Some("2"));
    }

    #[rstest]
    fn test_deserialize_order() {
        let json = load_test_json("http_get_order.json");
        let order: AlpacaOrder = serde_json::from_str(&json).unwrap();

        assert_eq!(order.id, "61e69015-8549-4bfd-b9c3-01e75843f47d");
        assert_eq!(order.symbol, Some(Ustr::from("AAPL")));
        assert_eq!(order.side, Some(AlpacaOrderSide::Buy));
        assert_eq!(order.order_type, Some(AlpacaOrderType::Limit));
        assert_eq!(order.time_in_force, Some(AlpacaTimeInForce::Day));
        assert_eq!(order.status, AlpacaOrderStatus::Filled);
        assert_eq!(order.qty.as_deref(), Some("10"));
        assert_eq!(order.filled_qty.as_deref(), Some("10"));
        assert_eq!(order.filled_avg_price.as_deref(), Some("189.04"));
        assert_eq!(order.legs, None);
    }

    #[rstest]
    fn test_deserialize_order_with_empty_side_and_class() {
        let json = r#"{
            "id": "b1e69015-8549-4bfd-b9c3-01e75843f47d",
            "status": "new",
            "symbol": "",
            "asset_class": "",
            "side": "",
            "order_class": "mleg",
            "type": "limit"
        }"#;
        let order: AlpacaOrder = serde_json::from_str(json).unwrap();

        assert_eq!(order.symbol, None);
        assert_eq!(order.asset_class, None);
        assert_eq!(order.side, None);
        assert_eq!(order.order_class, Some(AlpacaOrderClass::Mleg));
    }

    #[rstest]
    fn test_deserialize_orders_list() {
        let json = load_test_json("http_get_orders.json");
        let orders: Vec<AlpacaOrder> = serde_json::from_str(&json).unwrap();

        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0].status, AlpacaOrderStatus::New);
        assert_eq!(orders[1].status, AlpacaOrderStatus::PartiallyFilled);
    }

    #[rstest]
    fn test_deserialize_positions() {
        let json = load_test_json("http_get_positions.json");
        let positions: Vec<AlpacaPosition> = serde_json::from_str(&json).unwrap();

        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].symbol, Ustr::from("AAPL"));
        assert_eq!(positions[0].side, AlpacaPositionSide::Long);
        assert_eq!(positions[0].qty, "100");
    }

    #[rstest]
    fn test_deserialize_bars_response() {
        let json = load_test_json("http_get_stock_bars.json");
        let response: AlpacaBarsResponse = serde_json::from_str(&json).unwrap();

        let bars = response.bars.get("AAPL").unwrap();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].o, 189.01);
        assert_eq!(bars[0].v, 12_345);
        assert_eq!(response.next_page_token.as_deref(), Some("QUFQTHxNfDE3Mz"));
    }

    #[rstest]
    fn test_deserialize_trades_response() {
        let json = load_test_json("http_get_stock_trades.json");
        let response: AlpacaTradesResponse = serde_json::from_str(&json).unwrap();

        let trades = response.trades.get("AAPL").unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].p, 189.05);
        assert_eq!(trades[0].s, 100);
        assert_eq!(trades[0].i, 52_983_525_029_461);
        assert_eq!(response.next_page_token, None);
    }

    #[rstest]
    fn test_deserialize_quotes_response() {
        let json = load_test_json("http_get_stock_quotes.json");
        let response: AlpacaQuotesResponse = serde_json::from_str(&json).unwrap();

        let quotes = response.quotes.get("AAPL").unwrap();
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].bp, 189.04);
        assert_eq!(quotes[0].ask_size, 300);
    }

    #[rstest]
    fn test_deserialize_account_activities() {
        let json = load_test_json("http_get_account_activities.json");
        let activities: Vec<AlpacaAccountActivity> = serde_json::from_str(&json).unwrap();

        assert_eq!(activities.len(), 3);

        let fill = &activities[0];
        assert_eq!(fill.activity_type, "FILL");
        assert_eq!(fill.fill_type.as_deref(), Some("fill"));
        assert_eq!(fill.side.as_deref(), Some("buy"));
        assert_eq!(fill.price.as_deref(), Some("295.2"));
        assert_eq!(fill.qty.as_deref(), Some("50"));
        assert_eq!(
            fill.order_id.as_deref(),
            Some("1cc0059d-d5ab-45fb-b82f-1e022e850e45")
        );

        let partial = &activities[1];
        assert_eq!(partial.fill_type.as_deref(), Some("partial_fill"));
        assert_eq!(partial.leaves_qty.as_deref(), Some("42"));

        let fee = &activities[2];
        assert_eq!(fee.activity_type, "FEE");
        assert_eq!(fee.net_amount.as_deref(), Some("-0.41"));
        assert_eq!(fee.date.as_deref(), Some("2026-07-02"));
        assert!(fee.price.is_none());
    }
}
