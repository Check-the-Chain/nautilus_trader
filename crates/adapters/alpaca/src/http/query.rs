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

//! Query parameter and request body builders for Alpaca REST endpoints.

use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use crate::common::enums::{
    AlpacaAssetClass, AlpacaAssetStatus, AlpacaDataFeed, AlpacaOrderClass, AlpacaOrderSide,
    AlpacaOrderType, AlpacaTimeInForce,
};

/// Query parameters for `GET /v2/assets`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, Builder, PartialEq, Eq)]
#[builder(setter(strip_option), default)]
pub struct GetAssetsParams {
    /// Asset status filter (all statuses when unset).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<AlpacaAssetStatus>,
    /// Asset class filter (defaults to `us_equity` venue-side).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asset_class: Option<AlpacaAssetClass>,
    /// Exchange filter (e.g. `NASDAQ`).
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange: Option<String>,
    /// Comma-separated attributes filter (matches ANY attribute).
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<String>,
}

/// Query parameters for `GET /v2/orders`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, Builder, PartialEq, Eq)]
#[builder(setter(strip_option), default)]
pub struct GetOrdersParams {
    /// Order status filter: `open` (default venue-side), `closed`, or `all`.
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// Maximum number of orders (default 50, max 500).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Exclusive lower bound on submission time (RFC 3339).
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    /// Exclusive upper bound on submission time (RFC 3339).
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub until: Option<String>,
    /// Sort direction (`asc` / `desc`, default `desc`).
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
    /// Roll multi-leg orders under `legs`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nested: Option<bool>,
    /// Comma-separated symbol filter.
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbols: Option<String>,
    /// Order side filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side: Option<AlpacaOrderSide>,
}

/// Query parameters for `GET /v2/stocks/bars`.
#[derive(Clone, Debug, Deserialize, Serialize, Builder, PartialEq)]
#[builder(setter(into, strip_option))]
pub struct GetStockBarsParams {
    /// Comma-separated symbols (required).
    pub symbols: String,
    /// Bar timeframe (e.g. `1Min`, `5Min`, `1Hour`, `1Day`).
    pub timeframe: String,
    /// Inclusive start (RFC 3339 or `YYYY-MM-DD`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    /// Inclusive end (RFC 3339 or `YYYY-MM-DD`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    /// Maximum rows across all symbols (default 1000, max 10000).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Corporate action adjustment (`raw` default, `split`, `dividend`, `all`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adjustment: Option<String>,
    /// Symbol-mapping date (`YYYY-MM-DD`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asof: Option<String>,
    /// Data feed (`sip` default venue-side).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feed: Option<AlpacaDataFeed>,
    /// Pagination cursor from a prior response.
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
    /// Sort order (`asc` default / `desc`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<String>,
}

/// Query parameters for `GET /v2/stocks/trades`.
#[derive(Clone, Debug, Deserialize, Serialize, Builder, PartialEq)]
#[builder(setter(into, strip_option))]
pub struct GetStockTradesParams {
    /// Comma-separated symbols (required).
    pub symbols: String,
    /// Inclusive start (RFC 3339 or `YYYY-MM-DD`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    /// Inclusive end (RFC 3339 or `YYYY-MM-DD`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    /// Maximum rows across all symbols (default 1000, max 10000).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Symbol-mapping date (`YYYY-MM-DD`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asof: Option<String>,
    /// Data feed (`sip` default venue-side).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feed: Option<AlpacaDataFeed>,
    /// Pagination cursor from a prior response.
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
    /// Sort order (`asc` default / `desc`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<String>,
}

/// Query parameters for `GET /v2/stocks/quotes`.
#[derive(Clone, Debug, Deserialize, Serialize, Builder, PartialEq)]
#[builder(setter(into, strip_option))]
pub struct GetStockQuotesParams {
    /// Comma-separated symbols (required).
    pub symbols: String,
    /// Inclusive start (RFC 3339 or `YYYY-MM-DD`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    /// Inclusive end (RFC 3339 or `YYYY-MM-DD`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    /// Maximum rows across all symbols (default 1000, max 10000).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Symbol-mapping date (`YYYY-MM-DD`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asof: Option<String>,
    /// Data feed (`sip` default venue-side).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feed: Option<AlpacaDataFeed>,
    /// Pagination cursor from a prior response.
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
    /// Sort order (`asc` default / `desc`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<String>,
}

/// Take-profit leg for bracket/OTO orders.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AlpacaTakeProfit {
    /// Take-profit limit price (decimal string).
    pub limit_price: String,
}

/// Stop-loss leg for bracket/OTO/OCO orders.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AlpacaStopLoss {
    /// Stop trigger price (decimal string).
    pub stop_price: String,
    /// Optional stop-limit price; omitting yields a stop-market leg.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_price: Option<String>,
}

/// Request body for `POST /v2/orders`.
///
/// `qty` and `notional` are mutually exclusive; the venue rejects requests
/// carrying both. Prices are decimal strings and must respect the sub-penny
/// rule (max 2 decimals at or above $1.00, max 4 below).
#[derive(Clone, Debug, Deserialize, Serialize, Builder, PartialEq, Eq)]
#[builder(setter(into, strip_option))]
pub struct PostOrderParams {
    /// Ticker symbol.
    pub symbol: String,
    /// Order quantity (decimal string; up to 9 decimals when fractionable).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty: Option<String>,
    /// Notional dollar amount (decimal string; market + day orders only).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notional: Option<String>,
    /// Order side.
    pub side: AlpacaOrderSide,
    /// Order type.
    #[serde(rename = "type")]
    pub order_type: AlpacaOrderType,
    /// Time in force.
    pub time_in_force: AlpacaTimeInForce,
    /// Limit price (required for `limit` / `stop_limit`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_price: Option<String>,
    /// Stop price (required for `stop` / `stop_limit`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_price: Option<String>,
    /// Trailing stop offset in price (one of trail price/percent required for `trailing_stop`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trail_price: Option<String>,
    /// Trailing stop offset in percent.
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trail_percent: Option<String>,
    /// Extended-hours eligibility (limit + day/gtc only).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extended_hours: Option<bool>,
    /// Client-assigned order identifier (max 128 chars).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
    /// Order class (`simple` when unset).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_class: Option<AlpacaOrderClass>,
    /// Take-profit leg for bracket/OTO orders.
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub take_profit: Option<AlpacaTakeProfit>,
    /// Stop-loss leg for bracket/OTO/OCO orders.
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_loss: Option<AlpacaStopLoss>,
    /// Position intent (e.g. `buy_to_open`).
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position_intent: Option<String>,
}

/// Request body for `PATCH /v2/orders/{order_id}`.
///
/// A successful replace returns a new order with a new order ID. Orders in
/// `accepted`, `pending_new`, `pending_cancel`, or `pending_replace` states
/// cannot be replaced.
#[derive(Clone, Debug, Default, Deserialize, Serialize, Builder, PartialEq, Eq)]
#[builder(setter(strip_option), default)]
pub struct PatchOrderParams {
    /// New quantity (decimal string; whole shares only).
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty: Option<String>,
    /// New time in force.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<AlpacaTimeInForce>,
    /// New limit price (decimal string).
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_price: Option<String>,
    /// New stop price (decimal string).
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_price: Option<String>,
    /// New trailing offset for trailing-stop orders (decimal string).
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trail: Option<String>,
    /// Client-assigned identifier for the replacement order.
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
}

/// Query parameters for `GET /v2/account/activities`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, Builder, PartialEq, Eq)]
#[builder(setter(strip_option), default)]
pub struct GetAccountActivitiesParams {
    /// Comma-separated activity types filter (e.g. `FILL,FEE`).
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activity_types: Option<String>,
    /// Single date filter (`YYYY-MM-DD`); mutually exclusive with after/until.
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    /// Exclusive lower time bound (RFC 3339).
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    /// Exclusive upper time bound (RFC 3339).
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub until: Option<String>,
    /// Sort direction: `asc` or `desc` (default `desc` venue-side).
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
    /// Maximum entries per page (max 100).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
    /// Pagination cursor (the `id` of the last entry from the prior page).
    #[builder(setter(into, strip_option))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_get_assets_params_query_string() {
        let params = GetAssetsParamsBuilder::default()
            .status(AlpacaAssetStatus::Active)
            .asset_class(AlpacaAssetClass::UsEquity)
            .build()
            .unwrap();

        let query = serde_urlencoded::to_string(&params).unwrap();
        assert_eq!(query, "status=active&asset_class=us_equity");
    }

    #[rstest]
    fn test_get_orders_params_query_string() {
        let params = GetOrdersParamsBuilder::default()
            .status("open")
            .limit(100u32)
            .symbols("AAPL,MSFT")
            .build()
            .unwrap();

        let query = serde_urlencoded::to_string(&params).unwrap();
        assert_eq!(query, "status=open&limit=100&symbols=AAPL%2CMSFT");
    }

    #[rstest]
    fn test_get_stock_bars_params_query_string() {
        let params = GetStockBarsParamsBuilder::default()
            .symbols("AAPL")
            .timeframe("1Min")
            .feed(AlpacaDataFeed::Iex)
            .limit(500u32)
            .build()
            .unwrap();

        let query = serde_urlencoded::to_string(&params).unwrap();
        assert_eq!(query, "symbols=AAPL&timeframe=1Min&limit=500&feed=iex");
    }

    #[rstest]
    fn test_post_order_params_body() {
        let params = PostOrderParamsBuilder::default()
            .symbol("AAPL")
            .qty("10")
            .side(AlpacaOrderSide::Buy)
            .order_type(AlpacaOrderType::Limit)
            .time_in_force(AlpacaTimeInForce::Day)
            .limit_price("189.05")
            .client_order_id("O-20260702-001")
            .build()
            .unwrap();

        let body = serde_json::to_value(&params).unwrap();
        assert_eq!(body["symbol"], "AAPL");
        assert_eq!(body["type"], "limit");
        assert_eq!(body["time_in_force"], "day");
        assert_eq!(body["limit_price"], "189.05");
        assert!(body.get("notional").is_none());
        assert!(body.get("stop_price").is_none());
    }

    #[rstest]
    fn test_post_order_params_bracket_body() {
        let params = PostOrderParamsBuilder::default()
            .symbol("AAPL")
            .qty("10")
            .side(AlpacaOrderSide::Buy)
            .order_type(AlpacaOrderType::Market)
            .time_in_force(AlpacaTimeInForce::Day)
            .order_class(AlpacaOrderClass::Bracket)
            .take_profit(AlpacaTakeProfit {
                limit_price: "200.00".to_string(),
            })
            .stop_loss(AlpacaStopLoss {
                stop_price: "180.00".to_string(),
                limit_price: None,
            })
            .build()
            .unwrap();

        let body = serde_json::to_value(&params).unwrap();
        assert_eq!(body["order_class"], "bracket");
        assert_eq!(body["take_profit"]["limit_price"], "200.00");
        assert_eq!(body["stop_loss"]["stop_price"], "180.00");
        assert!(body["stop_loss"].get("limit_price").is_none());
    }

    #[rstest]
    fn test_patch_order_params_body() {
        let params = PatchOrderParamsBuilder::default()
            .qty("20")
            .limit_price("190.00")
            .build()
            .unwrap();

        let body = serde_json::to_value(&params).unwrap();
        assert_eq!(body["qty"], "20");
        assert_eq!(body["limit_price"], "190.00");
        assert!(body.get("stop_price").is_none());
    }
}
