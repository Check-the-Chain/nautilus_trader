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

//! Integration tests for the Alpaca HTTP client using a mock Axum server.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::{
    Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post},
};
use nautilus_alpaca::{
    common::{
        credential::Credential,
        enums::{
            AlpacaEnvironment, AlpacaOrderSide, AlpacaOrderStatus, AlpacaOrderType,
            AlpacaTimeInForce,
        },
    },
    http::{
        client::{AlpacaHttpClient, AlpacaRawHttpClient},
        error::AlpacaHttpError,
        query::{
            GetAssetsParamsBuilder, GetOrdersParamsBuilder, GetStockBarsParamsBuilder,
            PatchOrderParamsBuilder, PostOrderParamsBuilder,
        },
    },
};
use nautilus_model::instruments::Instrument;
use nautilus_network::retry::{RetryConfig, RetryManager};
use ustr::Ustr;

const HTTP_GET_ASSETS: &str = include_str!("../test_data/http_get_assets.json");
const HTTP_GET_ACCOUNT: &str = include_str!("../test_data/http_get_account.json");
const HTTP_GET_ORDER: &str = include_str!("../test_data/http_get_order.json");
const HTTP_GET_ORDERS: &str = include_str!("../test_data/http_get_orders.json");
const HTTP_GET_POSITIONS: &str = include_str!("../test_data/http_get_positions.json");
const HTTP_GET_STOCK_BARS: &str = include_str!("../test_data/http_get_stock_bars.json");
const HTTP_GET_STOCK_BARS_PAGE2: &str = include_str!("../test_data/http_get_stock_bars_page2.json");
const HTTP_GET_STOCK_TRADES: &str = include_str!("../test_data/http_get_stock_trades.json");
const HTTP_GET_STOCK_QUOTES: &str = include_str!("../test_data/http_get_stock_quotes.json");
const HTTP_ERROR: &str = include_str!("../test_data/http_error.json");

const TEST_API_KEY: &str = "PKTEST1234567890";
const TEST_API_SECRET: &str = "supersecretvalue";

async fn spawn_server(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

// Compressed retry timings keep retry-path assertions in the millisecond range.
fn fast_retry_manager(max_retries: u32) -> RetryManager<AlpacaHttpError> {
    RetryManager::new(RetryConfig {
        max_retries,
        initial_delay_ms: 1,
        max_delay_ms: 1,
        backoff_factor: 1.0,
        jitter_ms: 0,
        operation_timeout_ms: Some(60_000),
        immediate_first: true,
        max_elapsed_ms: Some(60_000),
    })
}

fn authenticated_raw_client(base_url: &str) -> AlpacaRawHttpClient {
    let credential = Credential::new(TEST_API_KEY.to_string(), TEST_API_SECRET.to_string());
    let mut client = AlpacaRawHttpClient::new(
        AlpacaEnvironment::Paper,
        Some(base_url.to_string()),
        Some(base_url.to_string()),
        10,
        None,
        Some(credential),
    )
    .unwrap();
    client.set_retry_manager(fast_retry_manager(3));
    client
}

fn authenticated_client(base_url: &str) -> AlpacaHttpClient {
    AlpacaHttpClient::from_raw(authenticated_raw_client(base_url))
}

fn assert_auth_headers(headers: &HeaderMap) {
    assert_eq!(
        headers.get("APCA-API-KEY-ID").and_then(|v| v.to_str().ok()),
        Some(TEST_API_KEY),
    );
    assert_eq!(
        headers
            .get("APCA-API-SECRET-KEY")
            .and_then(|v| v.to_str().ok()),
        Some(TEST_API_SECRET),
    );
}

async fn handle_account(headers: HeaderMap) -> Response {
    assert_auth_headers(&headers);
    ([("content-type", "application/json")], HTTP_GET_ACCOUNT).into_response()
}

async fn handle_assets(
    headers: HeaderMap,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    assert_auth_headers(&headers);
    assert_eq!(params.get("status").map(String::as_str), Some("active"));
    assert_eq!(
        params.get("asset_class").map(String::as_str),
        Some("us_equity"),
    );
    ([("content-type", "application/json")], HTTP_GET_ASSETS).into_response()
}

async fn handle_submit_order(headers: HeaderMap, body: Bytes) -> Response {
    assert_auth_headers(&headers);
    let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(request["symbol"], "AAPL");
    assert_eq!(request["type"], "limit");
    assert_eq!(request["time_in_force"], "day");
    assert_eq!(request["qty"], "10");
    ([("content-type", "application/json")], HTTP_GET_ORDER).into_response()
}

#[tokio::test]
async fn client_get_account_sends_auth_headers_and_parses_response() {
    let base_url = spawn_server(Router::new().route("/v2/account", get(handle_account))).await;
    let client = authenticated_raw_client(&base_url);

    let account = client.get_account().await.unwrap();

    assert_eq!(account.id, "e6a5b6cd-1f27-4b3e-9c31-1a2c5d3f7c11");
    assert_eq!(account.status, "ACTIVE");
    assert_eq!(account.buying_power.as_deref(), Some("250000.00"));
}

#[tokio::test]
async fn client_request_instruments_parses_and_caches_equities() {
    let base_url = spawn_server(Router::new().route("/v2/assets", get(handle_assets))).await;
    let client = authenticated_client(&base_url);

    let instruments = client.request_instruments().await.unwrap();

    assert_eq!(instruments.len(), 2);
    assert_eq!(instruments[0].id().to_string(), "AAPL.ALPACA");

    let cached = client.get_instrument(&Ustr::from("AAPL")).unwrap();
    assert_eq!(cached.id(), instruments[0].id());
}

#[tokio::test]
async fn client_submit_order_posts_json_body() {
    let base_url = spawn_server(Router::new().route("/v2/orders", post(handle_submit_order))).await;
    let client = authenticated_client(&base_url);
    let params = PostOrderParamsBuilder::default()
        .symbol("AAPL")
        .qty("10")
        .side(AlpacaOrderSide::Buy)
        .order_type(AlpacaOrderType::Limit)
        .time_in_force(AlpacaTimeInForce::Day)
        .limit_price("189.05")
        .build()
        .unwrap();

    let order = client.submit_order(&params).await.unwrap();

    assert_eq!(order.id, "61e69015-8549-4bfd-b9c3-01e75843f47d");
    assert_eq!(order.status, AlpacaOrderStatus::Filled);
}

#[tokio::test]
async fn client_get_orders_sends_query_params() {
    async fn handler(Query(params): Query<std::collections::HashMap<String, String>>) -> Response {
        assert_eq!(params.get("status").map(String::as_str), Some("all"));
        assert_eq!(params.get("limit").map(String::as_str), Some("500"));
        ([("content-type", "application/json")], HTTP_GET_ORDERS).into_response()
    }

    let base_url = spawn_server(Router::new().route("/v2/orders", get(handler))).await;
    let client = authenticated_client(&base_url);
    let params = GetOrdersParamsBuilder::default()
        .status("all")
        .limit(500u32)
        .build()
        .unwrap();

    let orders = client.get_orders(&params).await.unwrap();

    assert_eq!(orders.len(), 2);
    assert_eq!(orders[1].status, AlpacaOrderStatus::PartiallyFilled);
}

#[tokio::test]
async fn client_cancel_order_accepts_204() {
    async fn handler(Path(order_id): Path<String>) -> Response {
        assert_eq!(order_id, "61e69015-8549-4bfd-b9c3-01e75843f47d");
        StatusCode::NO_CONTENT.into_response()
    }

    let base_url =
        spawn_server(Router::new().route("/v2/orders/{order_id}", delete(handler))).await;
    let client = authenticated_client(&base_url);

    client
        .cancel_order("61e69015-8549-4bfd-b9c3-01e75843f47d")
        .await
        .unwrap();
}

#[tokio::test]
async fn client_cancel_order_maps_422_to_venue_error() {
    async fn handler() -> Response {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            [("content-type", "application/json")],
            r#"{"code":42210000,"message":"order is not cancelable"}"#,
        )
            .into_response()
    }

    let base_url =
        spawn_server(Router::new().route("/v2/orders/{order_id}", delete(handler))).await;
    let client = authenticated_client(&base_url);

    let error = client.cancel_order("abc").await.unwrap_err();

    match error {
        AlpacaHttpError::Venue { code, message } => {
            assert_eq!(code, 42_210_000);
            assert!(message.contains("not cancelable"));
        }
        other => panic!("expected venue error, got {other:?}"),
    }
}

#[tokio::test]
async fn client_patch_order_returns_replacement() {
    async fn handler(Path(order_id): Path<String>, body: Bytes) -> Response {
        assert_eq!(order_id, "61e69015-8549-4bfd-b9c3-01e75843f47d");
        let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(request["qty"], "20");
        ([("content-type", "application/json")], HTTP_GET_ORDER).into_response()
    }

    let base_url = spawn_server(Router::new().route("/v2/orders/{order_id}", patch(handler))).await;
    let client = authenticated_client(&base_url);
    let params = PatchOrderParamsBuilder::default()
        .qty("20")
        .build()
        .unwrap();

    let order = client
        .patch_order("61e69015-8549-4bfd-b9c3-01e75843f47d", &params)
        .await
        .unwrap();

    assert_eq!(order.id, "61e69015-8549-4bfd-b9c3-01e75843f47d");
}

#[tokio::test]
async fn client_get_positions_parses_response() {
    async fn handler(headers: HeaderMap) -> Response {
        assert_auth_headers(&headers);
        ([("content-type", "application/json")], HTTP_GET_POSITIONS).into_response()
    }

    let base_url = spawn_server(Router::new().route("/v2/positions", get(handler))).await;
    let client = authenticated_client(&base_url);

    let positions = client.get_positions().await.unwrap();

    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].symbol, Ustr::from("AAPL"));
}

#[derive(Clone)]
struct PaginatedBarsState {
    calls: Arc<AtomicUsize>,
}

async fn handle_paginated_bars(
    State(state): State<PaginatedBarsState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let call = state.calls.fetch_add(1, Ordering::SeqCst);
    if call == 0 {
        assert!(!params.contains_key("page_token"));
        ([("content-type", "application/json")], HTTP_GET_STOCK_BARS).into_response()
    } else {
        assert_eq!(
            params.get("page_token").map(String::as_str),
            Some("QUFQTHxNfDE3Mz"),
        );
        (
            [("content-type", "application/json")],
            HTTP_GET_STOCK_BARS_PAGE2,
        )
            .into_response()
    }
}

#[tokio::test]
async fn client_get_stock_bars_paginated_follows_page_tokens() {
    let state = PaginatedBarsState {
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let base_url = spawn_server(
        Router::new()
            .route("/v2/stocks/bars", get(handle_paginated_bars))
            .with_state(state.clone()),
    )
    .await;
    let client = authenticated_client(&base_url);
    let params = GetStockBarsParamsBuilder::default()
        .symbols("AAPL")
        .timeframe("1Min")
        .build()
        .unwrap();

    let bars = client.get_stock_bars_paginated(&params).await.unwrap();

    assert_eq!(state.calls.load(Ordering::SeqCst), 2);
    assert_eq!(bars.get("AAPL").unwrap().len(), 3);
}

#[tokio::test]
async fn client_get_stock_trades_parses_response() {
    async fn handler() -> Response {
        (
            [("content-type", "application/json")],
            HTTP_GET_STOCK_TRADES,
        )
            .into_response()
    }

    let base_url = spawn_server(Router::new().route("/v2/stocks/trades", get(handler))).await;
    let client = authenticated_client(&base_url);
    let params = nautilus_alpaca::http::query::GetStockTradesParamsBuilder::default()
        .symbols("AAPL")
        .build()
        .unwrap();

    let trades = client.get_stock_trades_paginated(&params).await.unwrap();

    assert_eq!(trades.get("AAPL").unwrap().len(), 2);
}

#[tokio::test]
async fn client_get_stock_quotes_parses_response() {
    async fn handler() -> Response {
        (
            [("content-type", "application/json")],
            HTTP_GET_STOCK_QUOTES,
        )
            .into_response()
    }

    let base_url = spawn_server(Router::new().route("/v2/stocks/quotes", get(handler))).await;
    let client = authenticated_client(&base_url);
    let params = nautilus_alpaca::http::query::GetStockQuotesParamsBuilder::default()
        .symbols("AAPL")
        .build()
        .unwrap();

    let quotes = client.get_stock_quotes_paginated(&params).await.unwrap();

    assert_eq!(quotes.get("AAPL").unwrap().len(), 1);
}

#[derive(Clone)]
struct FlakyState {
    calls: Arc<AtomicUsize>,
}

async fn handle_flaky_account(State(state): State<FlakyState>, headers: HeaderMap) -> Response {
    assert_auth_headers(&headers);
    let call = state.calls.fetch_add(1, Ordering::SeqCst);
    if call == 0 {
        (StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response()
    } else {
        ([("content-type", "application/json")], HTTP_GET_ACCOUNT).into_response()
    }
}

#[tokio::test]
async fn client_retries_5xx_and_succeeds() {
    let state = FlakyState {
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let base_url = spawn_server(
        Router::new()
            .route("/v2/account", get(handle_flaky_account))
            .with_state(state.clone()),
    )
    .await;
    let client = authenticated_raw_client(&base_url);

    let account = client.get_account().await.unwrap();

    assert_eq!(state.calls.load(Ordering::SeqCst), 2);
    assert_eq!(account.status, "ACTIVE");
}

#[tokio::test]
async fn client_does_not_retry_venue_errors() {
    #[derive(Clone)]
    struct CountingState {
        calls: Arc<AtomicUsize>,
    }

    async fn handler(State(state): State<CountingState>) -> Response {
        state.calls.fetch_add(1, Ordering::SeqCst);
        (
            StatusCode::FORBIDDEN,
            [("content-type", "application/json")],
            HTTP_ERROR,
        )
            .into_response()
    }

    let state = CountingState {
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let base_url = spawn_server(
        Router::new()
            .route("/v2/account", get(handler))
            .with_state(state.clone()),
    )
    .await;
    let client = authenticated_raw_client(&base_url);

    let error = client.get_account().await.unwrap_err();

    assert_eq!(state.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(error, AlpacaHttpError::Venue { code, .. } if code == 42_210_000));
}

#[tokio::test]
async fn unauthenticated_client_can_fetch_market_data() {
    async fn handler(headers: HeaderMap) -> Response {
        assert!(headers.get("APCA-API-KEY-ID").is_none());
        (
            [("content-type", "application/json")],
            HTTP_GET_STOCK_BARS_PAGE2,
        )
            .into_response()
    }

    let base_url = spawn_server(Router::new().route("/v2/stocks/bars", get(handler))).await;
    let mut client = AlpacaHttpClient::new(
        AlpacaEnvironment::Paper,
        Some(base_url.clone()),
        Some(base_url),
        10,
        None,
    )
    .unwrap();
    // No-op: URLs already set via constructor; keep set_base_urls covered.
    let trading = client.base_url_trading().to_string();
    let data = client.base_url_data().to_string();
    client.set_base_urls(&trading, &data);

    let params = GetStockBarsParamsBuilder::default()
        .symbols("AAPL")
        .timeframe("1Min")
        .build()
        .unwrap();

    let response = client.get_stock_bars(&params).await.unwrap();

    assert_eq!(response.bars.get("AAPL").unwrap().len(), 1);
    assert_eq!(response.next_page_token, None);
}

#[tokio::test]
async fn client_get_assets_builder_defaults_produce_no_query() {
    async fn handler(Query(params): Query<std::collections::HashMap<String, String>>) -> Response {
        assert!(params.is_empty());
        ([("content-type", "application/json")], HTTP_GET_ASSETS).into_response()
    }

    let base_url = spawn_server(Router::new().route("/v2/assets", get(handler))).await;
    let client = authenticated_raw_client(&base_url);
    let params = GetAssetsParamsBuilder::default().build().unwrap();

    let assets = client.get_assets(&params).await.unwrap();

    assert_eq!(assets.len(), 2);
}
