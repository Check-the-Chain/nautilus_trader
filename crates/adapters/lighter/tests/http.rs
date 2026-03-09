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

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::{
    Router,
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
};
use nautilus_lighter::{
    config::Config, error::SdkError, http::client::LighterHttpClient,
    rest::client::LighterRestClient,
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::Mutex};

#[derive(Clone, Default)]
struct TestServerState {
    requests: Arc<Mutex<Vec<(String, String, Option<String>)>>>,
}

async fn spawn_server(router: Router) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    addr
}

fn test_config(addr: SocketAddr) -> Config {
    Config::for_network(false)
        .with_http_base_url(format!("http://{addr}"))
        .with_ws_base_url(format!("ws://{addr}/stream"))
}

async fn record_request(
    state: &TestServerState,
    path: &str,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
) {
    state.requests.lock().await.push((
        path.to_string(),
        serde_urlencoded::to_string(query).unwrap(),
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string),
    ));
}

async fn handle_order_books(State(state): State<TestServerState>, headers: HeaderMap) -> Response {
    record_request(&state, "/api/v1/orderBooks", &HashMap::new(), &headers).await;
    Json(json!({
        "code": 200,
        "order_books": [
            {"market_id": 1, "symbol": "BTC-USDC", "market_type": "perp"},
            {"market_id": 2048, "symbol": "ETH-USDC", "market_type": "spot"}
        ]
    }))
    .into_response()
}

async fn handle_asset_details(
    State(state): State<TestServerState>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/assetDetails", &HashMap::new(), &headers).await;
    Json(json!({
        "code": 200,
        "asset_details": [
            {"asset_id": 1, "symbol": "BTC", "decimals": 8, "index_price": "68421.4"},
            {"asset_id": 2, "symbol": "USDC", "decimals": 6, "index_price": "1.0"},
            {"asset_id": 3, "symbol": "ETH", "decimals": 8, "index_price": "3412.1"}
        ]
    }))
    .into_response()
}

async fn handle_order_book_details(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/orderBookDetails", &query, &headers).await;
    let payload = match query.get("market_id").map(String::as_str) {
        None => json!({
            "code": 200,
            "order_book_details": [{
                "market_id": 1,
                "symbol": "BTC-USDC",
                "market_type": "perp",
                "base_asset_id": 1,
                "quote_asset_id": 2,
                "price_decimals": 2,
                "size_decimals": 4,
                "supported_price_decimals": 2,
                "supported_size_decimals": 4,
                "default_initial_margin_fraction": 500,
                "maintenance_margin_fraction": 250
            }],
            "spot_order_book_details": [{
                "market_id": 2048,
                "symbol": "ETH-USDC",
                "market_type": "spot",
                "base_asset_id": 3,
                "quote_asset_id": 2,
                "price_decimals": 2,
                "size_decimals": 4,
                "supported_price_decimals": 2,
                "supported_size_decimals": 4
            }]
        }),
        Some("1") => json!({
            "code": 200,
            "order_book_details": [{
                "market_id": 1,
                "symbol": "BTC-USDC",
                "market_type": "perp",
                "base_asset_id": 1,
                "quote_asset_id": 2,
                "price_decimals": 2,
                "size_decimals": 4,
                "supported_price_decimals": 2,
                "supported_size_decimals": 4,
                "default_initial_margin_fraction": 500,
                "maintenance_margin_fraction": 250
            }],
            "spot_order_book_details": []
        }),
        Some("2048") => json!({
            "code": 200,
            "order_book_details": [],
            "spot_order_book_details": [{
                "market_id": 2048,
                "symbol": "ETH-USDC",
                "market_type": "spot",
                "base_asset_id": 3,
                "quote_asset_id": 2,
                "price_decimals": 2,
                "size_decimals": 4,
                "supported_price_decimals": 2,
                "supported_size_decimals": 4
            }]
        }),
        _ => json!({"code": 404, "message": "unknown market"}),
    };
    Json(payload).into_response()
}

async fn handle_recent_trades(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/recentTrades", &query, &headers).await;
    Json(json!({
    "code": 200,
        "trades": [{
            "trade_id": 1,
            "market_id": 1,
            "size": "0.1",
            "price": "100000.0",
            "is_maker_ask": false,
            "timestamp": 1704067200000i64
        }]
    }))
    .into_response()
}

async fn handle_account(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/account", &query, &headers).await;
    Json(json!({
        "code": 200,
        "accounts": [{
            "account_index": 7,
            "assets": [
                {"asset_id": 2, "symbol": "USDC", "balance": "100000", "locked_balance": "10"}
            ],
            "positions": [{
                "market_id": 1,
                "symbol": "BTC-USDC",
                "initial_margin_fraction": "500",
                "open_order_count": 0,
                "pending_order_count": 0,
                "position_tied_order_count": 0,
                "sign": 1,
                "position": "0.5",
                "avg_entry_price": "100000.0",
                "position_value": "50000.0",
                "unrealized_pnl": "10.0",
                "realized_pnl": "5.0",
                "liquidation_price": "80000.0",
                "margin_mode": 0,
                "allocated_margin": "1000.0"
            }]
        }]
    }))
    .into_response()
}

async fn handle_inactive_orders(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/accountInactiveOrders", &query, &headers).await;
    Json(json!({"code": 200, "orders": [], "cursor": null})).into_response()
}

async fn handle_l1_metadata(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/l1Metadata", &query, &headers).await;
    Json(json!({
        "code": 200,
        "l1_address": query.get("l1_address").cloned().unwrap_or_default(),
        "nickname": "primary"
    }))
    .into_response()
}

async fn handle_public_pools_metadata(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/publicPoolsMetadata", &query, &headers).await;
    Json(json!({
        "code": 200,
        "pools": [{
            "public_pool_index": 11,
            "account_index": query.get("account_index").and_then(|value| value.parse::<i64>().ok()).unwrap_or(7),
            "info": {
                "operator_fee": "10",
                "min_operator_share_rate": "5"
            }
        }]
    }))
    .into_response()
}

async fn handle_tx_from_l1_tx_hash(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/txFromL1TxHash", &query, &headers).await;
    Json(json!({
        "code": 200,
        "hash": query.get("hash").cloned().unwrap_or_default(),
        "status": 1
    }))
    .into_response()
}

async fn handle_tokens(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/tokens", &query, &headers).await;
    Json(json!({
        "code": 200,
        "tokens": [{
            "token_id": 11,
            "name": "reporting"
        }]
    }))
    .into_response()
}

async fn handle_tokens_create(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let form: HashMap<String, String> = serde_urlencoded::from_bytes(&body).unwrap();
    record_request(&state, "/api/v1/tokens/create", &form, &headers).await;
    Json(json!({
        "code": 200,
        "token_id": 11,
        "api_token": "ro:7:all:1767139200:deadbeef"
    }))
    .into_response()
}

async fn handle_tokens_revoke(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let form: HashMap<String, String> = serde_urlencoded::from_bytes(&body).unwrap();
    record_request(&state, "/api/v1/tokens/revoke", &form, &headers).await;
    Json(json!({"code": 200, "message": "ok"})).into_response()
}

async fn handle_notification_ack(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let form: HashMap<String, String> = serde_urlencoded::from_bytes(&body).unwrap();
    record_request(&state, "/api/v1/notification/ack", &form, &headers).await;
    Json(json!({"code": 200, "message": "ok"})).into_response()
}

async fn handle_referral_user_referrals(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/referral/userReferrals", &query, &headers).await;
    Json(json!({
        "code": 200,
        "cursor": 2,
        "referrals": [{
            "l1_address": query.get("l1_address").cloned().unwrap_or_default(),
            "referral_code": "LIGHTER7",
            "used_at": 1704067200000i64
        }]
    }))
    .into_response()
}

async fn handle_referral_get(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/referral/get", &query, &headers).await;
    Json(json!({
        "code": 200,
        "referral_code": "LIGHTER7",
        "remaining_usage": 3
    }))
    .into_response()
}

async fn handle_referral_create(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let form: HashMap<String, String> = serde_urlencoded::from_bytes(&body).unwrap();
    record_request(&state, "/api/v1/referral/create", &form, &headers).await;
    Json(json!({
        "code": 200,
        "referral_code": "LIGHTER7",
        "remaining_usage": 3
    }))
    .into_response()
}

async fn handle_referral_update(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let form: HashMap<String, String> = serde_urlencoded::from_bytes(&body).unwrap();
    record_request(&state, "/api/v1/referral/update", &form, &headers).await;
    Json(json!({"code": 200, "success": true})).into_response()
}

async fn handle_referral_kickback_update(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let form: HashMap<String, String> = serde_urlencoded::from_bytes(&body).unwrap();
    record_request(&state, "/api/v1/referral/kickback/update", &form, &headers).await;
    Json(json!({"code": 200, "success": true})).into_response()
}

async fn handle_referral_use(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let form: HashMap<String, String> = serde_urlencoded::from_bytes(&body).unwrap();
    record_request(&state, "/api/v1/referral/use", &form, &headers).await;
    Json(json!({"code": 200, "message": "ok"})).into_response()
}

async fn handle_liquidations(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/liquidations", &query, &headers).await;
    Json(json!({
        "code": 200,
        "liquidations": [{
            "id": 1,
            "market_id": query.get("market_id").and_then(|value| value.parse::<i64>().ok()).unwrap_or(1),
            "type": "partial",
            "trade": {
                "price": "100000.0",
                "size": "0.1000",
                "taker_fee": "5.0",
                "maker_fee": "2.5",
                "transaction_time": 1704067200000i64
            },
            "info": {
                "positions": [{
                    "market_id": 1,
                    "symbol": "BTC-USDC",
                    "initial_margin_fraction": "500",
                    "open_order_count": 0,
                    "pending_order_count": 0,
                    "position_tied_order_count": 0,
                    "sign": 1,
                    "position": "0.5",
                    "avg_entry_price": "100000.0",
                    "position_value": "50000.0",
                    "unrealized_pnl": "10.0",
                    "realized_pnl": "5.0",
                    "liquidation_price": "80000.0",
                    "margin_mode": 0,
                    "allocated_margin": "1000.0"
                }],
                "risk_info_before": {
                    "cross_risk_parameters": {
                        "market_id": 1,
                        "collateral": "100000.0",
                        "total_account_value": "101000.0",
                        "initial_margin_req": "5000.0",
                        "maintenance_margin_req": "2500.0",
                        "close_out_margin_req": "2000.0"
                    },
                    "isolated_risk_parameters": []
                },
                "risk_info_after": {
                    "cross_risk_parameters": {
                        "market_id": 1,
                        "collateral": "99000.0",
                        "total_account_value": "100000.0",
                        "initial_margin_req": "4500.0",
                        "maintenance_margin_req": "2250.0",
                        "close_out_margin_req": "1800.0"
                    },
                    "isolated_risk_parameters": []
                },
                "mark_prices": {
                    "1": 100001.0
                }
            },
            "executed_at": 1704067260000i64
        }],
        "next_cursor": "cursor-2"
    }))
    .into_response()
}

async fn handle_transfer_fee_info(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/transferFeeInfo", &query, &headers).await;
    Json(json!({"code": 200, "transfer_fee_usdc": 15})).into_response()
}

async fn handle_withdrawal_delay(
    State(state): State<TestServerState>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/withdrawalDelay", &HashMap::new(), &headers).await;
    Json(json!({"seconds": 86400})).into_response()
}

async fn handle_announcement(State(state): State<TestServerState>, headers: HeaderMap) -> Response {
    record_request(&state, "/api/v1/announcement", &HashMap::new(), &headers).await;
    Json(json!({"code": 200, "announcements": [{"title": "listing"}]})).into_response()
}

async fn handle_status(State(state): State<TestServerState>, headers: HeaderMap) -> Response {
    record_request(&state, "/", &HashMap::new(), &headers).await;
    Json(json!({"status": 1, "network_id": 1, "timestamp": 1704067200})).into_response()
}

async fn handle_system_config(
    State(state): State<TestServerState>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/systemConfig", &HashMap::new(), &headers).await;
    Json(json!({"code": 200, "liquidity_pool_index": 1})).into_response()
}

async fn handle_exchange_metrics(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/exchangeMetrics", &query, &headers).await;
    Json(json!({"code": 200, "metrics": [{"period": query.get("period"), "kind": query.get("kind")}] }))
        .into_response()
}

async fn handle_execute_stats(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/executeStats", &query, &headers).await;
    Json(json!({"code": 200, "period": query.get("period"), "result": {"success": 99.9}}))
        .into_response()
}

async fn handle_layer1_basic_info(
    State(state): State<TestServerState>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/layer1BasicInfo", &HashMap::new(), &headers).await;
    Json(json!({"code": 200, "validator_info": {"status": "ok"}})).into_response()
}

async fn handle_info(State(state): State<TestServerState>, headers: HeaderMap) -> Response {
    record_request(&state, "/info", &HashMap::new(), &headers).await;
    Json(json!({"address": "0xcontract", "contract_address": "0xcontract"})).into_response()
}

async fn handle_create_intent_address(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let form: HashMap<String, String> = serde_urlencoded::from_bytes(&body).unwrap();
    record_request(&state, "/api/v1/createIntentAddress", &form, &headers).await;
    Json(json!({"code": 200, "intent_address": "0xintent"})).into_response()
}

async fn handle_fastbridge_info(
    State(state): State<TestServerState>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/fastbridge/info", &HashMap::new(), &headers).await;
    Json(json!({"code": 200, "fast_bridge_limit": "50000"})).into_response()
}

async fn handle_deposit_latest(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/deposit/latest", &query, &headers).await;
    Json(json!({
        "code": 200,
        "source": "bridge",
        "intent_address": "0xintent",
        "status": "settled",
        "description": "done"
    }))
    .into_response()
}

async fn handle_deposit_networks(
    State(state): State<TestServerState>,
    headers: HeaderMap,
) -> Response {
    record_request(
        &state,
        "/api/v1/deposit/networks",
        &HashMap::new(),
        &headers,
    )
    .await;
    Json(json!({"code": 200, "networks": [{"chain_id": 1, "name": "Ethereum"}]})).into_response()
}

async fn handle_fastwithdraw_info(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/fastwithdraw/info", &query, &headers).await;
    Json(json!({
        "code": 200,
        "to_account_index": 17,
        "withdraw_limit": "1000",
        "max_withdrawal_amount": "800"
    }))
    .into_response()
}

async fn handle_fastwithdraw(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let form: HashMap<String, String> = serde_urlencoded::from_bytes(&body).unwrap();
    record_request(&state, "/api/v1/fastwithdraw", &form, &headers).await;
    Json(json!({"code": 200, "message": "ok"})).into_response()
}

async fn handle_lease_options(
    State(state): State<TestServerState>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/leaseOptions", &HashMap::new(), &headers).await;
    Json(json!({
        "code": 200,
        "options": [{"duration_days": 30, "apr_bps": 100}],
        "lit_incentives_account_index": 99
    }))
    .into_response()
}

async fn handle_leases(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/leases", &query, &headers).await;
    Json(json!({"code": 200, "leases": [{"lease_id": 1}], "next_cursor": null})).into_response()
}

async fn handle_lit_lease(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let form: HashMap<String, String> = serde_urlencoded::from_bytes(&body).unwrap();
    record_request(&state, "/api/v1/litLease", &form, &headers).await;
    Json(json!({"code": 200, "tx_hash": "0xlease"})).into_response()
}

async fn handle_export(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/export", &query, &headers).await;
    Json(json!({"code": 200, "data_url": "https://example.com/export.csv"})).into_response()
}

async fn handle_txs(
    State(state): State<TestServerState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record_request(&state, "/api/v1/txs", &query, &headers).await;
    Json(json!({"code": 200, "txs": [{"hash": "0xabc"}]})).into_response()
}

async fn handle_funding_rates_error() -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        Json(json!({"code": 429, "message": "rate limit exceeded"})),
    )
        .into_response()
}

fn build_router(state: TestServerState) -> Router {
    Router::new()
        .route("/", get(handle_status))
        .route("/api/v1/orderBooks", get(handle_order_books))
        .route("/api/v1/assetDetails", get(handle_asset_details))
        .route("/api/v1/orderBookDetails", get(handle_order_book_details))
        .route("/api/v1/recentTrades", get(handle_recent_trades))
        .route("/api/v1/systemConfig", get(handle_system_config))
        .route("/api/v1/account", get(handle_account))
        .route("/api/v1/accountInactiveOrders", get(handle_inactive_orders))
        .route("/api/v1/layer1BasicInfo", get(handle_layer1_basic_info))
        .route("/info", get(handle_info))
        .route("/api/v1/l1Metadata", get(handle_l1_metadata))
        .route(
            "/api/v1/publicPoolsMetadata",
            get(handle_public_pools_metadata),
        )
        .route("/api/v1/txFromL1TxHash", get(handle_tx_from_l1_tx_hash))
        .route("/api/v1/tokens", get(handle_tokens))
        .route("/api/v1/tokens/create", post(handle_tokens_create))
        .route("/api/v1/tokens/revoke", post(handle_tokens_revoke))
        .route("/api/v1/notification/ack", post(handle_notification_ack))
        .route(
            "/api/v1/referral/userReferrals",
            get(handle_referral_user_referrals),
        )
        .route("/api/v1/referral/get", get(handle_referral_get))
        .route("/api/v1/referral/create", post(handle_referral_create))
        .route("/api/v1/referral/update", post(handle_referral_update))
        .route(
            "/api/v1/referral/kickback/update",
            post(handle_referral_kickback_update),
        )
        .route("/api/v1/referral/use", post(handle_referral_use))
        .route("/api/v1/liquidations", get(handle_liquidations))
        .route("/api/v1/transferFeeInfo", get(handle_transfer_fee_info))
        .route("/api/v1/withdrawalDelay", get(handle_withdrawal_delay))
        .route("/api/v1/announcement", get(handle_announcement))
        .route("/api/v1/exchangeMetrics", get(handle_exchange_metrics))
        .route("/api/v1/executeStats", get(handle_execute_stats))
        .route(
            "/api/v1/createIntentAddress",
            post(handle_create_intent_address),
        )
        .route("/api/v1/fastbridge/info", get(handle_fastbridge_info))
        .route("/api/v1/deposit/latest", get(handle_deposit_latest))
        .route("/api/v1/deposit/networks", get(handle_deposit_networks))
        .route("/api/v1/fastwithdraw/info", get(handle_fastwithdraw_info))
        .route("/api/v1/fastwithdraw", post(handle_fastwithdraw))
        .route("/api/v1/leaseOptions", get(handle_lease_options))
        .route("/api/v1/leases", get(handle_leases))
        .route("/api/v1/litLease", post(handle_lit_lease))
        .route("/api/v1/export", get(handle_export))
        .route("/api/v1/txs", get(handle_txs))
        .route("/api/v1/funding-rates", get(handle_funding_rates_error))
        .with_state(state)
}

#[tokio::test]
async fn test_load_market_metadata_json_flattens_perp_and_spot_details() {
    let state = TestServerState::default();
    let addr = spawn_server(build_router(state.clone())).await;
    let client = LighterHttpClient::new_public(test_config(addr)).unwrap();

    let payload = client.load_market_metadata_json().await.unwrap();
    let payload: Value = serde_json::from_str(&payload).unwrap();

    assert_eq!(payload["assets"].as_array().unwrap().len(), 3);
    assert_eq!(payload["details"].as_array().unwrap().len(), 2);
    assert_eq!(payload["details"][0]["market_id"], 1);
    assert_eq!(payload["details"][1]["market_id"], 2048);

    let requests = state.requests.lock().await.clone();
    assert!(
        requests
            .iter()
            .any(|(path, _, _)| path == "/api/v1/orderBooks")
    );
    assert!(
        requests
            .iter()
            .any(|(path, _, _)| path == "/api/v1/assetDetails")
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(path, _, _)| path == "/api/v1/orderBookDetails")
            .count(),
        1,
    );
}

#[tokio::test]
async fn test_rest_client_encodes_queries_and_auth_headers() {
    let state = TestServerState::default();
    let addr = spawn_server(build_router(state.clone())).await;
    let rest = LighterRestClient::new(&test_config(addr)).unwrap();

    let trades = rest.get_recent_trades(1, 50).await.unwrap();
    let account = rest
        .get_detailed_account_by_index(7, "Bearer lighter-auth")
        .await
        .unwrap();
    let inactive = rest
        .get_account_inactive_orders(7, 1, "Bearer lighter-auth", Some("cursor-1"))
        .await
        .unwrap();

    assert_eq!(trades.trades.len(), 1);
    assert_eq!(account.accounts.len(), 1);
    assert!(inactive.orders.is_empty());

    let requests = state.requests.lock().await.clone();
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/recentTrades"
            && query.contains("market_id=1")
            && query.contains("limit=50")
            && auth.is_none()
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/account"
            && query.contains("by=index")
            && query.contains("value=7")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/accountInactiveOrders"
            && query.contains("account_index=7")
            && query.contains("market_id=1")
            && query.contains("cursor=cursor-1")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
}

#[tokio::test]
async fn test_rest_client_surfaces_api_errors() {
    let state = TestServerState::default();
    let addr = spawn_server(build_router(state)).await;
    let rest = LighterRestClient::new(&test_config(addr)).unwrap();

    let error = rest.get_funding_rates(1, None).await.unwrap_err();

    match error {
        SdkError::Api { code, message } => {
            assert_eq!(code, 429);
            assert_eq!(message, "rate limit exceeded");
        }
        other => panic!("unexpected error variant: {other:?}"),
    }
}

#[tokio::test]
async fn test_rest_client_forwards_token_and_notification_requests() {
    let state = TestServerState::default();
    let addr = spawn_server(build_router(state.clone())).await;
    let rest = LighterRestClient::new(&test_config(addr)).unwrap();

    let tokens = rest.get_tokens(7, "Bearer lighter-auth").await.unwrap();
    let created = rest
        .create_token(
            "reporting",
            7,
            1_767_139_200,
            true,
            "read.*",
            "Bearer lighter-auth",
        )
        .await
        .unwrap();
    let revoked = rest
        .revoke_token(11, 7, "Bearer lighter-auth")
        .await
        .unwrap();
    let acked = rest
        .ack_notification("notif-1", 7, "Bearer lighter-auth")
        .await
        .unwrap();

    assert_eq!(tokens["tokens"][0]["token_id"], 11);
    assert_eq!(created["token_id"], 11);
    assert_eq!(revoked["code"], 200);
    assert_eq!(acked["code"], 200);

    let requests = state.requests.lock().await.clone();
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/tokens"
            && query.contains("account_index=7")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/tokens/create"
            && query.contains("name=reporting")
            && query.contains("account_index=7")
            && query.contains("expiry=1767139200")
            && query.contains("sub_account_access=true")
            && (query.contains("scopes=read.%2A") || query.contains("scopes=read.*"))
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/tokens/revoke"
            && query.contains("token_id=11")
            && query.contains("account_index=7")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/notification/ack"
            && query.contains("notif_id=notif-1")
            && query.contains("account_index=7")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
}

#[tokio::test]
async fn test_rest_client_supports_pool_l1_and_l1_tx_queries() {
    let state = TestServerState::default();
    let addr = spawn_server(build_router(state.clone())).await;
    let rest = LighterRestClient::new(&test_config(addr)).unwrap();

    let l1 = rest
        .get_l1_metadata("0xabc", Some("Bearer lighter-auth"))
        .await
        .unwrap();
    let pools = rest
        .get_public_pools_metadata("account_index", 5, 25, Some(7), Some("Bearer lighter-auth"))
        .await
        .unwrap();
    let tx = rest.get_tx_from_l1_tx_hash("0xl1").await.unwrap();

    assert_eq!(l1["l1_address"], "0xabc");
    assert_eq!(pools.pools.len(), 1);
    assert_eq!(tx["hash"], "0xl1");

    let requests = state.requests.lock().await.clone();
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/l1Metadata"
            && query.contains("l1_address=0xabc")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/publicPoolsMetadata"
            && query.contains("filter=account_index")
            && query.contains("index=5")
            && query.contains("limit=25")
            && query.contains("account_index=7")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/txFromL1TxHash" && query.contains("hash=0xl1") && auth.is_none()
    }));
}

#[tokio::test]
async fn test_rest_client_supports_referral_and_account_info_requests() {
    let state = TestServerState::default();
    let addr = spawn_server(build_router(state.clone())).await;
    let rest = LighterRestClient::new(&test_config(addr)).unwrap();

    let referrals = rest
        .get_user_referrals("0xabc", Some(2), Some("Bearer lighter-auth"))
        .await
        .unwrap();
    let referral = rest
        .get_referral_code(7, Some("Bearer lighter-auth"))
        .await
        .unwrap();
    let created = rest
        .create_referral_code(7, Some("Bearer lighter-auth"))
        .await
        .unwrap();
    let updated = rest
        .update_referral_code(7, "LIGHTER7", Some("Bearer lighter-auth"))
        .await
        .unwrap();
    let kickback = rest
        .update_referral_kickback(7, 25.0, Some("Bearer lighter-auth"))
        .await
        .unwrap();
    let used = rest
        .use_referral_code(
            "0xabc",
            "LIGHTER7",
            "x_user",
            Some("discord#7"),
            None,
            None,
            Some("Bearer lighter-auth"),
        )
        .await
        .unwrap();
    let liquidations = rest
        .get_liquidations(
            7,
            25,
            Some(1),
            Some("cursor-1"),
            Some("Bearer lighter-auth"),
        )
        .await
        .unwrap();
    let transfer_fee = rest
        .get_transfer_fee_info(7, Some(9), Some("Bearer lighter-auth"))
        .await
        .unwrap();
    let withdrawal_delay = rest.get_withdrawal_delay().await.unwrap();

    assert_eq!(referrals.referrals.len(), 1);
    assert_eq!(referral.referral_code.as_deref(), Some("LIGHTER7"));
    assert_eq!(created.remaining_usage, Some(3));
    assert_eq!(updated.success, Some(true));
    assert_eq!(kickback.success, Some(true));
    assert_eq!(used.code, 200);
    assert_eq!(liquidations.liquidations.len(), 1);
    assert_eq!(transfer_fee.transfer_fee_usdc, Some(15));
    assert_eq!(withdrawal_delay.seconds, Some(86400));

    let requests = state.requests.lock().await.clone();
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/referral/userReferrals"
            && query.contains("l1_address=0xabc")
            && query.contains("cursor=2")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/referral/get"
            && query.contains("account_index=7")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/referral/create"
            && query.contains("account_index=7")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/referral/update"
            && query.contains("account_index=7")
            && query.contains("new_referral_code=LIGHTER7")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/referral/kickback/update"
            && query.contains("account_index=7")
            && query.contains("kickback_percentage=25")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/referral/use"
            && query.contains("l1_address=0xabc")
            && query.contains("referral_code=LIGHTER7")
            && query.contains("x=x_user")
            && query.contains("discord=discord%237")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/liquidations"
            && query.contains("account_index=7")
            && query.contains("limit=25")
            && query.contains("market_id=1")
            && query.contains("cursor=cursor-1")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/transferFeeInfo"
            && query.contains("account_index=7")
            && query.contains("to_account_index=9")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(
        requests
            .iter()
            .any(|(path, _, auth)| { path == "/api/v1/withdrawalDelay" && auth.is_none() })
    );
}

#[tokio::test]
async fn test_rest_client_supports_public_info_bridge_and_lease_requests() {
    let state = TestServerState::default();
    let addr = spawn_server(build_router(state.clone())).await;
    let rest = LighterRestClient::new(&test_config(addr)).unwrap();

    let announcements = rest.get_announcements().await.unwrap();
    let metrics = rest
        .get_exchange_metrics("d", "volume", Some("byMarket"), Some("BTC-USDC"))
        .await
        .unwrap();
    let execute_stats = rest.get_execute_stats("d").await.unwrap();
    let intent = rest
        .create_intent_address("1", "0xabc", "1000", true)
        .await
        .unwrap();
    let fast_bridge = rest.get_fast_bridge_info().await.unwrap();
    let deposit_latest = rest.get_deposit_latest("0xabc").await.unwrap();
    let deposit_networks = rest.get_deposit_networks().await.unwrap();
    let fast_withdraw_info = rest
        .get_fast_withdraw_info(7, "Bearer lighter-auth")
        .await
        .unwrap();
    let fast_withdraw = rest
        .fast_withdraw("{\"nonce\":1}", "0xdef", "Bearer lighter-auth")
        .await
        .unwrap();
    let lease_options = rest.get_lease_options().await.unwrap();
    let leases = rest
        .get_leases(7, "Bearer lighter-auth", Some("cursor-1"), Some(25))
        .await
        .unwrap();
    let lit_lease = rest
        .lit_lease(
            "{\"nonce\":2}",
            Some("2500"),
            Some(30),
            "Bearer lighter-auth",
        )
        .await
        .unwrap();

    assert_eq!(announcements.announcements.len(), 1);
    assert_eq!(metrics["code"], 200);
    assert_eq!(execute_stats["code"], 200);
    assert_eq!(intent["intent_address"], "0xintent");
    assert_eq!(fast_bridge["code"], 200);
    assert_eq!(deposit_latest["code"], 200);
    assert_eq!(deposit_networks["code"], 200);
    assert_eq!(fast_withdraw_info["code"], 200);
    assert_eq!(fast_withdraw["code"], 200);
    assert_eq!(lease_options["code"], 200);
    assert_eq!(leases["code"], 200);
    assert_eq!(lit_lease["tx_hash"], "0xlease");

    let requests = state.requests.lock().await.clone();
    assert!(
        requests
            .iter()
            .any(|(path, _, auth)| { path == "/api/v1/announcement" && auth.is_none() })
    );
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/exchangeMetrics"
            && query.contains("period=d")
            && query.contains("kind=volume")
            && query.contains("filter=byMarket")
            && query.contains("value=BTC-USDC")
            && auth.is_none()
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/executeStats" && query.contains("period=d") && auth.is_none()
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/createIntentAddress"
            && query.contains("chain_id=1")
            && query.contains("from_addr=0xabc")
            && query.contains("amount=1000")
            && query.contains("is_external_deposit=true")
            && auth.is_none()
    }));
    assert!(
        requests
            .iter()
            .any(|(path, _, auth)| { path == "/api/v1/fastbridge/info" && auth.is_none() })
    );
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/deposit/latest" && query.contains("l1_address=0xabc") && auth.is_none()
    }));
    assert!(
        requests
            .iter()
            .any(|(path, _, auth)| { path == "/api/v1/deposit/networks" && auth.is_none() })
    );
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/fastwithdraw/info"
            && query.contains("account_index=7")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/fastwithdraw"
            && query.contains("tx_info=%7B%22nonce%22%3A1%7D")
            && query.contains("to_address=0xdef")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(
        requests
            .iter()
            .any(|(path, _, auth)| { path == "/api/v1/leaseOptions" && auth.is_none() })
    );
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/leases"
            && query.contains("account_index=7")
            && query.contains("cursor=cursor-1")
            && query.contains("limit=25")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/litLease"
            && query.contains("tx_info=%7B%22nonce%22%3A2%7D")
            && query.contains("lease_amount=2500")
            && query.contains("duration_days=30")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
}

#[tokio::test]
async fn test_rest_client_supports_status_export_and_tx_history_requests() {
    let state = TestServerState::default();
    let addr = spawn_server(build_router(state.clone())).await;
    let rest = LighterRestClient::new(&test_config(addr)).unwrap();

    let status = rest.get_status().await.unwrap();
    let system_config = rest.get_system_config().await.unwrap();
    let layer1_basic_info = rest.get_layer1_basic_info().await.unwrap();
    let info = rest.get_zk_lighter_info().await.unwrap();
    let export = rest
        .get_export(
            "trade",
            Some("Bearer lighter-auth"),
            Some(7),
            Some(1),
            Some(10),
            Some(20),
            Some("long"),
            Some("maker"),
            Some("trade"),
        )
        .await
        .unwrap();
    let txs = rest.get_txs(25, Some(10)).await.unwrap();

    assert_eq!(status["status"], 1);
    assert_eq!(system_config["code"], 200);
    assert_eq!(layer1_basic_info["code"], 200);
    assert_eq!(info.address.as_deref(), Some("0xcontract"));
    assert_eq!(export["data_url"], "https://example.com/export.csv");
    assert_eq!(txs["code"], 200);

    let requests = state.requests.lock().await.clone();
    assert!(
        requests
            .iter()
            .any(|(path, _, auth)| { path == "/" && auth.is_none() })
    );
    assert!(
        requests
            .iter()
            .any(|(path, _, auth)| { path == "/api/v1/systemConfig" && auth.is_none() })
    );
    assert!(
        requests
            .iter()
            .any(|(path, _, auth)| { path == "/api/v1/layer1BasicInfo" && auth.is_none() })
    );
    assert!(
        requests
            .iter()
            .any(|(path, _, auth)| { path == "/info" && auth.is_none() })
    );
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/export"
            && query.contains("type=trade")
            && query.contains("account_index=7")
            && query.contains("market_id=1")
            && query.contains("start_timestamp=10")
            && query.contains("end_timestamp=20")
            && query.contains("side=long")
            && query.contains("role=maker")
            && query.contains("trade_type=trade")
            && auth.as_deref() == Some("Bearer lighter-auth")
    }));
    assert!(requests.iter().any(|(path, query, auth)| {
        path == "/api/v1/txs"
            && query.contains("limit=25")
            && query.contains("index=10")
            && auth.is_none()
    }));
}
