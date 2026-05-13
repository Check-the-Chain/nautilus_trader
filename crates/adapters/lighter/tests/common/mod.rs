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

#![allow(dead_code)]

use std::{collections::HashMap, net::SocketAddr, time::Duration};

use axum::{
    Router,
    extract::{
        Query,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::{IntoResponse, Json, Response},
    routing::get,
};
use futures_util::StreamExt;
use nautilus_lighter::{
    config::{Config, LighterDataClientConfig},
    http::client::LighterHttpClient,
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, time::sleep};

pub const TEST_MARKET_ID: i64 = 1;
pub const TEST_ACCOUNT_INDEX: i64 = 7;
pub const TEST_INSTRUMENT_ID: &str = "BTC-USDC-PERP.LIGHTER";

pub async fn start_mock_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router()).await.unwrap();
    });
    wait_for_server(addr).await;
    addr
}

pub fn data_client_config(addr: SocketAddr) -> LighterDataClientConfig {
    LighterDataClientConfig {
        base_url_http: Some(format!("http://{addr}")),
        base_url_ws: Some(format!("ws://{addr}/stream")),
        ..LighterDataClientConfig::default()
    }
}

pub fn http_config(addr: SocketAddr) -> Config {
    Config::for_network(false)
        .with_http_base_url(format!("http://{addr}"))
        .with_ws_base_url(format!("ws://{addr}/stream"))
}

pub fn public_http_client(addr: SocketAddr) -> LighterHttpClient {
    LighterHttpClient::new_public(http_config(addr)).unwrap()
}

fn router() -> Router {
    Router::new()
        .route("/health", get(handle_health))
        .route("/stream", get(handle_ws_upgrade))
        .route("/api/v1/orderBooks", get(handle_order_books))
        .route("/api/v1/assetDetails", get(handle_asset_details))
        .route("/api/v1/orderBookDetails", get(handle_order_book_details))
        .route("/api/v1/orderBookOrders", get(handle_order_book_orders))
        .route("/api/v1/recentTrades", get(handle_recent_trades))
        .route("/api/v1/candles", get(handle_candles))
        .route("/api/v1/funding-rates", get(handle_funding_rates))
}

async fn wait_for_server(addr: SocketAddr) {
    let health_url = format!("http://{addr}/health");
    for _ in 0..50 {
        if let Ok(response) = reqwest::get(&health_url).await
            && response.status().is_success()
        {
            return;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("mock server did not start");
}

async fn handle_health() -> impl IntoResponse {
    axum::http::StatusCode::OK
}

async fn handle_order_books() -> impl IntoResponse {
    Json(json!({
        "code": 200,
        "order_books": [
            {"market_id": TEST_MARKET_ID, "symbol": "BTC-USDC", "market_type": "perp"}
        ]
    }))
}

async fn handle_asset_details() -> impl IntoResponse {
    Json(json!({
        "code": 200,
        "asset_details": [
            {"asset_id": 1, "symbol": "BTC", "balance": "0", "locked_balance": "0"},
            {"asset_id": 2, "symbol": "USDC", "balance": "0", "locked_balance": "0"}
        ]
    }))
}

async fn handle_order_book_details(
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let payload = if !query.contains_key("market_id")
        || query.get("market_id").map(String::as_str) == Some("1")
    {
        json!({
            "code": 200,
            "order_book_details": [{
                "market_id": TEST_MARKET_ID,
                "symbol": "BTC-USDC",
                "market_type": "perp",
                "base_asset_id": 1,
                "quote_asset_id": 2,
                "price_decimals": 2,
                "size_decimals": 4,
                "supported_price_decimals": 2,
                "supported_size_decimals": 4,
                "min_base_amount": "0.0001",
                "maker_fee": "0.0002",
                "taker_fee": "0.0005",
                "default_initial_margin_fraction": 500,
                "maintenance_margin_fraction": 250
            }],
            "spot_order_book_details": []
        })
    } else {
        json!({"code": 404, "message": "unknown market"})
    };
    Json(payload)
}

async fn handle_order_book_orders() -> impl IntoResponse {
    Json(json!({
        "code": 200,
        "total_asks": 1,
        "asks": [{"price": "100010.00", "remaining_base_amount": "0.5000"}],
        "total_bids": 1,
        "bids": [{"price": "100000.00", "remaining_base_amount": "0.7000"}]
    }))
}

async fn handle_recent_trades() -> impl IntoResponse {
    Json(json!({
        "code": 200,
        "trades": [{
            "trade_id": 12345,
            "market_id": TEST_MARKET_ID,
            "size": "0.1000",
            "price": "100005.00",
            "is_maker_ask": false,
            "timestamp": 1704067260000i64
        }]
    }))
}

async fn handle_candles() -> impl IntoResponse {
    Json(json!({
        "code": 200,
        "candles": [{
            "open": "100000.00",
            "high": "100020.00",
            "low": "99990.00",
            "close": "100010.00",
            "volume": "12.5",
            "timestamp": 1704067200000i64
        }]
    }))
}

async fn handle_funding_rates() -> impl IntoResponse {
    Json(json!({
        "code": 200,
        "funding_rates": [{
            "market_id": TEST_MARKET_ID,
            "mark_price": "100005.00",
            "index_price": "100000.00",
            "funding_rate": "0.0001",
            "settlement_time": 1704067800000i64
        }]
    }))
}

async fn handle_ws_upgrade(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_ws_socket)
}

async fn handle_ws_socket(mut socket: WebSocket) {
    while let Some(message) = socket.next().await {
        let Ok(message) = message else {
            break;
        };

        match message {
            Message::Text(text) => {
                let Ok(payload) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let msg_type = payload.get("type").and_then(Value::as_str);
                if msg_type == Some("pong") {
                    continue;
                }
                if msg_type != Some("subscribe") {
                    continue;
                }

                let Some(channel) = payload.get("channel").and_then(Value::as_str) else {
                    continue;
                };

                let outbound = match channel {
                    "order_book/1" => Some(json!({
                        "type": "subscribed/order_book",
                        "channel": channel,
                        "offset": 1,
                        "timestamp": 1704067260000i64,
                        "order_book": {
                            "code": 200,
                            "asks": [{"price": "100010.00", "size": "0.5000"}],
                            "bids": [{"price": "100000.00", "size": "0.7000"}],
                            "offset": 1,
                            "nonce": 1,
                            "begin_nonce": 1
                        }
                    })),
                    "ticker/1" => Some(json!({
                        "type": "update/ticker",
                        "channel": channel,
                        "ticker": {
                            "s": "BTC-USDC",
                            "a": {"price": "100010.00", "size": "0.5000"},
                            "b": {"price": "100000.00", "size": "0.7000"}
                        }
                    })),
                    "trade/1" => Some(json!({
                        "type": "update/trade",
                        "channel": channel,
                        "trades": [{
                            "trade_id": 12346,
                            "market_id": TEST_MARKET_ID,
                            "size": "0.1000",
                            "price": "100006.00",
                            "is_maker_ask": false,
                            "timestamp": 1704067265000i64
                        }]
                    })),
                    "market_stats/all" => Some(json!({
                        "type": "update/market_stats",
                        "channel": channel,
                        "market_stats": {
                            "1": {
                                "market_id": TEST_MARKET_ID,
                                "symbol": "BTC-USDC",
                                "mark_price": "100005.00",
                                "index_price": "100000.00",
                                "current_funding_rate": "0.0001",
                                "funding_timestamp": 1704067800000i64
                            }
                        }
                    })),
                    _ => None,
                };

                if let Some(outbound) = outbound
                    && socket
                        .send(Message::Text(outbound.to_string().into()))
                        .await
                        .is_err()
                {
                    break;
                }
            }
            Message::Ping(data) if socket.send(Message::Pong(data.clone())).await.is_err() => break,
            Message::Close(_) => break,
            _ => {}
        }
    }
}
