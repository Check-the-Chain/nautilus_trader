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

use std::net::SocketAddr;

use axum::{Json, Router, response::IntoResponse, routing::get};
use nautilus_model::instruments::Instrument;
use nautilus_variational::{common::load_instrument_registry, http::client::VariationalHttpClient};
use serde_json::json;
use tokio::net::TcpListener;

async fn spawn_server(router: Router) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    addr
}

async fn handle_stats() -> impl IntoResponse {
    Json(json!({
        "total_volume_24h": "1000",
        "cumulative_volume": "2000",
        "tvl": "3000",
        "open_interest": "4000",
        "num_markets": 1,
        "loss_refund": {
            "pool_size": "0",
            "refunded_24h": "0"
        },
        "listings": [{
            "ticker": "BTC",
            "name": "Bitcoin",
            "mark_price": "93787.9606019699",
            "volume_24h": "100",
            "open_interest": {
                "long_open_interest": "10",
                "short_open_interest": "11"
            },
            "funding_rate": "0.037347",
            "funding_interval_s": 28800,
            "base_spread_bps": "0.4",
            "quotes": {
                "updated_at": "2026-01-06T06:38:52.476166127Z",
                "base": {"bid": "93750.97", "ask": "93755.01"},
                "size_1k": {"bid": "93750.97", "ask": "93755.01"}
            }
        }]
    }))
}

#[tokio::test]
async fn stats_client_parses_public_response_shape() {
    let addr = spawn_server(Router::new().route("/metadata/stats", get(handle_stats))).await;
    let client = VariationalHttpClient::new(Some(format!("http://{addr}")), None, 5).unwrap();

    let stats = client.stats().await.unwrap();

    assert_eq!(stats.num_markets, Some(1));
    assert_eq!(stats.listings[0].ticker, "BTC");
    assert_eq!(
        stats.listings[0]
            .quotes
            .as_ref()
            .unwrap()
            .base
            .as_ref()
            .unwrap()
            .bid
            .as_deref(),
        Some("93750.97"),
    );
}

#[tokio::test]
async fn registry_loads_instruments_from_stats() {
    let addr = spawn_server(Router::new().route("/metadata/stats", get(handle_stats))).await;
    let client = VariationalHttpClient::new(Some(format!("http://{addr}")), None, 5).unwrap();

    let registry = load_instrument_registry(&client, 8).await.unwrap();
    let instruments = registry.instruments();

    assert_eq!(instruments.len(), 1);
    assert_eq!(instruments[0].id().to_string(), "BTC-USDC-PERP.VARIATIONAL");
}

#[tokio::test]
#[ignore = "live Variational endpoint check"]
async fn live_stats_endpoint_shape() {
    let client = VariationalHttpClient::default();
    let stats = client.stats().await.unwrap();

    assert!(stats.num_markets.unwrap_or_default() > 0);
    assert!(!stats.listings.is_empty());
    assert!(stats.listings.iter().any(|listing| {
        listing.mark_price.is_some()
            && listing
                .quotes
                .as_ref()
                .and_then(|quotes| quotes.preferred_quote("base"))
                .is_some()
    }));
}
