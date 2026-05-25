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

use std::time::Duration;

use nautilus_network::websocket::{
    TransportBackend, WebSocketClient, WebSocketConfig, channel_message_handler,
};
use nautilus_variational::{
    config::{VARIATIONAL_WS_PRICE_FUNDING_INTERVAL_SECS, VariationalDataClientConfig},
    websocket::messages::{
        VariationalWsMessage, VariationalWsSubscriptionRequest, default_price_ws_instrument,
        ticker_from_price_channel,
    },
};
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;

#[test]
fn subscription_request_matches_live_prices_shape() {
    let request = VariationalWsSubscriptionRequest::subscribe_tickers(
        ["BTC".to_string()],
        VARIATIONAL_WS_PRICE_FUNDING_INTERVAL_SECS,
    );

    let value = serde_json::to_value(request).unwrap();

    assert_eq!(
        value,
        json!({
            "action": "subscribe",
            "instruments": [{
                "underlying": "BTC",
                "instrument_type": "perpetual_future",
                "settlement_asset": "USDC",
                "funding_interval_s": 3600
            }]
        })
    );
}

#[test]
fn parses_price_channel_ticker() {
    assert_eq!(
        ticker_from_price_channel("instrument_price:P-BTC-USDC-3600"),
        Some("BTC"),
    );
    assert_eq!(
        ticker_from_price_channel("instrument_price:P-solana_abc123-FOO-USDC-3600"),
        Some("FOO"),
    );
}

#[test]
fn parses_live_price_message_shape() {
    let text = r#"{"channel":"instrument_price:P-BTC-USDC-3600","pricing":{"price":"77125.4","native_price":"0.9995","delta":"1","gamma":"0","theta":"0","vega":"0","rho":"0","iv":"0","underlying_price":"77161.99","interest_rate":"0.0000626800000000000029657873","timestamp":"2026-05-20T16:38:34.753596Z"}}"#;

    let msg = serde_json::from_str::<VariationalWsMessage>(text).unwrap();

    let VariationalWsMessage::Price(price) = msg else {
        panic!("expected price message");
    };
    assert_eq!(price.ticker(), Some("BTC"));
    assert_eq!(price.pricing.price.as_deref(), Some("77125.4"));
    assert_eq!(price.pricing.underlying_price.as_deref(), Some("77161.99"));
}

#[test]
fn parses_heartbeat_message_shape() {
    let text = r#"{"timestamp":"2026-05-20T16:37:42.935540965Z","type":"heartbeat"}"#;

    let msg = serde_json::from_str::<VariationalWsMessage>(text).unwrap();

    let VariationalWsMessage::Heartbeat(heartbeat) = msg else {
        panic!("expected heartbeat message");
    };
    assert!(heartbeat.is_heartbeat());
}

#[tokio::test]
#[ignore = "live Variational websocket endpoint check"]
async fn live_prices_ws_endpoint_shape() {
    let (message_handler, mut raw_rx) = channel_message_handler();
    let config = VariationalDataClientConfig::default();
    let ws_config = WebSocketConfig {
        url: config.ws_prices_url(),
        headers: vec![],
        heartbeat: None,
        heartbeat_msg: None,
        reconnect_timeout_ms: Some(15_000),
        reconnect_delay_initial_ms: Some(250),
        reconnect_delay_max_ms: Some(5_000),
        reconnect_backoff_factor: Some(2.0),
        reconnect_jitter_ms: Some(200),
        reconnect_max_attempts: Some(0),
        idle_timeout_ms: Some(15_000),
        backend: TransportBackend::Tungstenite,
        proxy_url: None,
    };
    let ws_client =
        WebSocketClient::connect(ws_config, Some(message_handler), None, None, vec![], None)
            .await
            .unwrap();
    let request = VariationalWsSubscriptionRequest::subscribe_tickers(
        [default_price_ws_instrument("BTC").underlying],
        VARIATIONAL_WS_PRICE_FUNDING_INTERVAL_SECS,
    );
    ws_client
        .send_text(serde_json::to_string(&request).unwrap(), None)
        .await
        .unwrap();

    let mut saw_price = false;
    let deadline = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            () = &mut deadline => break,
            msg = raw_rx.recv() => {
                let Some(Message::Text(text)) = msg else {
                    continue;
                };
                if let Ok(VariationalWsMessage::Price(price)) =
                    serde_json::from_str::<VariationalWsMessage>(&text)
                {
                    assert_eq!(price.ticker(), Some("BTC"));
                    assert!(price.pricing.price.is_some());
                    assert!(price.pricing.underlying_price.is_some());
                    saw_price = true;
                    break;
                }
            }
        }
    }

    ws_client.disconnect().await;
    assert!(saw_price, "expected a live BTC price message");
}
