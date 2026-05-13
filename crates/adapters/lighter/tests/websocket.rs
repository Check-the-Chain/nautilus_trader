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

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
    routing::get,
};
use futures_util::StreamExt;
use nautilus_lighter::websocket::client::LighterWebSocketClient;
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::Mutex, time::timeout};

#[derive(Clone, Default)]
struct TestWsState {
    connection_count: Arc<AtomicUsize>,
    received: Arc<Mutex<Vec<Value>>>,
    send_initial_ping: Arc<AtomicBool>,
    received_pong: Arc<AtomicBool>,
}

async fn spawn_server(router: Router) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    addr
}

async fn handle_ws_upgrade(ws: WebSocketUpgrade, State(state): State<TestWsState>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: TestWsState) {
    state.connection_count.fetch_add(1, Ordering::Relaxed);

    if state.send_initial_ping.load(Ordering::Relaxed) {
        let _ = socket
            .send(Message::Text(json!({"type": "ping"}).to_string().into()))
            .await;
    }

    while let Some(message) = socket.next().await {
        let Ok(message) = message else { break };
        let Message::Text(text) = message else {
            continue;
        };
        let payload: Value = serde_json::from_str(&text).unwrap();
        state.received.lock().await.push(payload.clone());

        if payload.get("type").and_then(Value::as_str) == Some("pong") {
            state.received_pong.store(true, Ordering::Relaxed);
            continue;
        }

        if payload.get("type").and_then(Value::as_str) == Some("subscribe") {
            let channel = payload.get("channel").and_then(Value::as_str).unwrap();
            let _ = socket
                .send(Message::Text(
                    json!({"type": "ack", "channel": channel})
                        .to_string()
                        .into(),
                ))
                .await;
        }
    }
}

fn build_router(state: TestWsState) -> Router {
    Router::new()
        .route("/stream", get(handle_ws_upgrade))
        .with_state(state)
}

async fn wait_for<F>(predicate: F)
where
    F: Fn() -> bool,
{
    timeout(Duration::from_secs(3), async {
        loop {
            if predicate() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

async fn wait_for_received_len(state: &TestWsState, expected: usize) {
    timeout(Duration::from_secs(3), async {
        loop {
            if state.received.lock().await.len() >= expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_connect_and_subscribe_receives_messages() {
    let state = TestWsState::default();
    let addr = spawn_server(build_router(state.clone())).await;
    let client = LighterWebSocketClient::new(format!("ws://{addr}/stream"), None);

    client.connect().await.unwrap();
    client
        .subscribe("order_book/1".to_string(), None)
        .await
        .unwrap();

    let message = timeout(Duration::from_secs(3), client.next_message())
        .await
        .unwrap()
        .unwrap();
    assert!(message.contains("\"channel\":\"order_book/1\""));

    wait_for(|| state.connection_count.load(Ordering::Relaxed) == 1).await;
    let received = state.received.lock().await.clone();
    assert!(
        received
            .iter()
            .any(|payload| payload["channel"] == "order_book/1")
    );

    client.close().await.unwrap();
}

#[tokio::test]
async fn test_stored_subscriptions_restore_on_connect_with_default_auth() {
    let state = TestWsState::default();
    let addr = spawn_server(build_router(state.clone())).await;
    let client = LighterWebSocketClient::new(
        format!("ws://{addr}/stream"),
        Some("default-auth".to_string()),
    );

    client
        .subscribe("account_all/7".to_string(), None)
        .await
        .unwrap();
    client.connect().await.unwrap();

    let _ = timeout(Duration::from_secs(3), client.next_message())
        .await
        .unwrap()
        .unwrap();

    wait_for_received_len(&state, 1).await;
    let received = state.received.lock().await.clone();
    assert!(received.iter().any(|payload| {
        payload["type"] == "subscribe"
            && payload["channel"] == "account_all/7"
            && payload["auth"] == "default-auth"
    }));

    client.close().await.unwrap();
}

#[tokio::test]
async fn test_client_replies_to_ping_messages() {
    let state = TestWsState::default();
    state.send_initial_ping.store(true, Ordering::Relaxed);

    let addr = spawn_server(build_router(state.clone())).await;
    let client = LighterWebSocketClient::new(format!("ws://{addr}/stream"), None);

    client.connect().await.unwrap();
    wait_for(|| state.received_pong.load(Ordering::Relaxed)).await;
    assert!(state.received_pong.load(Ordering::Relaxed));

    client.close().await.unwrap();
}

#[tokio::test]
async fn test_unsubscribe_sends_command() {
    let state = TestWsState::default();
    let addr = spawn_server(build_router(state.clone())).await;
    let client = LighterWebSocketClient::new(format!("ws://{addr}/stream"), None);

    client.connect().await.unwrap();
    client.subscribe("trade/1".to_string(), None).await.unwrap();
    let _ = timeout(Duration::from_secs(3), client.next_message())
        .await
        .unwrap()
        .unwrap();
    client.unsubscribe("trade/1".to_string()).await.unwrap();

    wait_for_received_len(&state, 2).await;
    let received = state.received.lock().await.clone();
    assert!(
        received
            .iter()
            .any(|payload| { payload["type"] == "unsubscribe" && payload["channel"] == "trade/1" })
    );

    client.close().await.unwrap();
}
