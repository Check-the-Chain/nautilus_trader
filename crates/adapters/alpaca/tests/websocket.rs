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

//! Integration tests for the Alpaca WebSocket clients using a mock Axum server.
//!
//! The harness mirrors the Lighter / OKX shape: a `TestServerState` records
//! every inbound message from the client, `handle_socket` replies with venue
//! acks and pre-arranged data frames, and each test drives the public client
//! surface and asserts on the resulting [`NautilusWsMessage`] stream and the
//! recorded server-side state.

use std::{
    net::SocketAddr,
    path::PathBuf,
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
use futures_util::{SinkExt, StreamExt};
use nautilus_alpaca::{
    common::{
        consts::ALPACA_VENUE,
        credential::Credential,
        enums::{AlpacaDataFeed, AlpacaEnvironment, AlpacaTradeUpdateEvent},
    },
    websocket::{
        AlpacaInstrumentInfo, AlpacaTradeUpdatesWebSocketClient, AlpacaWebSocketClient,
        NautilusWsMessage,
    },
};
use nautilus_common::testing::wait_until_async;
use nautilus_model::identifiers::{InstrumentId, Symbol};
use nautilus_network::websocket::TransportBackend;
use serde_json::{Value, json};

const RECV_TIMEOUT: Duration = Duration::from_secs(5);

fn data_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data")
}

fn load_json_text(filename: &str) -> String {
    std::fs::read_to_string(data_path().join(filename))
        .unwrap_or_else(|_| panic!("failed to read {filename}"))
}

fn test_credential() -> Credential {
    Credential::new("test-key".to_string(), "test-secret".to_string())
}

fn instrument_info(symbol: &str) -> AlpacaInstrumentInfo {
    AlpacaInstrumentInfo {
        instrument_id: InstrumentId::new(Symbol::new(symbol), *ALPACA_VENUE),
        price_precision: 2,
        size_precision: 0,
    }
}

#[derive(Clone, Default)]
struct TestServerState {
    connection_count: Arc<AtomicUsize>,
    auths: Arc<tokio::sync::Mutex<Vec<Value>>>,
    subscribes: Arc<tokio::sync::Mutex<Vec<Value>>>,
    unsubscribes: Arc<tokio::sync::Mutex<Vec<Value>>>,
    listens: Arc<tokio::sync::Mutex<Vec<Value>>>,
    /// Raw text frames pushed to the client after each handled subscribe
    /// (market data) or listen (trade updates), drained in order.
    push_after_ack: Arc<tokio::sync::Mutex<Vec<String>>>,
    /// When set, the server closes the socket after the next subscribe ack.
    drop_after_next_subscribe: Arc<AtomicBool>,
}

impl TestServerState {
    async fn enqueue_push(&self, frame: String) {
        self.push_after_ack.lock().await.push(frame);
    }

    async fn pop_push(&self) -> Option<String> {
        let mut queue = self.push_after_ack.lock().await;
        if queue.is_empty() {
            None
        } else {
            Some(queue.remove(0))
        }
    }
}

async fn handle_market_data_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<Arc<TestServerState>>,
) -> Response {
    ws.on_upgrade(move |socket| handle_market_data_socket(socket, state))
}

/// Market data protocol: connected greeting, auth ack, subscription acks
/// carrying the full server-side set.
async fn handle_market_data_socket(socket: WebSocket, state: Arc<TestServerState>) {
    state.connection_count.fetch_add(1, Ordering::SeqCst);
    let (mut sink, mut stream) = socket.split();

    let _ = sink
        .send(Message::Text(
            json!([{"T":"success","msg":"connected"}])
                .to_string()
                .into(),
        ))
        .await;

    // Server-side subscription sets, echoed in full on every ack.
    let mut trades: Vec<String> = Vec::new();
    let mut quotes: Vec<String> = Vec::new();
    let mut bars: Vec<String> = Vec::new();

    while let Some(Ok(message)) = stream.next().await {
        match message {
            Message::Text(text) => {
                let Ok(value) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let action = value.get("action").and_then(Value::as_str).unwrap_or("");
                match action {
                    "auth" => {
                        state.auths.lock().await.push(value);
                        let ack = json!([{"T":"success","msg":"authenticated"}]);
                        if sink
                            .send(Message::Text(ack.to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    "subscribe" | "unsubscribe" => {
                        let is_subscribe = action == "subscribe";
                        if is_subscribe {
                            state.subscribes.lock().await.push(value.clone());
                        } else {
                            state.unsubscribes.lock().await.push(value.clone());
                        }

                        for (field, set) in [
                            ("trades", &mut trades),
                            ("quotes", &mut quotes),
                            ("bars", &mut bars),
                        ] {
                            if let Some(symbols) = value.get(field).and_then(Value::as_array) {
                                for symbol in symbols {
                                    let symbol = symbol.as_str().unwrap_or_default().to_string();
                                    if is_subscribe {
                                        if !set.contains(&symbol) {
                                            set.push(symbol);
                                        }
                                    } else {
                                        set.retain(|s| s != &symbol);
                                    }
                                }
                            }
                        }

                        let ack = json!([{
                            "T": "subscription",
                            "trades": trades,
                            "quotes": quotes,
                            "bars": bars,
                            "dailyBars": [],
                            "updatedBars": [],
                        }]);
                        if sink
                            .send(Message::Text(ack.to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }

                        while let Some(frame) = state.pop_push().await {
                            if sink.send(Message::Text(frame.into())).await.is_err() {
                                break;
                            }
                        }

                        if is_subscribe
                            && state
                                .drop_after_next_subscribe
                                .swap(false, Ordering::Relaxed)
                        {
                            let _ = sink.send(Message::Close(None)).await;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            Message::Ping(payload) => {
                if sink.send(Message::Pong(payload)).await.is_err() {
                    break;
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    state.connection_count.fetch_sub(1, Ordering::SeqCst);
}

async fn handle_trade_updates_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<Arc<TestServerState>>,
) -> Response {
    ws.on_upgrade(move |socket| handle_trade_updates_socket(socket, state))
}

/// Trade-updates protocol: authorization envelope, listening ack, then any
/// queued event frames. Events are sent as BINARY frames to mirror the paper
/// endpoint's framing.
async fn handle_trade_updates_socket(socket: WebSocket, state: Arc<TestServerState>) {
    state.connection_count.fetch_add(1, Ordering::SeqCst);
    let (mut sink, mut stream) = socket.split();

    while let Some(Ok(message)) = stream.next().await {
        match message {
            Message::Text(text) => {
                let Ok(value) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let action = value.get("action").and_then(Value::as_str).unwrap_or("");
                match action {
                    "auth" => {
                        state.auths.lock().await.push(value);
                        let ack = json!({
                            "stream": "authorization",
                            "data": {"status": "authorized", "action": "authenticate"},
                        });
                        if sink
                            .send(Message::Text(ack.to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    "listen" => {
                        state.listens.lock().await.push(value.clone());
                        let streams = value
                            .get("data")
                            .and_then(|data| data.get("streams"))
                            .cloned()
                            .unwrap_or_else(|| json!([]));
                        let ack = json!({"stream": "listening", "data": {"streams": streams}});
                        if sink
                            .send(Message::Text(ack.to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }

                        while let Some(frame) = state.pop_push().await {
                            if sink
                                .send(Message::Binary(frame.into_bytes().into()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                    _ => {}
                }
            }
            Message::Ping(payload) => {
                if sink.send(Message::Pong(payload)).await.is_err() {
                    break;
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    state.connection_count.fetch_sub(1, Ordering::SeqCst);
}

type WsHandlerFn = fn(
    WebSocketUpgrade,
    State<Arc<TestServerState>>,
) -> std::pin::Pin<Box<dyn Future<Output = Response> + Send>>;

async fn start_server(state: Arc<TestServerState>, handler: WsHandlerFn) -> SocketAddr {
    let router = Router::new()
        .route("/stream", get(handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ws listener");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("ws server");
    });
    wait_until_async(
        || async move { tokio::net::TcpStream::connect(addr).await.is_ok() },
        Duration::from_secs(2),
    )
    .await;
    addr
}

fn market_data_handler(
    ws: WebSocketUpgrade,
    state: State<Arc<TestServerState>>,
) -> std::pin::Pin<Box<dyn Future<Output = Response> + Send>> {
    Box::pin(handle_market_data_upgrade(ws, state))
}

fn trade_updates_handler(
    ws: WebSocketUpgrade,
    state: State<Arc<TestServerState>>,
) -> std::pin::Pin<Box<dyn Future<Output = Response> + Send>> {
    Box::pin(handle_trade_updates_upgrade(ws, state))
}

async fn connect_market_data_client(addr: SocketAddr) -> AlpacaWebSocketClient {
    let mut client = AlpacaWebSocketClient::new(
        Some(format!("ws://{addr}/stream")),
        AlpacaDataFeed::Iex,
        test_credential(),
        TransportBackend::default(),
        None,
    );
    client.initialize_instruments(vec![instrument_info("AAPL"), instrument_info("MSFT")]);
    client.connect().await.expect("connect");
    client
}

/// Receives events until `predicate` matches, or panics after `RECV_TIMEOUT`.
async fn wait_for_message<F>(
    client: &mut AlpacaWebSocketClient,
    mut predicate: F,
) -> NautilusWsMessage
where
    F: FnMut(&NautilusWsMessage) -> bool,
{
    tokio::time::timeout(RECV_TIMEOUT, async {
        loop {
            let Some(msg) = client.next_event().await else {
                panic!("stream ended while waiting for message");
            };
            if predicate(&msg) {
                return msg;
            }
        }
    })
    .await
    .expect("timed out waiting for message")
}

#[tokio::test]
async fn test_connect_authenticates() {
    let state = Arc::new(TestServerState::default());
    let addr = start_server(Arc::clone(&state), market_data_handler).await;

    let mut client = connect_market_data_client(addr).await;

    assert!(client.is_active());
    assert_eq!(state.auths.lock().await.len(), 1);
    let auth = &state.auths.lock().await[0];
    assert_eq!(auth.get("key").and_then(Value::as_str), Some("test-key"));

    client.disconnect().await.expect("disconnect");
}

#[tokio::test]
async fn test_subscribe_trades_and_receive_ticks() {
    let state = Arc::new(TestServerState::default());
    let addr = start_server(Arc::clone(&state), market_data_handler).await;

    let mut client = connect_market_data_client(addr).await;
    state
        .enqueue_push(load_json_text("ws_market_data_batch.json"))
        .await;

    client
        .subscribe_trades(instrument_info("AAPL").instrument_id)
        .await
        .expect("subscribe");

    let msg = wait_for_message(&mut client, |msg| {
        matches!(msg, NautilusWsMessage::Trades(_))
    })
    .await;
    let NautilusWsMessage::Trades(trades) = msg else {
        unreachable!()
    };
    assert_eq!(trades.len(), 2);
    assert_eq!(trades[0].price.to_string(), "189.05");
    assert_eq!(trades[0].trade_id.to_string(), "96921");

    // The same batch carries quotes and a bar.
    let quote = wait_for_message(&mut client, |msg| {
        matches!(msg, NautilusWsMessage::Quote(_))
    })
    .await;
    let NautilusWsMessage::Quote(quote) = quote else {
        unreachable!()
    };
    assert_eq!(quote.bid_price.to_string(), "473.11");

    let bar = wait_for_message(&mut client, |msg| matches!(msg, NautilusWsMessage::Bar(_))).await;
    let NautilusWsMessage::Bar(bar) = bar else {
        unreachable!()
    };
    assert_eq!(bar.close.to_string(), "189.05");

    let subscribes = state.subscribes.lock().await;
    assert_eq!(subscribes.len(), 1);
    assert_eq!(
        subscribes[0]
            .get("trades")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(1)
    );

    drop(subscribes);
    client.disconnect().await.expect("disconnect");
}

#[tokio::test]
async fn test_subscription_ack_reconciles_tracked_state() {
    let state = Arc::new(TestServerState::default());
    let addr = start_server(Arc::clone(&state), market_data_handler).await;

    let mut client = connect_market_data_client(addr).await;

    client
        .subscribe_quotes(instrument_info("MSFT").instrument_id)
        .await
        .expect("subscribe");

    let ack = wait_for_message(&mut client, |msg| {
        matches!(msg, NautilusWsMessage::SubscriptionAck(_))
    })
    .await;
    let NautilusWsMessage::SubscriptionAck(ack) = ack else {
        unreachable!()
    };
    assert_eq!(ack.quotes.len(), 1);
    assert_eq!(client.subscription_count(), 1);

    client
        .unsubscribe_quotes(instrument_info("MSFT").instrument_id)
        .await
        .expect("unsubscribe");

    wait_for_message(
        &mut client,
        |msg| matches!(msg, NautilusWsMessage::SubscriptionAck(ack) if ack.quotes.is_empty()),
    )
    .await;
    assert_eq!(client.subscription_count(), 0);

    client.disconnect().await.expect("disconnect");
}

#[tokio::test]
async fn test_reconnect_reauthenticates_and_resubscribes() {
    let state = Arc::new(TestServerState::default());
    let addr = start_server(Arc::clone(&state), market_data_handler).await;

    let mut client = connect_market_data_client(addr).await;

    state
        .drop_after_next_subscribe
        .store(true, Ordering::Relaxed);
    client
        .subscribe_trades(instrument_info("AAPL").instrument_id)
        .await
        .expect("subscribe");

    // The server drops the socket after acking; the transport reconnects and
    // the client must re-auth and replay the tracked subscription.
    wait_until_async(
        || {
            let state = Arc::clone(&state);
            async move {
                state.auths.lock().await.len() >= 2 && state.subscribes.lock().await.len() >= 2
            }
        },
        Duration::from_secs(15),
    )
    .await;

    let subscribes = state.subscribes.lock().await;
    let last = subscribes.last().expect("resubscribe recorded");
    assert_eq!(
        last.get("trades").and_then(Value::as_array).map(Vec::len),
        Some(1)
    );

    drop(subscribes);
    client.disconnect().await.expect("disconnect");
}

#[tokio::test]
async fn test_trade_updates_connect_listen_and_fill() {
    let state = Arc::new(TestServerState::default());
    let addr = start_server(Arc::clone(&state), trade_updates_handler).await;

    let mut client = AlpacaTradeUpdatesWebSocketClient::new(
        Some(format!("ws://{addr}/stream")),
        AlpacaEnvironment::Paper,
        test_credential(),
        TransportBackend::default(),
        None,
    );

    state
        .enqueue_push(load_json_text("ws_trade_updates_fill.json"))
        .await;

    client.connect().await.expect("connect");
    assert!(client.is_active());

    let msg = tokio::time::timeout(RECV_TIMEOUT, async {
        loop {
            let Some(msg) = client.next_event().await else {
                panic!("stream ended");
            };
            if let NautilusWsMessage::TradeUpdate(update) = msg {
                return update;
            }
        }
    })
    .await
    .expect("timed out waiting for trade update");

    assert_eq!(msg.event, AlpacaTradeUpdateEvent::Fill);
    assert_eq!(msg.qty.as_deref(), Some("100"));
    assert_eq!(msg.order.client_order_id, "O-20260105-001");

    assert_eq!(state.listens.lock().await.len(), 1);

    client.disconnect().await.expect("disconnect");
}
