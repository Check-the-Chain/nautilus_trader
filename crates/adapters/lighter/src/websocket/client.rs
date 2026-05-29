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

//! Lighter WebSocket session backed by the Nautilus network WebSocket client.

use std::{
    collections::HashMap,
    sync::{
        Arc, Once,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use nautilus_network::{
    mode::ConnectionMode,
    websocket::{TransportBackend, WebSocketClient, WebSocketConfig, channel_message_handler},
};
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::{
    error::{Result, SdkError},
    websocket::handler::{HandlerCommand, LighterWsFeedHandler, WsEvent, WsEventHandler},
};

static INSTALL_CRYPTO_PROVIDER: Once = Once::new();

const WS_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const WS_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const WS_RECONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const WS_RECONNECT_INITIAL_DELAY: Duration = Duration::from_millis(250);
const WS_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(5);
const WS_RECONNECT_JITTER: Duration = Duration::from_millis(200);

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.lighter", from_py_object)
)]
pub struct LighterWebSocketClient {
    url: String,
    proxy_url: Option<String>,
    transport_backend: TransportBackend,
    default_auth_token: Arc<Mutex<Option<String>>>,
    subscriptions: Arc<Mutex<HashMap<String, Option<String>>>>,
    command_tx: Arc<Mutex<Option<mpsc::UnboundedSender<HandlerCommand>>>>,
    event_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<WsEvent>>>>,
    connection_mode: Arc<ArcSwap<AtomicU8>>,
    task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    keepalive_interval: Duration,
}

impl Clone for LighterWebSocketClient {
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            proxy_url: self.proxy_url.clone(),
            transport_backend: self.transport_backend,
            default_auth_token: Arc::clone(&self.default_auth_token),
            subscriptions: Arc::clone(&self.subscriptions),
            command_tx: Arc::clone(&self.command_tx),
            event_rx: Arc::clone(&self.event_rx),
            connection_mode: Arc::clone(&self.connection_mode),
            task_handle: Arc::clone(&self.task_handle),
            keepalive_interval: self.keepalive_interval,
        }
    }
}

impl std::fmt::Debug for LighterWebSocketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LighterWebSocketClient")
            .field("url", &self.url)
            .field("proxy_url", &self.proxy_url)
            .field("is_active", &self.is_active())
            .finish()
    }
}

impl LighterWebSocketClient {
    #[must_use]
    pub fn new(url: String, auth_token: Option<String>) -> Self {
        Self {
            url,
            proxy_url: None,
            transport_backend: TransportBackend::default(),
            default_auth_token: Arc::new(Mutex::new(auth_token)),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            command_tx: Arc::new(Mutex::new(None)),
            event_rx: Arc::new(Mutex::new(None)),
            connection_mode: Arc::new(ArcSwap::new(Arc::new(AtomicU8::new(
                ConnectionMode::Closed.as_u8(),
            )))),
            task_handle: Arc::new(Mutex::new(None)),
            keepalive_interval: WS_KEEPALIVE_INTERVAL,
        }
    }

    #[must_use]
    pub fn with_proxy_url(mut self, proxy_url: Option<String>) -> Self {
        self.proxy_url = proxy_url;
        self
    }

    #[must_use]
    pub fn with_transport_backend(mut self, transport_backend: TransportBackend) -> Self {
        self.transport_backend = transport_backend;
        self
    }

    #[must_use]
    pub fn with_keepalive_interval(mut self, keepalive_interval: Duration) -> Self {
        self.keepalive_interval = keepalive_interval.max(Duration::from_millis(1));
        self
    }

    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        ConnectionMode::from_u8(self.connection_mode.load().load(Ordering::Relaxed)).is_active()
    }

    pub async fn set_auth_token(&self, token: Option<String>) {
        let mut guard = self.default_auth_token.lock().await;
        *guard = token;
    }

    pub async fn connect(&self) -> Result<()> {
        self.connect_inner(None).await
    }

    pub(crate) async fn connect_with_event_handler<F>(&self, handler: F) -> Result<()>
    where
        F: FnMut(WsEvent) + Send + 'static,
    {
        self.connect_inner(Some(Box::new(handler))).await
    }

    async fn connect_inner(&self, event_handler: Option<WsEventHandler>) -> Result<()> {
        if self.is_active() {
            return Ok(());
        }

        install_crypto_provider();

        let (command_tx, command_rx) = mpsc::unbounded_channel::<HandlerCommand>();
        let (event_tx, event_rx) = if event_handler.is_none() {
            let (tx, rx) = mpsc::unbounded_channel::<WsEvent>();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        {
            let mut tx_guard = self.command_tx.lock().await;
            *tx_guard = Some(command_tx.clone());
        }
        {
            let mut rx_guard = self.event_rx.lock().await;
            *rx_guard = event_rx;
        }

        let (message_handler, raw_rx) = channel_message_handler();
        let ping_tx = command_tx.clone();
        let ping_handler = Arc::new(move |payload: Vec<u8>| {
            let _ = ping_tx.send(HandlerCommand::SendPong(payload));
        });

        let cfg = WebSocketConfig {
            url: self.url.clone(),
            headers: websocket_headers(&self.url),
            heartbeat: Some(self.keepalive_interval.as_secs().max(1)),
            heartbeat_msg: None,
            reconnect_timeout_ms: Some(WS_RECONNECT_TIMEOUT.as_millis() as u64),
            reconnect_delay_initial_ms: Some(WS_RECONNECT_INITIAL_DELAY.as_millis() as u64),
            reconnect_delay_max_ms: Some(WS_RECONNECT_MAX_DELAY.as_millis() as u64),
            reconnect_backoff_factor: Some(2.0),
            reconnect_jitter_ms: Some(WS_RECONNECT_JITTER.as_millis() as u64),
            reconnect_max_attempts: None,
            idle_timeout_ms: Some(
                self.keepalive_interval
                    .saturating_mul(3)
                    .max(WS_IDLE_TIMEOUT)
                    .as_millis() as u64,
            ),
            backend: self.transport_backend,
            proxy_url: self.proxy_url.clone(),
        };
        let client = WebSocketClient::connect(
            cfg,
            Some(message_handler),
            Some(ping_handler),
            None,
            vec![],
            None,
        )
        .await
        .map_err(|error| SdkError::Other(format!("WebSocket connect failed: {error}")))?;
        self.connection_mode.store(client.connection_mode_atomic());

        let client = Arc::new(client);
        command_tx
            .send(HandlerCommand::SetClient(client))
            .map_err(|e| SdkError::Other(format!("Failed to initialize WebSocket handler: {e}")))?;

        let connection_mode = Arc::clone(&self.connection_mode);
        let mut handler = LighterWsFeedHandler::new(
            command_rx,
            raw_rx,
            Arc::clone(&self.default_auth_token),
            Arc::clone(&self.subscriptions),
            event_handler,
            event_tx,
        );
        let handle = tokio::spawn(async move {
            handler.run().await;
            connection_mode.store(Arc::new(AtomicU8::new(ConnectionMode::Closed.as_u8())));
        });
        *self.task_handle.lock().await = Some(handle);

        let _ = command_tx.send(HandlerCommand::ReplaySubscriptions);
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        if let Some(tx) = self.command_tx.lock().await.as_ref().cloned() {
            let _ = tx.send(HandlerCommand::Disconnect);
        }

        if let Some(handle) = self.task_handle.lock().await.take() {
            let abort_handle = handle.abort_handle();
            tokio::select! {
                result = handle => {
                    if let Err(error) = result
                        && !error.is_cancelled()
                    {
                        log::warn!("Lighter websocket handler task failed: {error}");
                    }
                }
                () = tokio::time::sleep(Duration::from_secs(2)) => {
                    abort_handle.abort();
                }
            }
        }

        *self.command_tx.lock().await = None;
        self.connection_mode
            .store(Arc::new(AtomicU8::new(ConnectionMode::Closed.as_u8())));
        Ok(())
    }

    pub async fn subscribe(&self, channel: String, auth_token: Option<String>) -> Result<()> {
        self.subscriptions
            .lock()
            .await
            .insert(channel.clone(), auth_token.clone());

        if let Some(tx) = self.command_tx.lock().await.as_ref() {
            tx.send(HandlerCommand::Subscribe {
                channel,
                auth_token,
            })
            .map_err(|e| SdkError::Other(format!("Failed to send subscribe command: {e}")))?;
        }

        Ok(())
    }

    pub async fn unsubscribe(&self, channel: String) -> Result<()> {
        self.subscriptions.lock().await.remove(&channel);

        if let Some(tx) = self.command_tx.lock().await.as_ref() {
            tx.send(HandlerCommand::Unsubscribe { channel })
                .map_err(|e| SdkError::Other(format!("Failed to send unsubscribe command: {e}")))?;
        }

        Ok(())
    }

    pub async fn send_json(&self, value: serde_json::Value) -> Result<()> {
        let Some(tx) = self.command_tx.lock().await.as_ref().cloned() else {
            return Err(SdkError::Other("WebSocket is not connected".to_string()));
        };

        let (completion_tx, completion_rx) = oneshot::channel();
        tx.send(HandlerCommand::SendText {
            text: value.to_string(),
            completion: Some(completion_tx),
        })
        .map_err(|e| SdkError::Other(format!("Failed to send WebSocket command: {e}")))?;

        completion_rx
            .await
            .map_err(|_| {
                SdkError::Other("WebSocket writer closed before send completed".to_string())
            })?
            .map_err(|e| SdkError::Other(format!("Failed to write WebSocket command: {e}")))
    }

    pub async fn next_message(&self) -> Option<String> {
        self.next_event().await.map(|event| event.text)
    }

    pub(crate) async fn next_event(&self) -> Option<WsEvent> {
        let mut guard = self.event_rx.lock().await;
        let receiver = guard.as_mut()?;
        receiver.recv().await
    }
}

fn install_crypto_provider() {
    INSTALL_CRYPTO_PROVIDER.call_once(|| {
        if CryptoProvider::get_default().is_none() {
            let _ = aws_lc_rs::default_provider().install_default();
        }
    });
}

fn websocket_headers(url: &str) -> Vec<(String, String)> {
    origin_for_ws_url(url)
        .map(|origin| vec![("Origin".to_string(), origin)])
        .unwrap_or_default()
}

fn origin_for_ws_url(url: &str) -> Option<String> {
    let (origin_scheme, rest) = if let Some(rest) = url.strip_prefix("wss://") {
        ("https", rest)
    } else if let Some(rest) = url.strip_prefix("ws://") {
        ("http", rest)
    } else {
        return None;
    };

    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .filter(|authority| !authority.is_empty())?;
    Some(format!("{origin_scheme}://{authority}"))
}
