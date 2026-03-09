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

//! Raw WebSocket transport for the Lighter adapter.

use std::{
    collections::HashMap,
    sync::{
        Arc, Once,
        atomic::{AtomicBool, Ordering},
    },
};

use futures_util::{SinkExt, StreamExt};
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use tokio::sync::{Mutex, mpsc};
use tokio_tungstenite::tungstenite::Message;

use crate::error::{Result, SdkError};

static INSTALL_CRYPTO_PROVIDER: Once = Once::new();

#[derive(Debug)]
enum WsCommand {
    Json(serde_json::Value),
    Close,
}

/// Raw Lighter WebSocket client that forwards venue messages to Python without
/// imposing Nautilus-specific parsing in Rust.
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.lighter", from_py_object)
)]
pub struct LighterWebSocketClient {
    url: String,
    default_auth_token: Arc<Mutex<Option<String>>>,
    subscriptions: Arc<Mutex<HashMap<String, Option<String>>>>,
    command_tx: Arc<Mutex<Option<mpsc::UnboundedSender<WsCommand>>>>,
    event_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<String>>>>,
    is_active: Arc<AtomicBool>,
}

impl Clone for LighterWebSocketClient {
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            default_auth_token: Arc::clone(&self.default_auth_token),
            subscriptions: Arc::clone(&self.subscriptions),
            command_tx: Arc::clone(&self.command_tx),
            event_rx: Arc::clone(&self.event_rx),
            is_active: Arc::clone(&self.is_active),
        }
    }
}

impl std::fmt::Debug for LighterWebSocketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LighterWebSocketClient")
            .field("url", &self.url)
            .field("is_active", &self.is_active())
            .finish()
    }
}

impl LighterWebSocketClient {
    #[must_use]
    pub fn new(url: String, auth_token: Option<String>) -> Self {
        Self {
            url,
            default_auth_token: Arc::new(Mutex::new(auth_token)),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            command_tx: Arc::new(Mutex::new(None)),
            event_rx: Arc::new(Mutex::new(None)),
            is_active: Arc::new(AtomicBool::new(false)),
        }
    }

    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        self.is_active.load(Ordering::SeqCst)
    }

    pub async fn set_auth_token(&self, token: Option<String>) {
        let mut guard = self.default_auth_token.lock().await;
        *guard = token;
    }

    pub async fn connect(&self) -> Result<()> {
        if self.is_active() {
            return Ok(());
        }

        install_crypto_provider();
        let (stream, _) = tokio_tungstenite::connect_async(&self.url).await?;
        let (mut write, mut read) = stream.split();

        let (command_tx, mut command_rx) = mpsc::unbounded_channel::<WsCommand>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<String>();

        {
            let mut tx_guard = self.command_tx.lock().await;
            *tx_guard = Some(command_tx.clone());
        }
        {
            let mut rx_guard = self.event_rx.lock().await;
            *rx_guard = Some(event_rx);
        }

        self.is_active.store(true, Ordering::SeqCst);

        let is_active_writer = Arc::clone(&self.is_active);
        tokio::spawn(async move {
            while let Some(command) = command_rx.recv().await {
                let result = match command {
                    WsCommand::Json(value) => {
                        write.send(Message::Text(value.to_string().into())).await
                    }
                    WsCommand::Close => {
                        let _ = write.close().await;
                        break;
                    }
                };

                if result.is_err() {
                    break;
                }
            }

            is_active_writer.store(false, Ordering::SeqCst);
        });

        let is_active_reader = Arc::clone(&self.is_active);
        let command_tx_reader = command_tx.clone();
        tokio::spawn(async move {
            while let Some(message) = read.next().await {
                let Ok(message) = message else {
                    break;
                };

                match message {
                    Message::Text(text) => {
                        let text_string = text.to_string();
                        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text_string)
                        {
                            if parsed.get("type").and_then(|value| value.as_str()) == Some("ping") {
                                let _ = command_tx_reader
                                    .send(WsCommand::Json(serde_json::json!({"type": "pong"})));
                            }
                        }
                        let _ = event_tx.send(text_string);
                    }
                    Message::Binary(bytes) => {
                        if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                            let _ = event_tx.send(text);
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }

            is_active_reader.store(false, Ordering::SeqCst);
        });

        let initial_subscriptions = self.subscriptions.lock().await.clone();
        for (channel, auth) in initial_subscriptions {
            let _ = command_tx.send(WsCommand::Json(subscription_message(&channel, auth)));
        }

        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        if let Some(tx) = self.command_tx.lock().await.as_ref() {
            let _ = tx.send(WsCommand::Close);
        }
        self.is_active.store(false, Ordering::SeqCst);
        Ok(())
    }

    pub async fn subscribe(&self, channel: String, auth_token: Option<String>) -> Result<()> {
        let resolved_auth = match auth_token {
            Some(token) => Some(token),
            None => self.default_auth_token.lock().await.clone(),
        };

        self.subscriptions
            .lock()
            .await
            .insert(channel.clone(), resolved_auth.clone());

        if let Some(tx) = self.command_tx.lock().await.as_ref() {
            tx.send(WsCommand::Json(subscription_message(
                &channel,
                resolved_auth,
            )))
            .map_err(|e| SdkError::Other(format!("Failed to send subscribe command: {e}")))?;
        }

        Ok(())
    }

    pub async fn unsubscribe(&self, channel: String) -> Result<()> {
        self.subscriptions.lock().await.remove(&channel);

        if let Some(tx) = self.command_tx.lock().await.as_ref() {
            tx.send(WsCommand::Json(serde_json::json!({
                "type": "unsubscribe",
                "channel": channel,
            })))
            .map_err(|e| SdkError::Other(format!("Failed to send unsubscribe command: {e}")))?;
        }

        Ok(())
    }

    pub async fn next_message(&self) -> Option<String> {
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

fn subscription_message(channel: &str, auth_token: Option<String>) -> serde_json::Value {
    let mut message = serde_json::json!({
        "type": "subscribe",
        "channel": channel,
    });

    if let Some(token) = auth_token {
        message["auth"] = serde_json::Value::String(token);
    }

    message
}
