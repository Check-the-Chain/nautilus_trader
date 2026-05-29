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

//! Lighter WebSocket feed handler.
//!
//! The outer client owns the public interface and connection setup; this handler
//! owns all WebSocket I/O after connect. This follows the Nautilus adapter
//! pattern used by Hyperliquid and Deribit: sends flow through a command enum
//! and raw frames are consumed at one I/O boundary.

use std::{collections::HashMap, sync::Arc};

#[cfg(feature = "latency-probe")]
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_network::{RECONNECTED, websocket::WebSocketClient};
use serde::Deserialize;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

pub(crate) type WsEventHandler = Box<dyn FnMut(WsEvent) + Send + 'static>;
type SendCompletion = oneshot::Sender<std::result::Result<(), String>>;

#[derive(Debug)]
pub(crate) enum HandlerCommand {
    SetClient(Arc<WebSocketClient>),
    SendText {
        text: String,
        completion: Option<SendCompletion>,
    },
    SendPong(Vec<u8>),
    Subscribe {
        channel: String,
        auth_token: Option<String>,
    },
    Unsubscribe {
        channel: String,
    },
    ReplaySubscriptions,
    Disconnect,
}

#[derive(Debug)]
pub(crate) struct WsEvent {
    pub(crate) text: String,
    #[cfg(feature = "latency-probe")]
    pub(crate) received_ns: u64,
}

impl WsEvent {
    fn new(text: String) -> Self {
        Self {
            text,
            #[cfg(feature = "latency-probe")]
            received_ns: get_atomic_clock_realtime().get_time_ns().as_u64(),
        }
    }
}

#[derive(Deserialize)]
struct WsPingProbe<'a> {
    #[serde(rename = "type", borrow)]
    msg_type: &'a str,
}

pub(crate) struct LighterWsFeedHandler {
    client: Option<Arc<WebSocketClient>>,
    cmd_rx: mpsc::UnboundedReceiver<HandlerCommand>,
    raw_rx: mpsc::UnboundedReceiver<Message>,
    default_auth_token: Arc<Mutex<Option<String>>>,
    subscriptions: Arc<Mutex<HashMap<String, Option<String>>>>,
    event_handler: Option<WsEventHandler>,
    event_tx: Option<mpsc::UnboundedSender<WsEvent>>,
    stopped: bool,
}

impl LighterWsFeedHandler {
    pub(crate) fn new(
        cmd_rx: mpsc::UnboundedReceiver<HandlerCommand>,
        raw_rx: mpsc::UnboundedReceiver<Message>,
        default_auth_token: Arc<Mutex<Option<String>>>,
        subscriptions: Arc<Mutex<HashMap<String, Option<String>>>>,
        event_handler: Option<WsEventHandler>,
        event_tx: Option<mpsc::UnboundedSender<WsEvent>>,
    ) -> Self {
        Self {
            client: None,
            cmd_rx,
            raw_rx,
            default_auth_token,
            subscriptions,
            event_handler,
            event_tx,
            stopped: false,
        }
    }

    pub(crate) async fn run(&mut self) {
        loop {
            tokio::select! {
                biased;
                Some(cmd) = self.cmd_rx.recv() => self.process_command(cmd).await,
                Some(message) = self.raw_rx.recv() => self.process_raw_message(message).await,
                else => break,
            }

            if self.stopped {
                break;
            }
        }
    }

    async fn process_command(&mut self, cmd: HandlerCommand) {
        match cmd {
            HandlerCommand::SetClient(client) => {
                self.client = Some(client);
            }
            HandlerCommand::SendText { text, completion } => {
                let result = match self.client.clone() {
                    Some(client) => client.send_text(text, None).await,
                    None => Ok(()),
                };
                if let Some(completion) = completion {
                    let _ = completion.send(result.map_err(|error| error.to_string()));
                }
            }
            HandlerCommand::SendPong(payload) => {
                if let Some(client) = self.client.clone()
                    && let Err(error) = client.send_pong(payload).await
                {
                    log::warn!("Failed to send Lighter websocket pong: {error}");
                }
            }
            HandlerCommand::Subscribe {
                channel,
                auth_token,
            } => {
                let auth_token = match auth_token {
                    Some(token) => Some(token),
                    None => self.default_auth_token.lock().await.clone(),
                };
                let mut payload = serde_json::json!({
                    "type": "subscribe",
                    "channel": channel,
                });
                if let Some(token) = auth_token {
                    payload["auth"] = serde_json::Value::String(token);
                }
                if let Some(client) = self.client.clone()
                    && let Err(error) = client.send_text(payload.to_string(), None).await
                {
                    log::warn!("Failed to send Lighter websocket subscription: {error}");
                }
            }
            HandlerCommand::Unsubscribe { channel } => {
                let payload = serde_json::json!({
                    "type": "unsubscribe",
                    "channel": channel,
                })
                .to_string();
                if let Some(client) = self.client.clone()
                    && let Err(error) = client.send_text(payload, None).await
                {
                    log::warn!(
                        "Failed to send Lighter websocket unsubscription channel={channel}: {error}"
                    );
                }
            }
            HandlerCommand::ReplaySubscriptions => self.replay_subscriptions().await,
            HandlerCommand::Disconnect => {
                if let Some(client) = &self.client {
                    client.disconnect().await;
                }
                self.stopped = true;
            }
        }
    }

    async fn process_raw_message(&mut self, message: Message) {
        match message {
            Message::Text(text) => {
                let text = text.to_string();
                if text == RECONNECTED {
                    self.replay_subscriptions().await;
                    return;
                }
                if is_ping_message(&text) {
                    if let Some(client) = self.client.clone() {
                        let _ = client
                            .send_text(serde_json::json!({"type": "pong"}).to_string(), None)
                            .await;
                    }
                    return;
                }
                self.dispatch_event(text);
            }
            Message::Binary(bytes) => {
                if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                    self.dispatch_event(text);
                }
            }
            Message::Close(_) => {
                self.stopped = true;
            }
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }

    #[allow(
        clippy::needless_pass_by_ref_mut,
        reason = "the handler owns a non-Sync FnMut event callback; using &mut self keeps this future Send for tokio::spawn"
    )]
    async fn replay_subscriptions(&mut self) {
        let current = self.subscriptions.lock().await.clone();
        let default_auth_token = self.default_auth_token.lock().await.clone();
        for (channel, auth) in current {
            let mut payload = serde_json::json!({
                "type": "subscribe",
                "channel": channel,
            });
            if let Some(token) = auth.or_else(|| default_auth_token.clone()) {
                payload["auth"] = serde_json::Value::String(token);
            }
            if let Some(client) = self.client.clone()
                && let Err(error) = client.send_text(payload.to_string(), None).await
            {
                log::warn!(
                    "Failed to replay Lighter websocket subscription channel={channel}: {error}"
                );
                break;
            }
        }
    }

    fn dispatch_event(&mut self, text: String) {
        let event = WsEvent::new(text);
        if let Some(handler) = self.event_handler.as_mut() {
            handler(event);
            return;
        }

        if let Some(event_tx) = &self.event_tx {
            let _ = event_tx.send(event);
        }
    }
}

fn is_ping_message(text: &str) -> bool {
    text.contains("ping")
        && serde_json::from_str::<WsPingProbe<'_>>(text)
            .is_ok_and(|message| message.msg_type == "ping")
}
