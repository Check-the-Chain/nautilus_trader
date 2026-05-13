use std::collections::HashMap;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::config::Config;
use crate::error::{Result, SdkError};
use crate::models::market::PerpsMarketStats;
use crate::models::order::Order;
use crate::models::ws::{
    WsAccountAllOrdersUpdate, WsAccountAllPositionsUpdate, WsAccountAllUpdate,
    WsAccountOrdersUpdate, WsMarketStatsUpdate, WsOrderBookMessage, WsOrderBookState,
    WsTickerUpdate, WsUserStatsUpdate,
};

use super::handler::{
    AccountAllPositionsHandler, AccountHandler, AccountOrdersHandler, MarketStatsHandler,
    OrderBookHandler, UserStatsHandler,
};

type OrderBookCallback = Box<dyn Fn(i64, &WsOrderBookState) + Send + Sync>;
type TickerCallback = Box<dyn Fn(i64, Option<f64>, Option<f64>) + Send + Sync>;
type AccountCallback = Box<dyn Fn(i64, &WsAccountAllUpdate) + Send + Sync>;
type AccountOrdersCallback = Box<dyn Fn(i64, &[Order]) + Send + Sync>;
type AccountAllOrdersCallback = Box<dyn Fn(i64, &HashMap<String, Vec<Order>>) + Send + Sync>;
type AccountAllPositionsCallback = Box<dyn Fn(i64, &WsAccountAllPositionsUpdate) + Send + Sync>;
type UserStatsCallback = Box<dyn Fn(i64, &WsUserStatsUpdate) + Send + Sync>;
type MarketStatsCallback = Box<dyn Fn(i64, &WsMarketStatsUpdate) + Send + Sync>;

struct AccountOrdersSubscription {
    market_id: i64,
    account_id: i64,
}

#[must_use]
pub struct WsClient {
    ws_url: String,
    auth_token: Option<String>,
    order_book_ids: Vec<i64>,
    ticker_ids: Vec<i64>,
    account_ids: Vec<i64>,
    account_all_positions_ids: Vec<i64>,
    user_stats_ids: Vec<i64>,
    market_stats_all: bool,
    account_orders_subs: Vec<AccountOrdersSubscription>,
    account_all_orders_ids: Vec<i64>,
    on_order_book_update: Option<OrderBookCallback>,
    on_ticker_update: Option<TickerCallback>,
    on_account_update: Option<AccountCallback>,
    on_account_orders_update: Option<AccountOrdersCallback>,
    on_account_all_orders_update: Option<AccountAllOrdersCallback>,
    on_account_all_positions_update: Option<AccountAllPositionsCallback>,
    on_user_stats_update: Option<UserStatsCallback>,
    on_market_stats_update: Option<MarketStatsCallback>,
    order_book_handler: Arc<Mutex<OrderBookHandler>>,
    account_handler: Arc<Mutex<AccountHandler>>,
    account_orders_handler: Arc<Mutex<AccountOrdersHandler>>,
    account_all_positions_handler: Arc<Mutex<AccountAllPositionsHandler>>,
    user_stats_handler: Arc<Mutex<UserStatsHandler>>,
    market_stats_handler: Arc<Mutex<MarketStatsHandler>>,
}

impl WsClient {
    pub fn new(config: &Config) -> Self {
        Self {
            ws_url: config.ws_base_url(),
            auth_token: None,
            order_book_ids: Vec::new(),
            ticker_ids: Vec::new(),
            account_ids: Vec::new(),
            account_all_positions_ids: Vec::new(),
            user_stats_ids: Vec::new(),
            market_stats_all: false,
            account_orders_subs: Vec::new(),
            account_all_orders_ids: Vec::new(),
            on_order_book_update: None,
            on_ticker_update: None,
            on_account_update: None,
            on_account_orders_update: None,
            on_account_all_orders_update: None,
            on_account_all_positions_update: None,
            on_user_stats_update: None,
            on_market_stats_update: None,
            order_book_handler: Arc::new(Mutex::new(OrderBookHandler::new())),
            account_handler: Arc::new(Mutex::new(AccountHandler::new())),
            account_orders_handler: Arc::new(Mutex::new(AccountOrdersHandler::new())),
            account_all_positions_handler: Arc::new(Mutex::new(AccountAllPositionsHandler::new())),
            user_stats_handler: Arc::new(Mutex::new(UserStatsHandler::new())),
            market_stats_handler: Arc::new(Mutex::new(MarketStatsHandler::new())),
        }
    }

    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    pub fn subscribe_order_books(mut self, market_ids: Vec<i64>) -> Self {
        self.order_book_ids = market_ids;
        self
    }

    pub fn subscribe_ticker(mut self, market_ids: Vec<i64>) -> Self {
        self.ticker_ids = market_ids;
        self
    }

    pub fn subscribe_accounts(mut self, account_ids: Vec<i64>) -> Self {
        self.account_ids = account_ids;
        self
    }

    pub fn subscribe_account_all_positions(mut self, account_ids: Vec<i64>) -> Self {
        self.account_all_positions_ids = account_ids;
        self
    }

    pub fn subscribe_account_orders(mut self, market_id: i64, account_id: i64) -> Self {
        self.account_orders_subs.push(AccountOrdersSubscription {
            market_id,
            account_id,
        });
        self
    }

    pub fn subscribe_account_all_orders(mut self, account_ids: Vec<i64>) -> Self {
        self.account_all_orders_ids = account_ids;
        self
    }

    pub fn subscribe_user_stats(mut self, account_ids: Vec<i64>) -> Self {
        self.user_stats_ids = account_ids;
        self
    }

    pub fn subscribe_market_stats_all(mut self) -> Self {
        self.market_stats_all = true;
        self
    }

    pub fn on_order_book_update<F>(mut self, callback: F) -> Self
    where
        F: Fn(i64, &WsOrderBookState) + Send + Sync + 'static,
    {
        self.on_order_book_update = Some(Box::new(callback));
        self
    }

    pub fn on_ticker_update<F>(mut self, callback: F) -> Self
    where
        F: Fn(i64, Option<f64>, Option<f64>) + Send + Sync + 'static,
    {
        self.on_ticker_update = Some(Box::new(callback));
        self
    }

    pub fn on_account_update<F>(mut self, callback: F) -> Self
    where
        F: Fn(i64, &WsAccountAllUpdate) + Send + Sync + 'static,
    {
        self.on_account_update = Some(Box::new(callback));
        self
    }

    pub fn on_account_orders_update<F>(mut self, callback: F) -> Self
    where
        F: Fn(i64, &[Order]) + Send + Sync + 'static,
    {
        self.on_account_orders_update = Some(Box::new(callback));
        self
    }

    pub fn on_account_all_orders_update<F>(mut self, callback: F) -> Self
    where
        F: Fn(i64, &HashMap<String, Vec<Order>>) + Send + Sync + 'static,
    {
        self.on_account_all_orders_update = Some(Box::new(callback));
        self
    }

    pub fn on_account_all_positions_update<F>(mut self, callback: F) -> Self
    where
        F: Fn(i64, &WsAccountAllPositionsUpdate) + Send + Sync + 'static,
    {
        self.on_account_all_positions_update = Some(Box::new(callback));
        self
    }

    pub fn on_user_stats_update<F>(mut self, callback: F) -> Self
    where
        F: Fn(i64, &WsUserStatsUpdate) + Send + Sync + 'static,
    {
        self.on_user_stats_update = Some(Box::new(callback));
        self
    }

    pub fn on_market_stats_update<F>(mut self, callback: F) -> Self
    where
        F: Fn(i64, &WsMarketStatsUpdate) + Send + Sync + 'static,
    {
        self.on_market_stats_update = Some(Box::new(callback));
        self
    }

    /// Get a reference to the order book handler for reading state.
    pub fn order_book_handler(&self) -> &Arc<Mutex<OrderBookHandler>> {
        &self.order_book_handler
    }

    /// Get a reference to the account handler for reading state.
    pub fn account_handler(&self) -> &Arc<Mutex<AccountHandler>> {
        &self.account_handler
    }

    /// Get a reference to the account orders handler for reading state.
    pub fn account_orders_handler(&self) -> &Arc<Mutex<AccountOrdersHandler>> {
        &self.account_orders_handler
    }

    /// Get a reference to the account-all-positions handler for reading state.
    pub fn account_all_positions_handler(&self) -> &Arc<Mutex<AccountAllPositionsHandler>> {
        &self.account_all_positions_handler
    }

    /// Get a reference to the user stats handler for reading state.
    pub fn user_stats_handler(&self) -> &Arc<Mutex<UserStatsHandler>> {
        &self.user_stats_handler
    }

    /// Get a reference to the market stats handler for reading state.
    pub fn market_stats_handler(&self) -> &Arc<Mutex<MarketStatsHandler>> {
        &self.market_stats_handler
    }

    /// Connect and run the WebSocket message loop.
    /// This method blocks until the connection is closed or an error occurs.
    pub async fn run(&self) -> Result<()> {
        if self.order_book_ids.is_empty()
            && self.ticker_ids.is_empty()
            && self.account_ids.is_empty()
            && self.account_all_positions_ids.is_empty()
            && self.user_stats_ids.is_empty()
            && !self.market_stats_all
            && self.account_orders_subs.is_empty()
            && self.account_all_orders_ids.is_empty()
        {
            return Err(SdkError::Other("No subscriptions provided".into()));
        }

        let (ws_stream, _) = connect_async(&self.ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        while let Some(msg) = read.next().await {
            let msg = msg?;
            match msg {
                Message::Text(text) => {
                    let parsed: serde_json::Value = serde_json::from_str(&text)?;
                    let msg_type = parsed.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    let channel = parsed
                        .get("channel")
                        .and_then(|v| v.as_str())
                        .unwrap_or("?");

                    match msg_type {
                        "connected" => {
                            self.send_subscriptions(&mut write).await?;
                        }
                        "ping" => {
                            let pong = serde_json::json!({"type": "pong"});
                            write
                                .send(Message::Text(pong.to_string().into()))
                                .await
                                .map_err(SdkError::from)?;
                        }
                        "subscribed/order_book" => {
                            match serde_json::from_value::<WsOrderBookMessage>(parsed.clone()) {
                                Ok(msg) => self.handle_order_book_snapshot(&msg).await,
                                Err(err) => {
                                    tracing::warn!(
                                        err = %err,
                                        msg_type = msg_type,
                                        channel = channel,
                                        "Failed to decode order_book snapshot"
                                    );
                                    tracing::debug!(
                                        err = %err,
                                        payload = %parsed,
                                        "Failed to decode order_book snapshot payload"
                                    );
                                }
                            }
                        }
                        "update/order_book" => {
                            match serde_json::from_value::<WsOrderBookMessage>(parsed.clone()) {
                                Ok(msg) => self.handle_order_book_update(&msg).await,
                                Err(err) => {
                                    tracing::warn!(
                                        err = %err,
                                        msg_type = msg_type,
                                        channel = channel,
                                        "Failed to decode order_book update"
                                    );
                                    tracing::debug!(
                                        err = %err,
                                        payload = %parsed,
                                        "Failed to decode order_book update payload"
                                    );
                                }
                            }
                        }
                        "subscribed/ticker" | "update/ticker" => {
                            match serde_json::from_value::<WsTickerUpdate>(parsed.clone()) {
                                Ok(msg) => self.handle_ticker_update(&msg),
                                Err(err) => {
                                    tracing::warn!(
                                        err = %err,
                                        msg_type = msg_type,
                                        channel = channel,
                                        "Failed to decode ticker update"
                                    );
                                    tracing::debug!(
                                        err = %err,
                                        payload = %parsed,
                                        "Failed to decode ticker update payload"
                                    );
                                }
                            }
                        }
                        "subscribed/account_all" | "update/account_all" => {
                            match serde_json::from_value::<WsAccountAllUpdate>(parsed.clone()) {
                                Ok(msg) => self.handle_account_update(&msg).await,
                                Err(err) => {
                                    tracing::warn!(
                                        err = %err,
                                        msg_type = msg_type,
                                        channel = channel,
                                        "Failed to decode account_all update"
                                    );
                                    tracing::debug!(
                                        err = %err,
                                        payload = %parsed,
                                        "Failed to decode account_all update payload"
                                    );
                                }
                            }
                        }
                        "subscribed/account_all_positions" | "update/account_all_positions" => {
                            match serde_json::from_value::<WsAccountAllPositionsUpdate>(
                                parsed.clone(),
                            ) {
                                Ok(msg) => self.handle_account_all_positions_update(&msg).await,
                                Err(err) => {
                                    tracing::warn!(
                                        err = %err,
                                        msg_type = msg_type,
                                        channel = channel,
                                        "Failed to decode account_all_positions update"
                                    );
                                    tracing::debug!(
                                        err = %err,
                                        payload = %parsed,
                                        "Failed to decode account_all_positions update payload"
                                    );
                                }
                            }
                        }
                        "subscribed/user_stats" | "update/user_stats" => {
                            match serde_json::from_value::<WsUserStatsUpdate>(parsed.clone()) {
                                Ok(msg) => self.handle_user_stats_update(&msg).await,
                                Err(err) => {
                                    tracing::warn!(
                                        err = %err,
                                        msg_type = msg_type,
                                        channel = channel,
                                        "Failed to decode user_stats update"
                                    );
                                    tracing::debug!(
                                        err = %err,
                                        payload = %parsed,
                                        "Failed to decode user_stats update payload"
                                    );
                                }
                            }
                        }
                        "subscribed/market_stats" | "update/market_stats" => {
                            let updates = parse_market_stats_updates(&parsed);
                            if updates.is_empty() {
                                tracing::warn!(
                                    msg_type = msg_type,
                                    channel = channel,
                                    "Failed to decode market_stats update"
                                );
                                tracing::debug!(
                                    payload = %parsed,
                                    "Failed to decode market_stats update payload"
                                );
                            } else {
                                for msg in updates {
                                    self.handle_market_stats_update(&msg).await;
                                }
                            }
                        }
                        "subscribed/account_orders" | "update/account_orders" => {
                            match serde_json::from_value::<WsAccountOrdersUpdate>(parsed.clone()) {
                                Ok(msg) => self.handle_account_orders_update(&msg).await,
                                Err(err) => {
                                    tracing::warn!(
                                        err = %err,
                                        msg_type = msg_type,
                                        channel = channel,
                                        "Failed to decode account_orders update"
                                    );
                                    tracing::debug!(
                                        err = %err,
                                        payload = %parsed,
                                        "Failed to decode account_orders update payload"
                                    );
                                }
                            }
                        }
                        "subscribed/account_all_orders" | "update/account_all_orders" => {
                            match serde_json::from_value::<WsAccountAllOrdersUpdate>(parsed.clone())
                            {
                                Ok(msg) => self.handle_account_all_orders_update(&msg).await,
                                Err(err) => {
                                    tracing::warn!(
                                        err = %err,
                                        msg_type = msg_type,
                                        channel = channel,
                                        "Failed to decode account_all_orders update"
                                    );
                                    tracing::debug!(
                                        err = %err,
                                        payload = %parsed,
                                        "Failed to decode account_all_orders update payload"
                                    );
                                }
                            }
                        }
                        _ => {
                            tracing::debug!("Unhandled WS message type: {msg_type}");
                        }
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }

        Ok(())
    }

    async fn send_subscriptions(
        &self,
        write: &mut futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            Message,
        >,
    ) -> Result<()> {
        for market_id in &self.order_book_ids {
            let msg = serde_json::json!({
                "type": "subscribe",
                "channel": format!("order_book/{market_id}")
            });
            write
                .send(Message::Text(msg.to_string().into()))
                .await
                .map_err(SdkError::from)?;
        }
        for market_id in &self.ticker_ids {
            let msg = serde_json::json!({
                "type": "subscribe",
                "channel": format!("ticker/{market_id}")
            });
            write
                .send(Message::Text(msg.to_string().into()))
                .await
                .map_err(SdkError::from)?;
        }
        for account_id in &self.account_ids {
            let msg = serde_json::json!({
                "type": "subscribe",
                "channel": format!("account_all/{account_id}")
            });
            write
                .send(Message::Text(msg.to_string().into()))
                .await
                .map_err(SdkError::from)?;
        }
        for account_id in &self.account_all_positions_ids {
            let mut msg = serde_json::json!({
                "type": "subscribe",
                "channel": format!("account_all_positions/{account_id}")
            });
            if let Some(ref token) = self.auth_token {
                msg["auth"] = serde_json::Value::String(token.clone());
            }
            write
                .send(Message::Text(msg.to_string().into()))
                .await
                .map_err(SdkError::from)?;
        }
        for account_id in &self.user_stats_ids {
            let mut msg = serde_json::json!({
                "type": "subscribe",
                "channel": format!("user_stats/{account_id}")
            });
            if let Some(ref token) = self.auth_token {
                msg["auth"] = serde_json::Value::String(token.clone());
            }
            write
                .send(Message::Text(msg.to_string().into()))
                .await
                .map_err(SdkError::from)?;
        }
        if self.market_stats_all {
            let msg = serde_json::json!({
                "type": "subscribe",
                "channel": "market_stats/all"
            });
            write
                .send(Message::Text(msg.to_string().into()))
                .await
                .map_err(SdkError::from)?;
        }
        for sub in &self.account_orders_subs {
            let mut msg = serde_json::json!({
                "type": "subscribe",
                "channel": format!("account_orders/{}/{}", sub.market_id, sub.account_id)
            });
            if let Some(ref token) = self.auth_token {
                msg["auth"] = serde_json::Value::String(token.clone());
            }
            write
                .send(Message::Text(msg.to_string().into()))
                .await
                .map_err(SdkError::from)?;
        }
        for account_id in &self.account_all_orders_ids {
            let mut msg = serde_json::json!({
                "type": "subscribe",
                "channel": format!("account_all_orders/{account_id}")
            });
            if let Some(ref token) = self.auth_token {
                msg["auth"] = serde_json::Value::String(token.clone());
            }
            write
                .send(Message::Text(msg.to_string().into()))
                .await
                .map_err(SdkError::from)?;
        }
        Ok(())
    }

    async fn handle_order_book_snapshot(&self, msg: &WsOrderBookMessage) {
        let market_id = match parse_market_id_from_channel(&msg.channel) {
            Some(id) => id,
            None => return,
        };

        let mut handler = self.order_book_handler.lock().await;
        handler.set_snapshot(market_id, WsOrderBookState::from(&msg.order_book));
        if let (Some(cb), Some(state)) = (&self.on_order_book_update, handler.get(market_id)) {
            cb(market_id, state);
        }
    }

    async fn handle_order_book_update(&self, msg: &WsOrderBookMessage) {
        let market_id = match parse_market_id_from_channel(&msg.channel) {
            Some(id) => id,
            None => return,
        };

        let mut handler = self.order_book_handler.lock().await;
        handler.apply_update(market_id, &msg.order_book);
        if let (Some(cb), Some(state)) = (&self.on_order_book_update, handler.get(market_id)) {
            cb(market_id, state);
        }
    }

    fn handle_ticker_update(&self, msg: &WsTickerUpdate) {
        let channel = msg.channel.as_deref().unwrap_or("");
        let market_id = match parse_market_id_from_channel(channel) {
            Some(id) => id,
            None => return,
        };

        let (best_bid, best_ask) = match &msg.ticker {
            Some(ticker) => (ticker.b.price, ticker.a.price),
            None => (None, None),
        };

        if let Some(cb) = &self.on_ticker_update {
            cb(market_id, best_bid, best_ask);
        }
    }

    async fn handle_account_update(&self, msg: &WsAccountAllUpdate) {
        let account_id = msg.account;

        let mut handler = self.account_handler.lock().await;
        handler.set_state(account_id, msg.clone());
        if let (Some(cb), Some(state)) = (&self.on_account_update, handler.get(account_id)) {
            cb(account_id, state);
        }
    }

    async fn handle_account_all_positions_update(&self, msg: &WsAccountAllPositionsUpdate) {
        let Some(account_id) = parse_account_id_from_channel(&msg.channel) else {
            tracing::debug!("WS account_all_positions update missing numeric account_id");
            return;
        };

        let mut handler = self.account_all_positions_handler.lock().await;
        handler.set_state(account_id, msg.clone());
        if let (Some(cb), Some(state)) = (
            &self.on_account_all_positions_update,
            handler.get(account_id),
        ) {
            cb(account_id, state);
        }
    }

    async fn handle_user_stats_update(&self, msg: &WsUserStatsUpdate) {
        let account_id = parse_account_id_from_channel(&msg.channel);
        let Some(account_id) = account_id else {
            tracing::debug!("WS user_stats update missing numeric account_id");
            return;
        };

        let mut handler = self.user_stats_handler.lock().await;
        handler.set_state(account_id, msg.clone());
        if let (Some(cb), Some(state)) = (&self.on_user_stats_update, handler.get(account_id)) {
            cb(account_id, state);
        }
    }

    async fn handle_market_stats_update(&self, msg: &WsMarketStatsUpdate) {
        let Some(market) = msg.market.as_ref() else {
            tracing::debug!("WS market_stats update missing market payload");
            return;
        };
        let market_id = market
            .market_id
            .or_else(|| parse_market_id_from_channel(&msg.channel));
        let Some(market_id) = market_id else {
            tracing::debug!("WS market_stats update missing numeric market_id");
            return;
        };

        let mut handler = self.market_stats_handler.lock().await;
        handler.set_state(market_id, msg.clone());
        if let (Some(cb), Some(state)) = (&self.on_market_stats_update, handler.get(market_id)) {
            cb(market_id, state);
        }
    }

    async fn handle_account_orders_update(&self, msg: &WsAccountOrdersUpdate) {
        let market_id = parse_market_id_from_channel(&msg.channel)
            .or_else(|| msg.orders.keys().find_map(|k| parse_market_id_token(k)));
        let Some(market_id) = market_id else {
            tracing::debug!("WS account_orders update missing numeric market_id");
            return;
        };

        let orders: Vec<Order> = msg.orders.values().flatten().cloned().collect();
        let mut handler = self.account_orders_handler.lock().await;
        handler.set_orders(market_id, orders.clone());
        handler.set_orders_for_account(msg.account, market_id, orders);
        if let (Some(cb), Some(orders)) = (&self.on_account_orders_update, handler.get(market_id)) {
            cb(market_id, orders);
        }
    }

    async fn handle_account_all_orders_update(&self, msg: &WsAccountAllOrdersUpdate) {
        let Some(account_id) = parse_account_id_from_channel(&msg.channel) else {
            tracing::debug!("WS account_all_orders update missing numeric account_id");
            return;
        };

        // Update account-scoped per-market handler state.
        let mut handler = self.account_orders_handler.lock().await;
        for (market_key, orders) in &msg.orders {
            if let Some(market_id) = parse_market_id_token(market_key) {
                handler.set_orders_for_account(account_id, market_id, orders.clone());
            }
        }
        drop(handler);

        if let Some(cb) = &self.on_account_all_orders_update {
            cb(account_id, &msg.orders);
        }
    }
}

fn extract_channel_suffix(channel: &str) -> Option<&str> {
    channel.split(':').nth(1)
}

/// Parse market id from strings like "42" or "42/123".
fn parse_market_id_token(s: &str) -> Option<i64> {
    s.split('/').next().and_then(|v| v.parse::<i64>().ok())
}

fn parse_market_id_from_channel(channel: &str) -> Option<i64> {
    if let Some(suffix) = extract_channel_suffix(channel) {
        return parse_market_id_token(suffix);
    }

    channel.split('/').nth(1).and_then(parse_market_id_token)
}

/// Parse account id from channels like "user_stats/54255" or "user_stats:54255".
fn parse_account_id_from_channel(channel: &str) -> Option<i64> {
    if let Some(suffix) = extract_channel_suffix(channel) {
        return suffix.parse::<i64>().ok();
    }
    channel
        .split('/')
        .nth(1)
        .and_then(|s| s.parse::<i64>().ok())
}

fn looks_like_market_stats_payload(value: &serde_json::Value) -> bool {
    let Some(obj) = value.as_object() else {
        return false;
    };
    obj.contains_key("market_id")
        || obj.contains_key("symbol")
        || obj.contains_key("mark_price")
        || obj.contains_key("index_price")
        || obj.contains_key("last_trade_price")
        || obj.contains_key("current_funding_rate")
}

fn parse_market_stats_updates(parsed: &serde_json::Value) -> Vec<WsMarketStatsUpdate> {
    let msg_type = parsed
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let channel = parsed
        .get("channel")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let mut updates = Vec::new();

    let Some(payload) = parsed.get("market_stats").or_else(|| parsed.get("market")) else {
        return updates;
    };

    if looks_like_market_stats_payload(payload) {
        if let Ok(market) = serde_json::from_value::<PerpsMarketStats>(payload.clone()) {
            updates.push(WsMarketStatsUpdate {
                msg_type,
                channel,
                market: Some(market),
            });
        }
        return updates;
    }

    let Some(map) = payload.as_object() else {
        return updates;
    };
    for (market_key, value) in map {
        if !value.is_object() {
            continue;
        }
        let Ok(mut market) = serde_json::from_value::<PerpsMarketStats>(value.clone()) else {
            continue;
        };
        if market.market_id.is_none() {
            market.market_id = market_key.parse::<i64>().ok();
        }
        updates.push(WsMarketStatsUpdate {
            msg_type: msg_type.clone(),
            channel: channel.clone(),
            market: Some(market),
        });
    }

    updates
}

#[cfg(test)]
mod tests {
    use super::parse_market_stats_updates;

    #[test]
    fn parse_market_stats_updates_handles_direct_shape() {
        let raw = serde_json::json!({
            "type": "update/market_stats",
            "channel": "market_stats:91",
            "market_stats": {
                "market_id": "91",
                "symbol": "BTC-USDC",
                "mark_price": "108330.8"
            }
        });

        let updates = parse_market_stats_updates(&raw);
        assert_eq!(updates.len(), 1);
        let market = updates[0].market.as_ref().unwrap();
        assert_eq!(market.market_id, Some(91));
        assert_eq!(market.mark_price, Some(108330.8));
    }

    #[test]
    fn parse_market_stats_updates_handles_all_map_shape() {
        let raw = serde_json::json!({
            "type": "update/market_stats",
            "channel": "market_stats:all",
            "market_stats": {
                "2": {
                    "symbol": "SOL",
                    "mark_price": "85.395"
                }
            }
        });

        let updates = parse_market_stats_updates(&raw);
        assert_eq!(updates.len(), 1);
        let market = updates[0].market.as_ref().unwrap();
        assert_eq!(market.market_id, Some(2));
        assert_eq!(market.symbol.as_deref(), Some("SOL"));
        assert_eq!(market.mark_price, Some(85.395));
    }

    #[test]
    fn parse_market_stats_updates_handles_multiple_markets() {
        let raw = serde_json::json!({
            "type": "update/market_stats",
            "channel": "market_stats:all",
            "market_stats": {
                "2": {
                    "symbol": "SOL",
                    "mark_price": "85.395"
                },
                "91": {
                    "symbol": "BTC-USDC",
                    "mark_price": "108330.8"
                }
            }
        });

        let updates = parse_market_stats_updates(&raw);
        assert_eq!(updates.len(), 2);
        assert!(updates.iter().any(|u| {
            let market = u.market.as_ref().unwrap();
            market.market_id == Some(2) && market.mark_price == Some(85.395)
        }));
        assert!(updates.iter().any(|u| {
            let market = u.market.as_ref().unwrap();
            market.market_id == Some(91) && market.mark_price == Some(108330.8)
        }));
    }
}
