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
    collections::{BTreeMap, VecDeque},
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use ahash::{AHashMap, AHashSet};
use async_trait::async_trait;
use nautilus_common::{
    clients::ExecutionClient,
    live::{runner::get_exec_event_sender, runtime::get_runtime},
    messages::execution::{
        BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
        ModifyOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
    },
};
use nautilus_core::{
    MUTEX_POISONED, UUID4, UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{
        AccountType, ContingencyType, OmsType, OrderSide, OrderStatus, OrderType,
        PositionSideSpecified, TimeInForce,
    },
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, Venue, VenueOrderId},
    instruments::Instrument,
    orders::{Order, any::OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance, Price, Quantity},
};
use tokio::{sync::Mutex as AsyncMutex, task::JoinHandle, time::timeout};

use crate::{
    client::{LighterCancelOrderRequest, LighterModifyOrderRequest, LighterSubmitOrderRequest},
    common::{
        LighterInstrumentMeta, LighterInstrumentRegistry, account_balances_from_assets,
        account_position_is_nonzero, lighter_client_order_index, load_instrument_registry,
        margin_balances_from_positions, order_report_from_lighter, position_report_from_lighter,
        position_reports_from_detailed_account, to_lighter_price, to_lighter_size, venue,
    },
    config::{Config, LighterExecClientConfig},
    error::SdkError,
    ffi::signer::SignedTx,
    http::client::LighterHttpClient,
    models::{
        account::{AccountPosition, DetailedAccounts},
        order::Orders,
        trade::Trades,
        transaction::{RespSendTx, RespSendTxBatch},
        ws::{
            Position as WsPosition, PositionWithDiscount, WsAccountAllOrdersUpdate,
            WsAccountAllPositionsUpdate, WsAccountAllTradesUpdate, WsAccountAllUpdate, WsMessage,
        },
    },
    websocket::client::LighterWebSocketClient,
};

const ALL_MARKETS_ID: i64 = 255;

const LIGHTER_HTTP_MAX_BATCH_TX_COUNT: usize = 50;
const LIGHTER_WS_MAX_BATCH_TX_COUNT: usize = 15;
const LIGHTER_MARKET_ORDER_BUY_PRICE_BUFFER: f64 = 1.01;
const LIGHTER_MARKET_ORDER_SELL_PRICE_BUFFER: f64 = 0.99;

#[derive(Clone, Copy, Debug)]
struct LighterOrderKeyRouter {
    taker_api_key_index: Option<u8>,
    maker_api_key_index: Option<u8>,
}

impl LighterOrderKeyRouter {
    const fn new(taker_api_key_index: Option<u8>, maker_api_key_index: Option<u8>) -> Self {
        Self {
            taker_api_key_index,
            maker_api_key_index,
        }
    }

    fn from_config(config: &LighterExecClientConfig) -> Self {
        Self::new(config.api_key_index, config.maker_api_key_index)
    }

    fn submit_key(self, order: &OrderAny) -> Option<u8> {
        if order.is_post_only() {
            self.maker_api_key_index.or(self.taker_api_key_index)
        } else {
            self.taker_api_key_index
        }
    }

    fn cancel_key(self, order: Option<&OrderAny>) -> Option<u8> {
        order.map_or(self.taker_api_key_index, |order| self.submit_key(order))
    }
}

#[async_trait]
pub trait LighterExecutionApi: std::fmt::Debug + Send + Sync {
    fn max_batch_tx_count(&self) -> usize {
        LIGHTER_HTTP_MAX_BATCH_TX_COUNT
    }

    async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn create_auth_token(
        &self,
        deadline_unix_secs: i64,
        api_key_index: Option<u8>,
    ) -> anyhow::Result<String>;
    async fn request_account(
        &self,
        account_index: i64,
        auth_token: &str,
    ) -> anyhow::Result<DetailedAccounts>;
    async fn request_account_active_orders(
        &self,
        account_index: i64,
        market_id: i64,
        auth_token: &str,
    ) -> anyhow::Result<Orders>;
    async fn request_account_inactive_orders(
        &self,
        account_index: i64,
        market_id: i64,
        auth_token: &str,
        cursor: Option<&str>,
    ) -> anyhow::Result<Orders>;
    async fn request_account_trades(
        &self,
        account_index: i64,
        auth_token: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> anyhow::Result<Trades>;
    async fn submit_order(&self, request: LighterSubmitOrderRequest) -> anyhow::Result<RespSendTx>;
    async fn submit_order_batch(
        &self,
        requests: Vec<LighterSubmitOrderRequest>,
    ) -> anyhow::Result<RespSendTxBatch>;
    async fn modify_order(&self, request: LighterModifyOrderRequest) -> anyhow::Result<RespSendTx>;
    async fn cancel_order(
        &self,
        market_index: i32,
        order_index: i64,
        api_key_index: Option<u8>,
    ) -> anyhow::Result<RespSendTx>;
    async fn cancel_order_batch(
        &self,
        requests: Vec<LighterCancelOrderRequest>,
    ) -> anyhow::Result<RespSendTxBatch>;
    async fn cancel_all_orders(
        &self,
        time_in_force: i32,
        timestamp_ms: i64,
        api_key_index: Option<u8>,
    ) -> anyhow::Result<RespSendTx>;
}

#[derive(Clone, Debug)]
struct LighterHttpExecutionApi {
    client: LighterHttpClient,
}

#[async_trait]
impl LighterExecutionApi for LighterHttpExecutionApi {
    async fn create_auth_token(
        &self,
        deadline_unix_secs: i64,
        api_key_index: Option<u8>,
    ) -> anyhow::Result<String> {
        self.client
            .create_auth_token(deadline_unix_secs, api_key_index)
            .await
            .map_err(Into::into)
    }

    async fn request_account(
        &self,
        account_index: i64,
        auth_token: &str,
    ) -> anyhow::Result<DetailedAccounts> {
        self.client
            .rest()
            .get_detailed_account_by_index(account_index, auth_token)
            .await
            .map_err(Into::into)
    }

    async fn request_account_active_orders(
        &self,
        account_index: i64,
        market_id: i64,
        auth_token: &str,
    ) -> anyhow::Result<Orders> {
        self.client
            .rest()
            .get_account_active_orders(account_index, market_id, auth_token)
            .await
            .map_err(Into::into)
    }

    async fn request_account_inactive_orders(
        &self,
        account_index: i64,
        market_id: i64,
        auth_token: &str,
        cursor: Option<&str>,
    ) -> anyhow::Result<Orders> {
        self.client
            .rest()
            .get_account_inactive_orders(account_index, market_id, auth_token, cursor)
            .await
            .map_err(Into::into)
    }

    async fn request_account_trades(
        &self,
        account_index: i64,
        auth_token: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> anyhow::Result<Trades> {
        self.client
            .rest()
            .get_account_trades(account_index, auth_token, limit, cursor)
            .await
            .map_err(Into::into)
    }

    async fn submit_order(&self, request: LighterSubmitOrderRequest) -> anyhow::Result<RespSendTx> {
        let response = self
            .client
            .submit_order(
                request.market_index,
                request.client_order_index,
                request.base_amount,
                request.price,
                request.is_ask,
                request.order_type,
                request.time_in_force,
                request.reduce_only,
                request.trigger_price,
                request.order_expiry,
                request.api_key_index,
                None,
            )
            .await?;
        Ok(serde_json::from_str(&response)?)
    }

    async fn submit_order_batch(
        &self,
        requests: Vec<LighterSubmitOrderRequest>,
    ) -> anyhow::Result<RespSendTxBatch> {
        let response = self.client.submit_order_batch(requests).await?;
        Ok(serde_json::from_str(&response)?)
    }

    async fn modify_order(&self, request: LighterModifyOrderRequest) -> anyhow::Result<RespSendTx> {
        let response = self
            .client
            .modify_order(
                request.market_index,
                request.order_index,
                request.base_amount,
                request.price,
                request.trigger_price,
                request.api_key_index,
                None,
            )
            .await?;
        Ok(serde_json::from_str(&response)?)
    }

    async fn cancel_order_batch(
        &self,
        requests: Vec<LighterCancelOrderRequest>,
    ) -> anyhow::Result<RespSendTxBatch> {
        let response = self.client.cancel_order_batch(requests).await?;
        Ok(serde_json::from_str(&response)?)
    }

    async fn cancel_order(
        &self,
        market_index: i32,
        order_index: i64,
        api_key_index: Option<u8>,
    ) -> anyhow::Result<RespSendTx> {
        let response = self
            .client
            .cancel_order(market_index, order_index, api_key_index, None)
            .await?;
        Ok(serde_json::from_str(&response)?)
    }

    async fn cancel_all_orders(
        &self,
        time_in_force: i32,
        timestamp_ms: i64,
        api_key_index: Option<u8>,
    ) -> anyhow::Result<RespSendTx> {
        let response = self
            .client
            .cancel_all_orders(time_in_force, timestamp_ms, api_key_index, None)
            .await?;
        Ok(serde_json::from_str(&response)?)
    }
}

#[derive(Debug)]
enum LighterWsTxError {
    BeforeSend(SdkError),
    AfterSend(SdkError),
}

impl fmt::Display for LighterWsTxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeforeSend(error) => write!(f, "WebSocket tx failed before send: {error}"),
            Self::AfterSend(error) => write!(f, "WebSocket tx failed after send: {error}"),
        }
    }
}

#[derive(Clone, Debug)]
struct LighterWsTxExecutionApi {
    http: LighterHttpExecutionApi,
    tx_ws_client: LighterWebSocketClient,
    send_lock: Arc<AsyncMutex<()>>,
    response_timeout: Duration,
}

impl LighterWsTxExecutionApi {
    fn new(client: LighterHttpClient, ws_url: String, response_timeout: Duration) -> Self {
        Self {
            http: LighterHttpExecutionApi { client },
            tx_ws_client: LighterWebSocketClient::new(ws_url, None),
            send_lock: Arc::new(AsyncMutex::new(())),
            response_timeout,
        }
    }

    async fn send_signed_tx_ws(
        &self,
        signed_tx: &SignedTx,
    ) -> Result<RespSendTx, LighterWsTxError> {
        let request_id = signed_tx.tx_hash.as_str();
        let _guard = self.send_lock.lock().await;
        self.ensure_tx_ws_connected()
            .await
            .map_err(LighterWsTxError::BeforeSend)?;

        let tx_info = serde_json::from_str::<serde_json::Value>(&signed_tx.tx_info)
            .map_err(SdkError::from)
            .map_err(LighterWsTxError::BeforeSend)?;
        let request = serde_json::json!({
            "type": "jsonapi/sendtx",
            "data": {
                "id": signed_tx.tx_hash,
                "tx_type": signed_tx.tx_type,
                "tx_info": tx_info,
            },
        });
        self.tx_ws_client
            .send_json(request)
            .await
            .map_err(LighterWsTxError::BeforeSend)?;

        timeout(
            self.response_timeout,
            self.next_tx_response::<RespSendTx>(request_id),
        )
        .await
        .map_err(|_| {
            LighterWsTxError::AfterSend(SdkError::Other(
                "Timed out waiting for Lighter WebSocket tx response".to_string(),
            ))
        })?
        .map_err(LighterWsTxError::AfterSend)
    }

    async fn send_signed_tx_batch_ws(
        &self,
        signed_txs: &[SignedTx],
    ) -> Result<RespSendTxBatch, LighterWsTxError> {
        if signed_txs.len() > LIGHTER_WS_MAX_BATCH_TX_COUNT {
            return Err(LighterWsTxError::BeforeSend(SdkError::Other(format!(
                "Lighter WebSocket batch size {} exceeds maximum {}",
                signed_txs.len(),
                LIGHTER_WS_MAX_BATCH_TX_COUNT
            ))));
        }

        let _guard = self.send_lock.lock().await;
        self.ensure_tx_ws_connected()
            .await
            .map_err(LighterWsTxError::BeforeSend)?;

        let tx_types = signed_txs.iter().map(|tx| tx.tx_type).collect::<Vec<_>>();
        let tx_infos = signed_txs
            .iter()
            .map(|tx| tx.tx_info.as_str())
            .collect::<Vec<_>>();
        let tx_types = serde_json::to_string(&tx_types)
            .map_err(SdkError::from)
            .map_err(LighterWsTxError::BeforeSend)?;
        let tx_infos = serde_json::to_string(&tx_infos)
            .map_err(SdkError::from)
            .map_err(LighterWsTxError::BeforeSend)?;
        let request_id = signed_txs
            .first()
            .map_or("lighter_batch", |tx| tx.tx_hash.as_str());
        let request = serde_json::json!({
            "type": "jsonapi/sendtxbatch",
            "data": {
                "id": request_id,
                "tx_types": tx_types,
                "tx_infos": tx_infos,
            },
        });
        self.tx_ws_client
            .send_json(request)
            .await
            .map_err(LighterWsTxError::BeforeSend)?;

        timeout(
            self.response_timeout,
            self.next_tx_response::<RespSendTxBatch>(request_id),
        )
        .await
        .map_err(|_| {
            LighterWsTxError::AfterSend(SdkError::Other(
                "Timed out waiting for Lighter WebSocket batch tx response".to_string(),
            ))
        })?
        .map_err(LighterWsTxError::AfterSend)
    }

    async fn ensure_tx_ws_connected(&self) -> Result<(), SdkError> {
        if !self.tx_ws_client.is_active() {
            self.tx_ws_client.connect().await?;
        }
        Ok(())
    }

    async fn next_tx_response<T>(&self, expected_id: &str) -> Result<T, SdkError>
    where
        T: serde::de::DeserializeOwned,
    {
        loop {
            let Some(message) = self.tx_ws_client.next_message().await else {
                return Err(SdkError::Other(
                    "Lighter WebSocket closed before tx response".to_string(),
                ));
            };
            let value: serde_json::Value = serde_json::from_str(&message)?;
            if value.get("type").and_then(|value| value.as_str()) == Some("ping") {
                continue;
            }

            let payload = if let Some(data) = value.get("data") {
                if let Some(response) = data.get("response").or_else(|| data.get("result")) {
                    response.clone()
                } else {
                    data.clone()
                }
            } else {
                value.clone()
            };

            if !tx_response_matches_request(&value, &payload, expected_id) {
                continue;
            }

            if let Some(error) = value.get("error") {
                return Err(SdkError::Other(format!(
                    "Lighter WebSocket tx error response: {error}"
                )));
            }

            let Some(payload) = normalize_send_tx_response(payload) else {
                continue;
            };

            return serde_json::from_value(payload).map_err(Into::into);
        }
    }

    async fn fallback_signed_tx(
        &self,
        signer: &crate::client::SignerClient,
        signed_tx: &SignedTx,
        api_key: u8,
        error: LighterWsTxError,
    ) -> anyhow::Result<RespSendTx> {
        match error {
            LighterWsTxError::BeforeSend(error) => {
                log::warn!("Lighter WebSocket tx unavailable, falling back to HTTP: {error}");
                let result = signer.send_signed_tx(signed_tx).await;
                signer.handle_signed_tx_result(&result, api_key).await;
                Ok(result?)
            }
            LighterWsTxError::AfterSend(error) => Err(error.into()),
        }
    }

    async fn fallback_signed_batch(
        &self,
        signer: &crate::client::SignerClient,
        signed_txs: &[SignedTx],
        api_key: u8,
        count: usize,
        error: LighterWsTxError,
    ) -> anyhow::Result<RespSendTxBatch> {
        match error {
            LighterWsTxError::BeforeSend(error) => {
                log::warn!("Lighter WebSocket batch tx unavailable, falling back to HTTP: {error}");
                let result = signer.sign_and_send_batch(signed_txs).await;
                signer
                    .handle_signed_batch_result(&result, api_key, count)
                    .await;
                Ok(result?)
            }
            LighterWsTxError::AfterSend(error) => Err(error.into()),
        }
    }
}

fn normalize_send_tx_response(mut value: serde_json::Value) -> Option<serde_json::Value> {
    if let serde_json::Value::Object(fields) = &mut value {
        if fields.contains_key("code") {
            return Some(value);
        }
        if fields.contains_key("tx_hash") {
            fields.insert("code".to_string(), serde_json::Value::from(200));
            return Some(value);
        }
        return None;
    }
    Some(value)
}

fn tx_response_matches_request(
    envelope: &serde_json::Value,
    payload: &serde_json::Value,
    expected_id: &str,
) -> bool {
    if expected_id.is_empty() {
        return true;
    }

    if let Some(candidate) = [
        json_field_as_string(envelope, &["id"]),
        json_field_as_string(envelope, &["data", "id"]),
        json_field_as_string(envelope, &["data", "request_id"]),
        json_field_as_string(payload, &["id"]),
        json_field_as_string(payload, &["request_id"]),
    ]
    .into_iter()
    .flatten()
    .next()
    {
        return candidate == expected_id;
    }

    tx_hash_matches(payload.get("tx_hash"), expected_id)
}

fn json_field_as_string(value: &serde_json::Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    match current {
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn tx_hash_matches(value: Option<&serde_json::Value>, expected_id: &str) -> bool {
    match value {
        Some(serde_json::Value::String(value)) => value == expected_id,
        Some(serde_json::Value::Array(values)) => values
            .iter()
            .any(|value| value.as_str().is_some_and(|tx_hash| tx_hash == expected_id)),
        _ => false,
    }
}

#[async_trait]
impl LighterExecutionApi for LighterWsTxExecutionApi {
    fn max_batch_tx_count(&self) -> usize {
        LIGHTER_WS_MAX_BATCH_TX_COUNT
    }

    async fn close(&self) -> anyhow::Result<()> {
        self.tx_ws_client.close().await.map_err(Into::into)
    }

    async fn create_auth_token(
        &self,
        deadline_unix_secs: i64,
        api_key_index: Option<u8>,
    ) -> anyhow::Result<String> {
        self.http
            .create_auth_token(deadline_unix_secs, api_key_index)
            .await
    }

    async fn request_account(
        &self,
        account_index: i64,
        auth_token: &str,
    ) -> anyhow::Result<DetailedAccounts> {
        self.http.request_account(account_index, auth_token).await
    }

    async fn request_account_active_orders(
        &self,
        account_index: i64,
        market_id: i64,
        auth_token: &str,
    ) -> anyhow::Result<Orders> {
        self.http
            .request_account_active_orders(account_index, market_id, auth_token)
            .await
    }

    async fn request_account_inactive_orders(
        &self,
        account_index: i64,
        market_id: i64,
        auth_token: &str,
        cursor: Option<&str>,
    ) -> anyhow::Result<Orders> {
        self.http
            .request_account_inactive_orders(account_index, market_id, auth_token, cursor)
            .await
    }

    async fn request_account_trades(
        &self,
        account_index: i64,
        auth_token: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> anyhow::Result<Trades> {
        self.http
            .request_account_trades(account_index, auth_token, limit, cursor)
            .await
    }

    async fn submit_order(&self, request: LighterSubmitOrderRequest) -> anyhow::Result<RespSendTx> {
        let (signer, api_key, signed_tx) = self.http.client.sign_submit_order(request).await?;
        match self.send_signed_tx_ws(&signed_tx).await {
            Ok(response) => {
                let result = Ok(response);
                signer.handle_signed_tx_result(&result, api_key).await;
                Ok(result?)
            }
            Err(error) => {
                self.fallback_signed_tx(&signer, &signed_tx, api_key, error)
                    .await
            }
        }
    }

    async fn submit_order_batch(
        &self,
        requests: Vec<LighterSubmitOrderRequest>,
    ) -> anyhow::Result<RespSendTxBatch> {
        if requests.is_empty() {
            return Ok(RespSendTxBatch {
                code: 200,
                message: None,
                tx_hash: Some(Vec::new()),
                predicted_execution_time_ms: None,
                volume_quota_remaining: None,
            });
        }

        let count = requests.len();
        let (signer, api_key, signed_txs) =
            self.http.client.sign_submit_order_batch(&requests).await?;
        match self.send_signed_tx_batch_ws(&signed_txs).await {
            Ok(response) => {
                let result = Ok(response);
                signer
                    .handle_signed_batch_result(&result, api_key, count)
                    .await;
                Ok(result?)
            }
            Err(error) => {
                self.fallback_signed_batch(&signer, &signed_txs, api_key, count, error)
                    .await
            }
        }
    }

    async fn modify_order(&self, request: LighterModifyOrderRequest) -> anyhow::Result<RespSendTx> {
        let (signer, api_key, signed_tx) = self.http.client.sign_modify_order(request).await?;
        match self.send_signed_tx_ws(&signed_tx).await {
            Ok(response) => {
                let result = Ok(response);
                signer.handle_signed_tx_result(&result, api_key).await;
                Ok(result?)
            }
            Err(error) => {
                self.fallback_signed_tx(&signer, &signed_tx, api_key, error)
                    .await
            }
        }
    }

    async fn cancel_order(
        &self,
        market_index: i32,
        order_index: i64,
        api_key_index: Option<u8>,
    ) -> anyhow::Result<RespSendTx> {
        let (signer, api_key, signed_tx) = self
            .http
            .client
            .sign_cancel_order(market_index, order_index, api_key_index)
            .await?;
        match self.send_signed_tx_ws(&signed_tx).await {
            Ok(response) => {
                let result = Ok(response);
                signer.handle_signed_tx_result(&result, api_key).await;
                Ok(result?)
            }
            Err(error) => {
                self.fallback_signed_tx(&signer, &signed_tx, api_key, error)
                    .await
            }
        }
    }

    async fn cancel_order_batch(
        &self,
        requests: Vec<LighterCancelOrderRequest>,
    ) -> anyhow::Result<RespSendTxBatch> {
        if requests.is_empty() {
            return Ok(RespSendTxBatch {
                code: 200,
                message: None,
                tx_hash: Some(Vec::new()),
                predicted_execution_time_ms: None,
                volume_quota_remaining: None,
            });
        }

        let count = requests.len();
        let (signer, api_key, signed_txs) =
            self.http.client.sign_cancel_order_batch(&requests).await?;
        match self.send_signed_tx_batch_ws(&signed_txs).await {
            Ok(response) => {
                let result = Ok(response);
                signer
                    .handle_signed_batch_result(&result, api_key, count)
                    .await;
                Ok(result?)
            }
            Err(error) => {
                self.fallback_signed_batch(&signer, &signed_txs, api_key, count, error)
                    .await
            }
        }
    }

    async fn cancel_all_orders(
        &self,
        time_in_force: i32,
        timestamp_ms: i64,
        api_key_index: Option<u8>,
    ) -> anyhow::Result<RespSendTx> {
        let (signer, api_key, signed_tx) = self
            .http
            .client
            .sign_cancel_all_orders(time_in_force, timestamp_ms, api_key_index)
            .await?;
        match self.send_signed_tx_ws(&signed_tx).await {
            Ok(response) => {
                let result = Ok(response);
                signer.handle_signed_tx_result(&result, api_key).await;
                Ok(result?)
            }
            Err(error) => {
                self.fallback_signed_tx(&signer, &signed_tx, api_key, error)
                    .await
            }
        }
    }
}

type RecentOrderState = (OrderStatus, UnixNanos, String);
type RecentOrderStates = Arc<Mutex<AHashMap<String, RecentOrderState>>>;

pub struct LighterExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: LighterExecClientConfig,
    emitter: ExecutionEventEmitter,
    public_http_client: LighterHttpClient,
    api: Arc<dyn LighterExecutionApi>,
    ws_client: LighterWebSocketClient,
    pending_tasks: Mutex<Vec<JoinHandle<()>>>,
    ws_stream_handle: Mutex<Option<JoinHandle<()>>>,
    registry: Arc<tokio::sync::RwLock<LighterInstrumentRegistry>>,
    auth_token: Arc<Mutex<Option<(String, Instant)>>>,
    client_order_index_to_id: Arc<Mutex<AHashMap<i64, ClientOrderId>>>,
    tracked_orders: Arc<Mutex<AHashMap<ClientOrderId, OrderAny>>>,
    venue_order_id_by_client_order_id: Arc<Mutex<AHashMap<ClientOrderId, VenueOrderId>>>,
    recent_order_states: RecentOrderStates,
    recent_order_state_queue: Arc<Mutex<VecDeque<String>>>,
    processed_trade_ids: Arc<Mutex<AHashSet<String>>>,
    processed_trade_queue: Arc<Mutex<VecDeque<String>>>,
}

impl std::fmt::Debug for LighterExecutionClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LighterExecutionClient")
            .field("client_id", &self.core.client_id)
            .field("account_id", &self.core.account_id)
            .field("is_connected", &self.core.is_connected())
            .finish()
    }
}

impl LighterExecutionClient {
    pub fn new(core: ExecutionClientCore, config: LighterExecClientConfig) -> anyhow::Result<Self> {
        let mut runtime_config = Config::for_environment(config.environment)
            .with_http_base_url(config.http_url())
            .with_ws_base_url(config.ws_url());
        if let Some(proxy) = &config.proxy_url {
            runtime_config = runtime_config.with_proxy(proxy.clone());
        }
        runtime_config = runtime_config.with_timeout_secs(config.http_timeout_secs);
        if let Some(path) = &config.signer_lib_path {
            runtime_config = runtime_config.with_signer_lib_path(path.clone());
        }

        let public_http_client = LighterHttpClient::new_public(runtime_config.clone())?;
        let private_client = LighterHttpClient::with_signer(
            runtime_config,
            config.account_index.unwrap_or_default(),
            config.credentials_map(),
            if config.nonce_mode.eq_ignore_ascii_case("api") {
                crate::nonce::NonceManagerType::Api
            } else {
                crate::nonce::NonceManagerType::Optimistic
            },
        )?;
        let api: Arc<dyn LighterExecutionApi> = Arc::new(LighterWsTxExecutionApi::new(
            private_client,
            config.ws_url(),
            Duration::from_secs(config.ws_timeout_secs),
        ));

        Self::new_with_api(core, config, public_http_client, api)
    }

    pub fn new_with_api(
        core: ExecutionClientCore,
        config: LighterExecClientConfig,
        public_http_client: LighterHttpClient,
        api: Arc<dyn LighterExecutionApi>,
    ) -> anyhow::Result<Self> {
        let ws_client = LighterWebSocketClient::new(config.readonly_ws_url(), None);
        let clock = get_atomic_clock_realtime();
        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            AccountType::Margin,
            None,
        );

        Ok(Self {
            core,
            clock,
            config,
            emitter,
            public_http_client,
            api,
            ws_client,
            pending_tasks: Mutex::new(Vec::new()),
            ws_stream_handle: Mutex::new(None),
            registry: Arc::new(tokio::sync::RwLock::new(
                LighterInstrumentRegistry::default(),
            )),
            auth_token: Arc::new(Mutex::new(None)),
            client_order_index_to_id: Arc::new(Mutex::new(AHashMap::new())),
            tracked_orders: Arc::new(Mutex::new(AHashMap::new())),
            venue_order_id_by_client_order_id: Arc::new(Mutex::new(AHashMap::new())),
            recent_order_states: Arc::new(Mutex::new(AHashMap::new())),
            recent_order_state_queue: Arc::new(Mutex::new(VecDeque::with_capacity(10_000))),
            processed_trade_ids: Arc::new(Mutex::new(AHashSet::new())),
            processed_trade_queue: Arc::new(Mutex::new(VecDeque::with_capacity(10_000))),
        })
    }

    async fn ensure_instruments_initialized(&self) -> anyhow::Result<()> {
        if self.core.instruments_initialized() {
            return Ok(());
        }
        let registry = load_instrument_registry(&self.public_http_client).await?;
        *self.registry.write().await = registry;
        self.core.set_instruments_initialized();
        Ok(())
    }

    fn auth_token_deadline_unix_secs(ttl: u64) -> anyhow::Result<i64> {
        let deadline = SystemTime::now()
            .checked_add(Duration::from_secs(ttl))
            .ok_or_else(|| anyhow::anyhow!("auth token deadline overflow"))?
            .duration_since(UNIX_EPOCH)?;
        Ok(deadline.as_secs() as i64)
    }

    async fn ensure_auth_token(&self, min_ttl_secs: u64) -> anyhow::Result<String> {
        if let Some((token, expires_at)) = self.auth_token.lock().expect(MUTEX_POISONED).clone()
            && expires_at > Instant::now() + Duration::from_secs(min_ttl_secs)
        {
            return Ok(token);
        }

        let ttl = self.config.default_auth_token_ttl_secs.max(min_ttl_secs);
        let deadline_unix_secs = Self::auth_token_deadline_unix_secs(ttl)?;
        let token = self
            .api
            .create_auth_token(deadline_unix_secs, self.config.api_key_index)
            .await?;
        self.ws_client.set_auth_token(Some(token.clone())).await;
        *self.auth_token.lock().expect(MUTEX_POISONED) =
            Some((token.clone(), Instant::now() + Duration::from_secs(ttl)));
        Ok(token)
    }

    async fn refresh_account_state(&self) -> anyhow::Result<()> {
        let token = self.ensure_auth_token(30).await?;
        let response = self
            .api
            .request_account(self.config.account_index.unwrap_or_default(), &token)
            .await?;
        let Some(account) = response.accounts.first() else {
            return Ok(());
        };

        let registry = self.registry.read().await;
        emit_account_state_from_detailed_account(
            &self.emitter,
            account,
            self.core.account_id,
            &registry,
            self.clock.get_time_ns(),
        );
        Ok(())
    }

    fn spawn_task<F>(&self, name: &'static str, future: F)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let handle = get_runtime().spawn(async move {
            if let Err(error) = future.await {
                log::warn!("Lighter execution task '{name}' failed: {error}");
            }
        });

        let mut tasks = self.pending_tasks.lock().expect(MUTEX_POISONED);
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }

    fn track_client_order(&self, client_order_id: ClientOrderId, client_order_index: i64) {
        self.client_order_index_to_id
            .lock()
            .expect(MUTEX_POISONED)
            .insert(client_order_index, client_order_id);
    }

    fn track_order(&self, order: OrderAny) {
        self.tracked_orders
            .lock()
            .expect(MUTEX_POISONED)
            .insert(order.client_order_id(), order);
    }

    fn resolve_client_order_id(&self, value: i64) -> Option<ClientOrderId> {
        self.client_order_index_to_id
            .lock()
            .expect(MUTEX_POISONED)
            .get(&value)
            .copied()
    }

    fn resolve_venue_order_id(
        &self,
        client_order_id: &ClientOrderId,
        venue_order_id: Option<VenueOrderId>,
    ) -> Option<VenueOrderId> {
        venue_order_id
            .or_else(|| self.core.cache().venue_order_id(client_order_id).copied())
            .or_else(|| {
                self.venue_order_id_by_client_order_id
                    .lock()
                    .expect(MUTEX_POISONED)
                    .get(client_order_id)
                    .copied()
            })
    }

    fn open_orders_for_cancel_all(
        &self,
        instrument_id: &InstrumentId,
        order_side: OrderSide,
    ) -> Vec<OrderAny> {
        self.core
            .cache()
            .orders_open(
                Some(&self.core.venue),
                Some(instrument_id),
                None,
                None,
                Some(order_side),
            )
            .into_iter()
            .cloned()
            .collect()
    }

    fn seed_client_order_indexes_from_cache(&self) {
        for order in self
            .core
            .cache()
            .orders_open(Some(&self.core.venue), None, None, None, None)
        {
            let client_order_id = order.client_order_id();
            self.track_client_order(
                client_order_id,
                lighter_client_order_index(&client_order_id),
            );
            self.track_order(order.clone());
            if let Some(venue_order_id) = order.venue_order_id() {
                track_venue_order_id(
                    &self.venue_order_id_by_client_order_id,
                    Some(client_order_id),
                    venue_order_id,
                );
            }
        }
    }

    fn spawn_cancel_batch_task(
        &self,
        name: &'static str,
        cancels: Vec<(CancelOrder, VenueOrderId, Option<u8>)>,
    ) {
        if cancels.is_empty() {
            return;
        }

        let registry = Arc::clone(&self.registry);
        let api = Arc::clone(&self.api);
        let max_batch_tx_count = api.max_batch_tx_count();
        let emitter = self.emitter.clone();
        let clock = self.clock;

        self.spawn_task(name, async move {
            let registry = registry.read().await;
            let mut requests_by_key: BTreeMap<
                Option<u8>,
                Vec<(LighterCancelOrderRequest, CancelOrder, VenueOrderId)>,
            > = BTreeMap::new();

            for (cancel, venue_order_id, api_key_index) in cancels {
                let Some(meta) = registry.meta_for_instrument_id(&cancel.instrument_id) else {
                    emitter.emit_order_cancel_rejected_event(
                        cancel.strategy_id,
                        cancel.instrument_id,
                        cancel.client_order_id,
                        Some(venue_order_id),
                        "Lighter metadata missing",
                        clock.get_time_ns(),
                    );
                    continue;
                };

                let order_index = match venue_order_id.as_str().parse::<i64>() {
                    Ok(order_index) => order_index,
                    Err(error) => {
                        emitter.emit_order_cancel_rejected_event(
                            cancel.strategy_id,
                            cancel.instrument_id,
                            cancel.client_order_id,
                            Some(venue_order_id),
                            &error.to_string(),
                            clock.get_time_ns(),
                        );
                        continue;
                    }
                };

                let request = LighterCancelOrderRequest {
                    market_index: meta.market_id as i32,
                    order_index,
                    api_key_index,
                };
                requests_by_key.entry(api_key_index).or_default().push((
                    request,
                    cancel,
                    venue_order_id,
                ));
            }

            for keyed_requests in requests_by_key.into_values() {
                for request_chunk in keyed_requests.chunks(max_batch_tx_count) {
                    let requests = request_chunk
                        .iter()
                        .map(|(request, _, _)| *request)
                        .collect::<Vec<_>>();
                    match api.cancel_order_batch(requests).await {
                        Ok(response) => {
                            if response.code != 200 {
                                let reason = response
                                    .message
                                    .as_deref()
                                    .unwrap_or("Lighter batch cancel failed");
                                for (_, cancel, venue_order_id) in request_chunk {
                                    emitter.emit_order_cancel_rejected_event(
                                        cancel.strategy_id,
                                        cancel.instrument_id,
                                        cancel.client_order_id,
                                        Some(*venue_order_id),
                                        reason,
                                        clock.get_time_ns(),
                                    );
                                }
                            }
                        }
                        Err(error) => {
                            let reason = format!("Lighter batch cancel failed: {error}");
                            for (_, cancel, venue_order_id) in request_chunk {
                                emitter.emit_order_cancel_rejected_event(
                                    cancel.strategy_id,
                                    cancel.instrument_id,
                                    cancel.client_order_id,
                                    Some(*venue_order_id),
                                    &reason,
                                    clock.get_time_ns(),
                                );
                            }
                        }
                    }
                }
            }

            Ok(())
        });
    }

    async fn start_ws_stream(&self) -> anyhow::Result<()> {
        if self
            .ws_stream_handle
            .lock()
            .expect(MUTEX_POISONED)
            .is_some()
        {
            return Ok(());
        }

        let token = self.ensure_auth_token(30).await?;
        self.ws_client.set_auth_token(Some(token)).await;
        self.ws_client.connect().await?;
        let account_index = self.config.account_index.unwrap_or_default();
        self.ws_client
            .subscribe(format!("account_all/{account_index}"), None)
            .await?;
        self.ws_client
            .subscribe(format!("account_all_orders/{account_index}"), None)
            .await?;
        self.ws_client
            .subscribe(format!("account_all_trades/{account_index}"), None)
            .await?;
        self.ws_client
            .subscribe(format!("account_all_positions/{account_index}"), None)
            .await?;
        self.ws_client
            .subscribe(format!("account_all_assets/{account_index}"), None)
            .await?;
        self.ws_client
            .subscribe(format!("user_stats/{account_index}"), None)
            .await?;

        let ws_client = self.ws_client.clone();
        let registry = Arc::clone(&self.registry);
        let emitter = self.emitter.clone();
        let api = Arc::clone(&self.api);
        let account_id = self.core.account_id;
        let account_index = self.config.account_index.unwrap_or_default();
        let auth_token = Arc::clone(&self.auth_token);
        let client_order_index_to_id = Arc::clone(&self.client_order_index_to_id);
        let tracked_orders = Arc::clone(&self.tracked_orders);
        let venue_order_id_by_client_order_id = Arc::clone(&self.venue_order_id_by_client_order_id);
        let processed_trade_ids = Arc::clone(&self.processed_trade_ids);
        let processed_trade_queue = Arc::clone(&self.processed_trade_queue);
        let recent_order_states = Arc::clone(&self.recent_order_states);
        let recent_order_state_queue = Arc::clone(&self.recent_order_state_queue);
        let clock = self.clock;

        let handle = get_runtime().spawn(async move {
            let account_refresh_interval = Duration::from_millis(250);
            let mut next_account_refresh = Instant::now();

            while let Some(message) = ws_client.next_message().await {
                let Ok(header) = serde_json::from_str::<WsMessage>(&message) else {
                    continue;
                };
                match header.msg_type.as_str() {
                    "update/account_all_orders" | "subscribed/account_all_orders" => {
                        if let Ok(payload) =
                            serde_json::from_str::<WsAccountAllOrdersUpdate>(&message)
                        {
                            let registry = registry.read().await;
                            for (market_id, orders) in payload.orders {
                                let Ok(market_id) = market_id.parse::<i64>() else {
                                    continue;
                                };
                                let Some(meta) = registry.meta_for_market_id(market_id) else {
                                    continue;
                                };
                                for order in orders {
                                    let ts_init = clock.get_time_ns();
                                    let report = order_report_from_lighter(
                                        &order,
                                        account_id,
                                        &meta.instrument,
                                        ts_init,
                                        |value| {
                                            client_order_index_to_id
                                                .lock()
                                                .expect(MUTEX_POISONED)
                                                .get(&value)
                                                .copied()
                                        },
                                    );
                                    process_order_report(
                                        &emitter,
                                        &tracked_orders,
                                        &venue_order_id_by_client_order_id,
                                        &recent_order_states,
                                        &recent_order_state_queue,
                                        report,
                                    );
                                }
                            }
                        }
                    }
                    "update/account_all_trades" | "subscribed/account_all_trades" => {
                        if let Ok(payload) =
                            serde_json::from_str::<WsAccountAllTradesUpdate>(&message)
                        {
                            let registry = registry.read().await;
                            for (market_id, trades) in payload.trades {
                                let Ok(market_id) = market_id.parse::<i64>() else {
                                    continue;
                                };
                                let Some(meta) = registry.meta_for_market_id(market_id) else {
                                    continue;
                                };
                                for trade in trades {
                                    let ts_init = clock.get_time_ns();
                                    if let Some(report) =
                                        crate::common::fill_report_from_lighter_trade(
                                            &trade,
                                            account_index,
                                            account_id,
                                            &meta.instrument,
                                            ts_init,
                                            |value| {
                                                client_order_index_to_id
                                                    .lock()
                                                    .expect(MUTEX_POISONED)
                                                    .get(&value)
                                                    .copied()
                                            },
                                        )
                                    {
                                        process_fill_report(
                                            &emitter,
                                            &tracked_orders,
                                            &venue_order_id_by_client_order_id,
                                            &processed_trade_ids,
                                            &processed_trade_queue,
                                            report,
                                        );
                                    }
                                }
                            }
                        }
                    }
                    "update/account_all" | "subscribed/account_all" => {
                        if let Ok(payload) = serde_json::from_str::<WsAccountAllUpdate>(&message) {
                            let balances = account_balances_from_assets(&payload.assets);
                            let positions = payload
                                .positions
                                .into_values()
                                .flatten()
                                .map(account_position_from_ws)
                                .collect::<Vec<_>>();
                            let registry = registry.read().await;
                            let ts_init = clock.get_time_ns();
                            emit_account_state_from_positions(
                                &emitter, balances, &positions, account_id, &registry, ts_init,
                            );
                        }
                    }
                    "update/account_all_positions" | "subscribed/account_all_positions" => {
                        if let Ok(payload) =
                            serde_json::from_str::<WsAccountAllPositionsUpdate>(&message)
                        {
                            let positions = payload
                                .positions
                                .into_values()
                                .flatten()
                                .map(account_position_from_ws_with_discount)
                                .collect::<Vec<_>>();
                            let registry = registry.read().await;
                            emit_position_reports(
                                &emitter,
                                &positions,
                                account_id,
                                &registry,
                                clock.get_time_ns(),
                            );
                        }
                    }
                    "update/account_all_assets" | "subscribed/user_stats" | "update/user_stats" => {
                        let now = Instant::now();
                        if now < next_account_refresh {
                            continue;
                        }
                        next_account_refresh = now + account_refresh_interval;

                        let Some((token, _)) = auth_token.lock().expect(MUTEX_POISONED).clone()
                        else {
                            continue;
                        };
                        if let Ok(response) = api.request_account(account_index, &token).await
                            && let Some(account) = response.accounts.first()
                        {
                            let registry = registry.read().await;
                            emit_account_state_from_detailed_account(
                                &emitter,
                                account,
                                account_id,
                                &registry,
                                clock.get_time_ns(),
                            );
                        }
                    }
                    _ => {}
                }
            }
        });

        *self.ws_stream_handle.lock().expect(MUTEX_POISONED) = Some(handle);
        Ok(())
    }
}

fn track_venue_order_id(
    venue_order_id_by_client_order_id: &Arc<Mutex<AHashMap<ClientOrderId, VenueOrderId>>>,
    client_order_id: Option<ClientOrderId>,
    venue_order_id: VenueOrderId,
) {
    if let Some(client_order_id) = client_order_id {
        venue_order_id_by_client_order_id
            .lock()
            .expect(MUTEX_POISONED)
            .insert(client_order_id, venue_order_id);
    }
}

fn is_duplicate_order_report(
    recent_order_states: &RecentOrderStates,
    recent_order_state_queue: &Arc<Mutex<VecDeque<String>>>,
    report: &OrderStatusReport,
) -> bool {
    let key = report.venue_order_id.to_string();
    let value = (
        report.order_status,
        report.ts_last,
        report.filled_qty.to_string(),
    );

    let mut states = recent_order_states.lock().expect(MUTEX_POISONED);
    if states.get(&key) == Some(&value) {
        return true;
    }
    states.insert(key.clone(), value);

    let mut queue = recent_order_state_queue.lock().expect(MUTEX_POISONED);
    queue.push_back(key);
    while queue.len() > 10_000 {
        if let Some(expired) = queue.pop_front() {
            states.remove(&expired);
        }
    }
    false
}

fn is_duplicate_trade_id(
    processed_trade_ids: &Arc<Mutex<AHashSet<String>>>,
    processed_trade_queue: &Arc<Mutex<VecDeque<String>>>,
    trade_id: &str,
) -> bool {
    let mut seen = processed_trade_ids.lock().expect(MUTEX_POISONED);
    if seen.contains(trade_id) {
        return true;
    }
    seen.insert(trade_id.to_string());

    let mut queue = processed_trade_queue.lock().expect(MUTEX_POISONED);
    queue.push_back(trade_id.to_string());
    while queue.len() > 10_000 {
        if let Some(expired) = queue.pop_front() {
            seen.remove(&expired);
        }
    }
    false
}

fn process_order_report(
    emitter: &ExecutionEventEmitter,
    tracked_orders: &Arc<Mutex<AHashMap<ClientOrderId, OrderAny>>>,
    venue_order_id_by_client_order_id: &Arc<Mutex<AHashMap<ClientOrderId, VenueOrderId>>>,
    recent_order_states: &RecentOrderStates,
    recent_order_state_queue: &Arc<Mutex<VecDeque<String>>>,
    mut report: OrderStatusReport,
) {
    if is_duplicate_order_report(recent_order_states, recent_order_state_queue, &report) {
        return;
    }

    track_venue_order_id(
        venue_order_id_by_client_order_id,
        report.client_order_id,
        report.venue_order_id,
    );

    if let Some(client_order_id) = report.client_order_id
        && let Some(order) = tracked_orders
            .lock()
            .expect(MUTEX_POISONED)
            .get(&client_order_id)
            .cloned()
    {
        apply_order_report_metadata(&order, &mut report);
        match report.order_status {
            OrderStatus::Rejected => {
                emitter.emit_order_rejected(
                    &order,
                    report.cancel_reason.as_deref().unwrap_or("REJECTED"),
                    report.ts_last,
                    false,
                );
            }
            OrderStatus::Accepted | OrderStatus::PartiallyFilled | OrderStatus::Filled
                if order.venue_order_id().is_none() =>
            {
                emitter.emit_order_accepted(&order, report.venue_order_id, report.ts_last);
            }
            OrderStatus::Canceled => {
                emitter.emit_order_canceled(&order, Some(report.venue_order_id), report.ts_last);
            }
            OrderStatus::Expired => {
                emitter.emit_order_expired(&order, Some(report.venue_order_id), report.ts_last);
            }
            _ => {}
        }
    }

    emitter.send_order_status_report(report);
}

fn process_fill_report(
    emitter: &ExecutionEventEmitter,
    tracked_orders: &Arc<Mutex<AHashMap<ClientOrderId, OrderAny>>>,
    venue_order_id_by_client_order_id: &Arc<Mutex<AHashMap<ClientOrderId, VenueOrderId>>>,
    processed_trade_ids: &Arc<Mutex<AHashSet<String>>>,
    processed_trade_queue: &Arc<Mutex<VecDeque<String>>>,
    report: FillReport,
) {
    if is_duplicate_trade_id(
        processed_trade_ids,
        processed_trade_queue,
        report.trade_id.as_str(),
    ) {
        return;
    }

    if let Some(client_order_id) = report.client_order_id
        && let Some(order) = tracked_orders
            .lock()
            .expect(MUTEX_POISONED)
            .get(&client_order_id)
            .cloned()
    {
        let venue_order_known = order.venue_order_id().is_some()
            || venue_order_id_by_client_order_id
                .lock()
                .expect(MUTEX_POISONED)
                .contains_key(&client_order_id);
        if !venue_order_known {
            emitter.emit_order_accepted(&order, report.venue_order_id, report.ts_event);
            track_venue_order_id(
                venue_order_id_by_client_order_id,
                Some(client_order_id),
                report.venue_order_id,
            );
        }

        emitter.emit_order_filled(
            &order,
            report.venue_order_id,
            report.venue_position_id,
            report.trade_id,
            report.last_qty,
            report.last_px,
            report.commission.currency,
            Some(report.commission),
            report.liquidity_side,
            report.ts_event,
        );
        return;
    }

    emitter.send_fill_report(report);
}

fn emit_account_state_from_detailed_account(
    emitter: &ExecutionEventEmitter,
    account: &crate::models::account::DetailedAccount,
    account_id: AccountId,
    registry: &LighterInstrumentRegistry,
    ts_init: UnixNanos,
) {
    let balances = account_balances_from_assets(account.assets.as_deref().unwrap_or(&[]));
    let positions = account.positions.as_deref().unwrap_or(&[]);
    let margins = margin_balances_from_positions(positions, registry);
    emitter.emit_account_state(balances, margins, true, ts_init);

    for report in position_reports_from_detailed_account(account, account_id, registry, ts_init) {
        emitter.send_position_report(report);
    }
}

fn emit_account_state_from_positions(
    emitter: &ExecutionEventEmitter,
    balances: Vec<AccountBalance>,
    positions: &[AccountPosition],
    account_id: AccountId,
    registry: &LighterInstrumentRegistry,
    ts_init: UnixNanos,
) {
    let margins = margin_balances_from_positions(positions, registry);
    emitter.emit_account_state(balances, margins, true, ts_init);
    emit_position_reports(emitter, positions, account_id, registry, ts_init);
}

fn emit_position_reports(
    emitter: &ExecutionEventEmitter,
    positions: &[AccountPosition],
    account_id: AccountId,
    registry: &LighterInstrumentRegistry,
    ts_init: UnixNanos,
) {
    for position in positions {
        if !account_position_is_nonzero(position) {
            continue;
        }
        let Some(instrument) = registry.instrument_for_market_id(position.market_id) else {
            continue;
        };
        emitter.send_position_report(position_report_from_lighter(
            position,
            account_id,
            &instrument,
            ts_init,
        ));
    }
}

fn account_position_from_ws(position: WsPosition) -> AccountPosition {
    AccountPosition {
        market_id: position.market_id,
        symbol: position.symbol,
        initial_margin_fraction: position.initial_margin_fraction,
        open_order_count: position.open_order_count,
        pending_order_count: position.pending_order_count,
        position_tied_order_count: position.position_tied_order_count,
        sign: position.sign,
        position: position.position,
        avg_entry_price: position.avg_entry_price,
        position_value: position.position_value,
        unrealized_pnl: position.unrealized_pnl,
        realized_pnl: position.realized_pnl,
        liquidation_price: position.liquidation_price,
        total_funding_paid_out: Some(position.total_funding_paid_out),
        margin_mode: position.margin_mode,
        allocated_margin: position.allocated_margin,
    }
}

fn account_position_from_ws_with_discount(position: PositionWithDiscount) -> AccountPosition {
    AccountPosition {
        market_id: position.market_id,
        symbol: position.symbol,
        initial_margin_fraction: position.initial_margin_fraction,
        open_order_count: position.open_order_count,
        pending_order_count: position.pending_order_count,
        position_tied_order_count: position.position_tied_order_count,
        sign: position.sign,
        position: position.position,
        avg_entry_price: position.avg_entry_price,
        position_value: position.position_value,
        unrealized_pnl: position.unrealized_pnl,
        realized_pnl: position.realized_pnl,
        liquidation_price: position.liquidation_price,
        total_funding_paid_out: Some(position.total_funding_paid_out),
        margin_mode: position.margin_mode,
        allocated_margin: position.allocated_margin,
    }
}

fn in_time_window(ts_event: UnixNanos, start: Option<UnixNanos>, end: Option<UnixNanos>) -> bool {
    start.is_none_or(|start| ts_event >= start) && end.is_none_or(|end| ts_event <= end)
}

#[async_trait(?Send)]
impl ExecutionClient for LighterExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        venue()
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account(&self.core.account_id).cloned()
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
    ) -> anyhow::Result<()> {
        self.emitter
            .emit_account_state(balances, margins, reported, ts_event);
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }
        self.emitter.set_sender(get_exec_event_sender());
        self.core.set_started();
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }
        if let Some(handle) = self.ws_stream_handle.lock().expect(MUTEX_POISONED).take() {
            handle.abort();
        }
        let api = Arc::clone(&self.api);
        get_runtime().spawn(async move {
            if let Err(error) = api.close().await {
                log::warn!("Failed to close Lighter execution API: {error}");
            }
        });
        for task in self.pending_tasks.lock().expect(MUTEX_POISONED).drain(..) {
            task.abort();
        }
        self.core.set_disconnected();
        self.core.set_stopped();
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.ensure_instruments_initialized().await?;
        self.seed_client_order_indexes_from_cache();
        self.refresh_account_state().await?;
        self.start_ws_stream().await?;
        self.core.set_connected();
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if let Some(handle) = self.ws_stream_handle.lock().expect(MUTEX_POISONED).take() {
            handle.abort();
        }
        for task in self.pending_tasks.lock().expect(MUTEX_POISONED).drain(..) {
            task.abort();
        }
        self.ws_client.close().await?;
        self.api.close().await?;
        self.core.set_disconnected();
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let cmd = cmd;
        let order = self.core.get_order(&cmd.client_order_id)?;
        let registry = Arc::clone(&self.registry);
        let api = Arc::clone(&self.api);
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let client_order_index = lighter_client_order_index(&cmd.client_order_id);
        let key_router = LighterOrderKeyRouter::from_config(&self.config);
        let api_key_index = key_router.submit_key(&order);
        if let Err(error) = validate_lighter_order(&order) {
            emitter.emit_order_denied(&order, &error.to_string());
            return Ok(());
        }
        let price = match submission_price_from_cache(
            &order,
            self.core.cache().quote(&order.instrument_id()),
        ) {
            Ok(price) => price,
            Err(error) => {
                emitter.emit_order_denied(&order, &error.to_string());
                return Ok(());
            }
        };

        self.track_client_order(cmd.client_order_id, client_order_index);
        self.track_order(order.clone());
        emitter.emit_order_submitted(&order);

        self.spawn_task("submit_order", async move {
            let registry = registry.read().await;
            let result = async {
                let meta = registry
                    .meta_for_instrument_id(&order.instrument_id())
                    .ok_or_else(|| anyhow::anyhow!("Lighter metadata missing"))?;
                let request = build_submit_order_request(
                    &order,
                    meta,
                    client_order_index,
                    price,
                    api_key_index,
                )?;
                api.submit_order(request).await
            }
            .await;

            match result {
                Ok(response) if response.code == 200 => {}
                Ok(response) => {
                    emitter.emit_order_rejected_event(
                        order.strategy_id(),
                        order.instrument_id(),
                        order.client_order_id(),
                        response
                            .message
                            .as_deref()
                            .unwrap_or("Lighter submission failed"),
                        clock.get_time_ns(),
                        false,
                    );
                }
                Err(error) => {
                    emitter.emit_order_rejected_event(
                        order.strategy_id(),
                        order.instrument_id(),
                        order.client_order_id(),
                        &format!("Lighter submission failed: {error}"),
                        clock.get_time_ns(),
                        false,
                    );
                }
            }
            Ok(())
        });

        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        let registry = Arc::clone(&self.registry);
        let api = Arc::clone(&self.api);
        let max_batch_tx_count = api.max_batch_tx_count();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let mut prepared_orders = Vec::new();
        let orders = self.core.get_orders_for_list(&cmd.order_list)?;
        let key_router = LighterOrderKeyRouter::from_config(&self.config);
        if orders.iter().any(is_contingent_order) {
            for order in &orders {
                emitter.emit_order_denied(order, "UNSUPPORTED_CONTINGENT_ORDER_LIST");
            }
            return Ok(());
        }

        for order in orders {
            if let Err(error) = validate_lighter_order(&order) {
                emitter.emit_order_denied(&order, &error.to_string());
                continue;
            }
            let price = match submission_price_from_cache(
                &order,
                self.core.cache().quote(&order.instrument_id()),
            ) {
                Ok(price) => price,
                Err(error) => {
                    emitter.emit_order_denied(&order, &error.to_string());
                    continue;
                }
            };
            let client_order_index = lighter_client_order_index(&order.client_order_id());
            let api_key_index = key_router.submit_key(&order);
            self.track_client_order(order.client_order_id(), client_order_index);
            self.track_order(order.clone());
            emitter.emit_order_submitted(&order);
            prepared_orders.push((order, client_order_index, price, api_key_index));
        }

        self.spawn_task("submit_order_list", async move {
            let registry = registry.read().await;
            let mut requests_by_key: BTreeMap<
                Option<u8>,
                Vec<(LighterSubmitOrderRequest, OrderAny)>,
            > = BTreeMap::new();

            for (order, client_order_index, price, api_key_index) in prepared_orders {
                let Some(meta) = registry.meta_for_instrument_id(&order.instrument_id()) else {
                    emitter.emit_order_rejected_event(
                        order.strategy_id(),
                        order.instrument_id(),
                        order.client_order_id(),
                        "Lighter metadata missing",
                        clock.get_time_ns(),
                        false,
                    );
                    continue;
                };

                let request = match build_submit_order_request(
                    &order,
                    meta,
                    client_order_index,
                    price,
                    api_key_index,
                ) {
                    Ok(request) => request,
                    Err(error) => {
                        emitter.emit_order_rejected_event(
                            order.strategy_id(),
                            order.instrument_id(),
                            order.client_order_id(),
                            &format!("Lighter submission failed: {error}"),
                            clock.get_time_ns(),
                            false,
                        );
                        continue;
                    }
                };
                requests_by_key
                    .entry(api_key_index)
                    .or_default()
                    .push((request, order));
            }

            for keyed_requests in requests_by_key.into_values() {
                for request_chunk in keyed_requests.chunks(max_batch_tx_count) {
                    let requests = request_chunk
                        .iter()
                        .map(|(request, _)| *request)
                        .collect::<Vec<_>>();
                    match api.submit_order_batch(requests).await {
                        Ok(response) => {
                            if response.code != 200 {
                                let reason = response
                                    .message
                                    .as_deref()
                                    .unwrap_or("Lighter batch submission failed");
                                for (_, order) in request_chunk {
                                    emitter.emit_order_rejected_event(
                                        order.strategy_id(),
                                        order.instrument_id(),
                                        order.client_order_id(),
                                        reason,
                                        clock.get_time_ns(),
                                        false,
                                    );
                                }
                            }
                        }
                        Err(error) => {
                            let reason = format!("Lighter batch submission failed: {error}");
                            for (_, order) in request_chunk {
                                emitter.emit_order_rejected_event(
                                    order.strategy_id(),
                                    order.instrument_id(),
                                    order.client_order_id(),
                                    &reason,
                                    clock.get_time_ns(),
                                    false,
                                );
                            }
                        }
                    }
                }
            }

            Ok(())
        });

        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let cmd = cmd;
        let order = self.core.get_order(&cmd.client_order_id)?;
        self.track_order(order.clone());
        let venue_order_id = self.resolve_venue_order_id(&cmd.client_order_id, cmd.venue_order_id);
        let Some(venue_order_id) = venue_order_id else {
            self.emitter.emit_order_modify_rejected_event(
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                None,
                "VENUE_ORDER_ID_REQUIRED",
                self.clock.get_time_ns(),
            );
            return Ok(());
        };

        let registry = Arc::clone(&self.registry);
        let api = Arc::clone(&self.api);
        let emitter = self.emitter.clone();
        let api_key_index = LighterOrderKeyRouter::from_config(&self.config).submit_key(&order);
        let clock = self.clock;

        self.spawn_task("modify_order", async move {
            let registry = registry.read().await;
            let result = async {
                let meta = registry
                    .meta_for_instrument_id(&cmd.instrument_id)
                    .ok_or_else(|| anyhow::anyhow!("Lighter metadata missing"))?;
                let request =
                    build_modify_order_request(&cmd, &order, meta, &venue_order_id, api_key_index)?;
                api.modify_order(request).await
            }
            .await;

            match result {
                Ok(response) if response.code == 200 => {}
                Ok(response) => {
                    emitter.emit_order_modify_rejected_event(
                        cmd.strategy_id,
                        cmd.instrument_id,
                        cmd.client_order_id,
                        Some(venue_order_id),
                        response
                            .message
                            .as_deref()
                            .unwrap_or("Lighter modify failed"),
                        clock.get_time_ns(),
                    );
                }
                Err(error) => {
                    emitter.emit_order_modify_rejected_event(
                        cmd.strategy_id,
                        cmd.instrument_id,
                        cmd.client_order_id,
                        Some(venue_order_id),
                        &format!("Lighter modify failed: {error}"),
                        clock.get_time_ns(),
                    );
                }
            }
            Ok(())
        });

        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let cmd = cmd;
        let order = self.core.get_order(&cmd.client_order_id).ok();
        if let Some(order) = &order {
            self.track_order(order.clone());
        }
        let api_key_index =
            LighterOrderKeyRouter::from_config(&self.config).cancel_key(order.as_ref());
        let venue_order_id = self.resolve_venue_order_id(&cmd.client_order_id, cmd.venue_order_id);
        let Some(venue_order_id) = venue_order_id else {
            self.emitter.emit_order_cancel_rejected_event(
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                None,
                "VENUE_ORDER_ID_REQUIRED",
                self.clock.get_time_ns(),
            );
            return Ok(());
        };

        let registry = Arc::clone(&self.registry);
        let api = Arc::clone(&self.api);
        let emitter = self.emitter.clone();
        let clock = self.clock;

        self.spawn_task("cancel_order", async move {
            let registry = registry.read().await;
            let result = async {
                let meta = registry
                    .meta_for_instrument_id(&cmd.instrument_id)
                    .ok_or_else(|| anyhow::anyhow!("Lighter metadata missing"))?;
                api.cancel_order(
                    meta.market_id as i32,
                    venue_order_id.as_str().parse()?,
                    api_key_index,
                )
                .await
            }
            .await;

            match result {
                Ok(response) if response.code == 200 => {}
                Ok(response) => {
                    emitter.emit_order_cancel_rejected_event(
                        cmd.strategy_id,
                        cmd.instrument_id,
                        cmd.client_order_id,
                        Some(venue_order_id),
                        response
                            .message
                            .as_deref()
                            .unwrap_or("Lighter cancel failed"),
                        clock.get_time_ns(),
                    );
                }
                Err(error) => {
                    emitter.emit_order_cancel_rejected_event(
                        cmd.strategy_id,
                        cmd.instrument_id,
                        cmd.client_order_id,
                        Some(venue_order_id),
                        &format!("Lighter cancel failed: {error}"),
                        clock.get_time_ns(),
                    );
                }
            }
            Ok(())
        });
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let mut cancels = Vec::new();
        for order in self.open_orders_for_cancel_all(&cmd.instrument_id, cmd.order_side) {
            let client_order_id = order.client_order_id();
            let venue_order_id =
                self.resolve_venue_order_id(&client_order_id, order.venue_order_id());
            let Some(venue_order_id) = venue_order_id else {
                self.emitter.emit_order_cancel_rejected_event(
                    order.strategy_id(),
                    order.instrument_id(),
                    client_order_id,
                    None,
                    "VENUE_ORDER_ID_REQUIRED",
                    self.clock.get_time_ns(),
                );
                continue;
            };

            cancels.push((
                CancelOrder::new(
                    cmd.trader_id,
                    cmd.client_id,
                    order.strategy_id(),
                    order.instrument_id(),
                    client_order_id,
                    Some(venue_order_id),
                    UUID4::new(),
                    cmd.ts_init,
                    None,
                ),
                venue_order_id,
                LighterOrderKeyRouter::from_config(&self.config).cancel_key(Some(&order)),
            ));
        }

        self.spawn_cancel_batch_task("cancel_all_orders", cancels);
        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        let mut cancels = Vec::new();
        for cancel in cmd.cancels {
            let venue_order_id =
                self.resolve_venue_order_id(&cancel.client_order_id, cancel.venue_order_id);
            let Some(venue_order_id) = venue_order_id else {
                self.emitter.emit_order_cancel_rejected_event(
                    cancel.strategy_id,
                    cancel.instrument_id,
                    cancel.client_order_id,
                    None,
                    "VENUE_ORDER_ID_REQUIRED",
                    self.clock.get_time_ns(),
                );
                continue;
            };
            let order = self.core.get_order(&cancel.client_order_id).ok();
            cancels.push((
                cancel,
                venue_order_id,
                LighterOrderKeyRouter::from_config(&self.config).cancel_key(order.as_ref()),
            ));
        }

        self.spawn_cancel_batch_task("batch_cancel_orders", cancels);
        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        let api = Arc::clone(&self.api);
        let registry = Arc::clone(&self.registry);
        let emitter = self.emitter.clone();
        let auth_token = Arc::clone(&self.auth_token);
        let account_index = self.config.account_index.unwrap_or_default();
        let clock = self.clock;

        self.spawn_task("query_account", async move {
            let Some((token, _)) = auth_token.lock().expect(MUTEX_POISONED).clone() else {
                return Ok(());
            };
            let response = api.request_account(account_index, &token).await?;
            if let Some(account) = response.accounts.first() {
                let balances =
                    account_balances_from_assets(account.assets.as_deref().unwrap_or(&[]));
                let registry = registry.read().await;
                let margins = margin_balances_from_positions(
                    account.positions.as_deref().unwrap_or(&[]),
                    &registry,
                );
                emitter.emit_account_state(balances, margins, true, clock.get_time_ns());
            }
            Ok(())
        });
        Ok(())
    }

    fn query_order(&self, _cmd: QueryOrder) -> anyhow::Result<()> {
        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let reports = self
            .generate_order_status_reports(&GenerateOrderStatusReports::new(
                UUID4::new(),
                cmd.ts_init,
                false,
                cmd.instrument_id,
                None,
                None,
                cmd.params.clone(),
                cmd.correlation_id,
            ))
            .await?;

        Ok(reports.into_iter().find(|report| {
            cmd.client_order_id
                .is_none_or(|id| report.client_order_id == Some(id))
                && cmd
                    .venue_order_id
                    .is_none_or(|id| report.venue_order_id == id)
        }))
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        self.ensure_instruments_initialized().await?;
        let token = self.ensure_auth_token(30).await?;
        let registry = self.registry.read().await;
        let market_ids = if let Some(instrument_id) = cmd.instrument_id {
            registry
                .meta_for_instrument_id(&instrument_id)
                .map(|meta| vec![meta.market_id])
                .unwrap_or_default()
        } else {
            vec![ALL_MARKETS_ID]
        };

        let mut reports = Vec::new();
        for market_id in market_ids {
            let market_label = registry.meta_for_market_id(market_id).map_or_else(
                || "all markets".to_string(),
                |meta| meta.instrument.id().to_string(),
            );

            let mut orders = match self
                .api
                .request_account_active_orders(
                    self.config.account_index.unwrap_or_default(),
                    market_id,
                    &token,
                )
                .await
            {
                Ok(orders) => orders.orders,
                Err(e) if is_lighter_invalid_param(&e) => {
                    log::warn!(
                        "Skipping Lighter active order reconciliation for {market_label}: {e}"
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };

            if !cmd.open_only {
                let mut cursor = None;
                loop {
                    let inactive = match self
                        .api
                        .request_account_inactive_orders(
                            self.config.account_index.unwrap_or_default(),
                            market_id,
                            &token,
                            cursor.as_deref(),
                        )
                        .await
                    {
                        Ok(inactive) => inactive,
                        Err(e) if is_lighter_invalid_param(&e) => {
                            log::warn!(
                                "Skipping Lighter inactive order reconciliation for {market_label}: {e}"
                            );
                            break;
                        }
                        Err(e) => return Err(e),
                    };
                    orders.extend(inactive.orders);
                    cursor = inactive.cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
            }

            for order in orders {
                let Some(meta) = registry.meta_for_market_id(order.market_index) else {
                    continue;
                };
                if cmd
                    .instrument_id
                    .is_some_and(|instrument_id| instrument_id != meta.instrument.id())
                {
                    continue;
                }
                let ts_init = self.clock.get_time_ns();
                let mut report = order_report_from_lighter(
                    &order,
                    self.core.account_id,
                    &meta.instrument,
                    ts_init,
                    |value| self.resolve_client_order_id(value),
                );
                if cmd.open_only && !report.order_status.is_open() {
                    continue;
                }
                if !in_time_window(report.ts_last, cmd.start, cmd.end) {
                    continue;
                }
                if let Some(client_order_id) = report.client_order_id
                    && let Some(cached_order) = self.core.cache().order(&client_order_id)
                {
                    apply_order_report_metadata(cached_order, &mut report);
                }
                reports.push(report);
            }
        }

        Ok(reports)
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        self.ensure_instruments_initialized().await?;
        let token = self.ensure_auth_token(30).await?;
        let registry = self.registry.read().await;
        let mut cursor = None;
        let mut reports = Vec::new();

        loop {
            let response = self
                .api
                .request_account_trades(
                    self.config.account_index.unwrap_or_default(),
                    &token,
                    500,
                    cursor.as_deref(),
                )
                .await?;
            if response.trades.is_empty() {
                break;
            }
            for trade in response.trades {
                let Some(meta) = registry.meta_for_market_id(trade.market_id) else {
                    continue;
                };
                if cmd
                    .instrument_id
                    .is_some_and(|instrument_id| instrument_id != meta.instrument.id())
                {
                    continue;
                }
                if let Some(report) = crate::common::fill_report_from_lighter_trade(
                    &trade,
                    self.config.account_index.unwrap_or_default(),
                    self.core.account_id,
                    &meta.instrument,
                    self.clock.get_time_ns(),
                    |value| self.resolve_client_order_id(value),
                ) && cmd
                    .venue_order_id
                    .is_none_or(|id| id == report.venue_order_id)
                    && in_time_window(report.ts_event, cmd.start, cmd.end)
                {
                    reports.push(report);
                }
            }
            cursor = response.cursor;
            if cursor.is_none() {
                break;
            }
        }

        Ok(reports)
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        self.ensure_instruments_initialized().await?;
        let token = self.ensure_auth_token(30).await?;
        let response = self
            .api
            .request_account(self.config.account_index.unwrap_or_default(), &token)
            .await?;
        let Some(account) = response.accounts.first() else {
            return Ok(Vec::new());
        };

        let registry = self.registry.read().await;
        let mut reports = position_reports_from_detailed_account(
            account,
            self.core.account_id,
            &registry,
            self.clock.get_time_ns(),
        );
        if let Some(instrument_id) = cmd.instrument_id {
            reports.retain(|report| report.instrument_id == instrument_id);
            if reports.is_empty()
                && let Some(meta) = registry.meta_for_instrument_id(&instrument_id)
            {
                let ts_init = self.clock.get_time_ns();
                reports.push(PositionStatusReport::new(
                    self.core.account_id,
                    instrument_id,
                    PositionSideSpecified::Flat,
                    Quantity::new(0.0, meta.instrument.size_precision()),
                    ts_init,
                    ts_init,
                    None,
                    None,
                    None,
                ));
            }
        }
        Ok(reports)
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let ts_init = self.clock.get_time_ns();
        let lookback_start = lookback_mins.map(|mins| {
            let lookback_nanos = mins.saturating_mul(60).saturating_mul(1_000_000_000);
            UnixNanos::from(ts_init.as_u64().saturating_sub(lookback_nanos))
        });
        let mut status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            venue(),
            ts_init,
            None,
        );

        let order_reports = if let Some(start) = lookback_start {
            let mut reports = match self
                .generate_order_status_reports(&GenerateOrderStatusReports::new(
                    UUID4::new(),
                    ts_init,
                    true,
                    None,
                    None,
                    None,
                    None,
                    None,
                ))
                .await
            {
                Ok(reports) => reports,
                Err(e) if is_lighter_invalid_param(&e) => {
                    log::warn!("Skipping Lighter open order mass-status reconciliation: {e}");
                    Vec::new()
                }
                Err(e) => return Err(e),
            };
            let mut historical_reports = match self
                .generate_order_status_reports(&GenerateOrderStatusReports::new(
                    UUID4::new(),
                    ts_init,
                    false,
                    None,
                    Some(start),
                    None,
                    None,
                    None,
                ))
                .await
            {
                Ok(reports) => reports,
                Err(e) if is_lighter_invalid_param(&e) => {
                    log::warn!("Skipping Lighter order history mass-status reconciliation: {e}");
                    Vec::new()
                }
                Err(e) => return Err(e),
            };
            reports.append(&mut historical_reports);
            reports
        } else {
            match self
                .generate_order_status_reports(&GenerateOrderStatusReports::new(
                    UUID4::new(),
                    ts_init,
                    false,
                    None,
                    None,
                    None,
                    None,
                    None,
                ))
                .await
            {
                Ok(reports) => reports,
                Err(e) if is_lighter_invalid_param(&e) => {
                    log::warn!("Skipping Lighter order report mass-status reconciliation: {e}");
                    Vec::new()
                }
                Err(e) => return Err(e),
            }
        };
        let fill_reports = match self
            .generate_fill_reports(GenerateFillReports::new(
                UUID4::new(),
                ts_init,
                None,
                None,
                lookback_start,
                None,
                None,
                None,
            ))
            .await
        {
            Ok(reports) => reports,
            Err(e) if is_lighter_invalid_param(&e) => {
                log::warn!("Skipping Lighter fill report mass-status reconciliation: {e}");
                Vec::new()
            }
            Err(e) => return Err(e),
        };
        let position_reports = self
            .generate_position_status_reports(&GeneratePositionStatusReports::new(
                UUID4::new(),
                ts_init,
                None,
                None,
                None,
                None,
                None,
            ))
            .await?;

        status.add_order_reports(order_reports);
        status.add_fill_reports(fill_reports);
        status.add_position_reports(position_reports);
        Ok(Some(status))
    }
}

fn build_submit_order_request(
    order: &OrderAny,
    meta: &LighterInstrumentMeta,
    client_order_index: i64,
    price: Price,
    api_key_index: Option<u8>,
) -> anyhow::Result<LighterSubmitOrderRequest> {
    Ok(LighterSubmitOrderRequest {
        market_index: meta.market_id as i32,
        client_order_index,
        base_amount: to_lighter_size(order.quantity().as_decimal(), meta.size_precision),
        price: to_lighter_price(price.as_decimal(), meta.price_precision) as i32,
        is_ask: order.order_side() == OrderSide::Sell,
        order_type: lighter_order_type(order.order_type())
            .ok_or_else(|| anyhow::anyhow!("UNSUPPORTED_ORDER_TYPE_{:?}", order.order_type()))?,
        time_in_force: lighter_time_in_force(order.time_in_force(), order.is_post_only())
            .ok_or_else(|| {
                anyhow::anyhow!("UNSUPPORTED_TIME_IN_FORCE_{:?}", order.time_in_force())
            })?,
        reduce_only: order.is_reduce_only(),
        trigger_price: order
            .trigger_price()
            .map(|value| to_lighter_price(value.as_decimal(), meta.price_precision) as i32)
            .unwrap_or_default(),
        order_expiry: lighter_order_expiry(order),
        api_key_index,
    })
}

fn validate_lighter_order(order: &OrderAny) -> anyhow::Result<()> {
    if lighter_order_type(order.order_type()).is_none() {
        anyhow::bail!("UNSUPPORTED_ORDER_TYPE_{:?}", order.order_type());
    }
    if lighter_time_in_force(order.time_in_force(), order.is_post_only()).is_none() {
        anyhow::bail!("UNSUPPORTED_TIME_IN_FORCE_{:?}", order.time_in_force());
    }
    Ok(())
}

fn is_contingent_order(order: &OrderAny) -> bool {
    order
        .contingency_type()
        .is_some_and(|contingency_type| contingency_type != ContingencyType::NoContingency)
        || order
            .linked_order_ids()
            .is_some_and(|linked_ids| !linked_ids.is_empty())
        || order.parent_order_id().is_some()
}

fn apply_order_report_metadata(order: &OrderAny, report: &mut OrderStatusReport) {
    report.order_list_id = order.order_list_id();
    report.parent_order_id = order.parent_order_id();
    report.contingency_type = order.contingency_type().unwrap_or_default();
    report.linked_order_ids = order
        .linked_order_ids()
        .map(|linked_ids| linked_ids.to_vec());
}

fn submission_price_from_cache(
    order: &OrderAny,
    quote: Option<&nautilus_model::data::QuoteTick>,
) -> anyhow::Result<Price> {
    match order.price() {
        Some(price) => Ok(price),
        None if matches!(
            order.order_type(),
            OrderType::Market | OrderType::StopMarket | OrderType::MarketIfTouched
        ) => quote
            .map(|quote| market_order_price_bound(order.order_side(), quote))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "No cached quote for {}: subscribe to Lighter quotes before submitting market orders",
                    order.instrument_id()
                )
            }),
        None => Err(anyhow::anyhow!("Lighter order price unavailable")),
    }
}

fn market_order_price_bound(side: OrderSide, quote: &nautilus_model::data::QuoteTick) -> Price {
    let (reference, multiplier) = if side == OrderSide::Buy {
        (quote.ask_price, LIGHTER_MARKET_ORDER_BUY_PRICE_BUFFER)
    } else {
        (quote.bid_price, LIGHTER_MARKET_ORDER_SELL_PRICE_BUFFER)
    };

    Price::new(reference.as_f64() * multiplier, reference.precision)
}

fn build_modify_order_request(
    cmd: &ModifyOrder,
    order: &OrderAny,
    meta: &LighterInstrumentMeta,
    venue_order_id: &VenueOrderId,
    api_key_index: Option<u8>,
) -> anyhow::Result<LighterModifyOrderRequest> {
    let price = cmd
        .price
        .or_else(|| order.price())
        .ok_or_else(|| anyhow::anyhow!("PRICE_REQUIRED"))?;
    let trigger_price = cmd
        .trigger_price
        .or_else(|| order.trigger_price())
        .map(|price| to_lighter_price(price.as_decimal(), meta.price_precision))
        .unwrap_or_default();

    Ok(LighterModifyOrderRequest {
        market_index: meta.market_id as i32,
        order_index: venue_order_id.as_str().parse()?,
        base_amount: to_lighter_size(
            cmd.quantity.unwrap_or(order.leaves_qty()).as_decimal(),
            meta.size_precision,
        ),
        price: to_lighter_price(price.as_decimal(), meta.price_precision),
        trigger_price,
        api_key_index,
    })
}

fn lighter_order_type(order_type: OrderType) -> Option<i32> {
    match order_type {
        OrderType::Limit => Some(0),
        OrderType::Market => Some(1),
        OrderType::StopMarket => Some(2),
        OrderType::StopLimit => Some(3),
        OrderType::MarketIfTouched => Some(4),
        OrderType::LimitIfTouched => Some(5),
        _ => None,
    }
}

fn lighter_time_in_force(time_in_force: TimeInForce, post_only: bool) -> Option<i32> {
    if post_only {
        return Some(2);
    }

    match time_in_force {
        TimeInForce::Ioc => Some(0),
        TimeInForce::Gtc | TimeInForce::Gtd => Some(1),
        _ => None,
    }
}

fn lighter_order_expiry(order: &OrderAny) -> i64 {
    if matches!(order.time_in_force(), TimeInForce::Ioc) {
        0
    } else if let Some(expire_time) = order.expire_time() {
        (expire_time.as_u64() / 1_000_000) as i64
    } else {
        unix_now_ms(30 * 24 * 60 * 60)
    }
}

fn is_lighter_invalid_param(error: &anyhow::Error) -> bool {
    error.downcast_ref::<SdkError>().is_some_and(|error| {
        matches!(
            error,
            SdkError::Api {
                code: 20001,
                message: _
            }
        )
    })
}

fn unix_now_ms(offset_secs: u64) -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    now + (offset_secs as i64 * 1_000)
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::QuoteTick,
        enums::OrderSide,
        identifiers::InstrumentId,
        types::{Price, Quantity},
    };
    use serde_json::json;

    use super::{
        market_order_price_bound, normalize_send_tx_response, tx_response_matches_request,
    };

    fn quote_tick() -> QuoteTick {
        QuoteTick::new(
            InstrumentId::from("BTC-PERP.LIGHTER"),
            Price::from("100.00"),
            Price::from("101.00"),
            Quantity::from("1.0000"),
            Quantity::from("1.0000"),
            UnixNanos::default(),
            UnixNanos::default(),
        )
    }

    #[test]
    fn tx_response_matching_ignores_stale_response_ids() {
        let stale = json!({
            "type": "jsonapi/sendtx",
            "data": {
                "id": "old",
                "tx_hash": "old",
            },
        });
        let payload = stale["data"].clone();

        assert!(!tx_response_matches_request(&stale, &payload, "new"));
    }

    #[test]
    fn tx_response_matching_accepts_matching_tx_hash_without_id() {
        let response = json!({
            "data": {
                "tx_hash": "0xabc",
            },
        });
        let payload = response["data"].clone();

        assert!(tx_response_matches_request(&response, &payload, "0xabc"));
        assert_eq!(normalize_send_tx_response(payload).unwrap()["code"], 200);
    }

    #[test]
    fn tx_response_matching_accepts_batch_tx_hash_member() {
        let response = json!({
            "data": {
                "tx_hash": ["0xabc", "0xdef"],
            },
        });
        let payload = response["data"].clone();

        assert!(tx_response_matches_request(&response, &payload, "0xabc"));
    }

    #[test]
    fn market_order_price_bound_crosses_top_of_book() {
        let quote = quote_tick();

        let buy = market_order_price_bound(OrderSide::Buy, &quote);
        let sell = market_order_price_bound(OrderSide::Sell, &quote);

        assert_eq!(buy, Price::from("102.01"));
        assert_eq!(sell, Price::from("99.00"));
    }
}
