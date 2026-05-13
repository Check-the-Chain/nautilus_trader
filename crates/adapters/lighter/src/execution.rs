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
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
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
        AccountType, ContingencyType, OmsType, OrderSide, OrderStatus, OrderType, TimeInForce,
    },
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, Venue, VenueOrderId},
    instruments::Instrument,
    orders::{Order, any::OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance, Price},
};
use tokio::task::JoinHandle;

use crate::{
    client::{LighterCancelOrderRequest, LighterModifyOrderRequest, LighterSubmitOrderRequest},
    common::{
        LighterInstrumentMeta, LighterInstrumentRegistry, account_balances_from_assets,
        lighter_client_order_index, load_instrument_registry, margin_balances_from_positions,
        order_report_from_lighter, position_reports_from_detailed_account, to_lighter_price,
        to_lighter_size, venue,
    },
    config::{Config, LighterExecClientConfig},
    http::client::LighterHttpClient,
    models::{
        account::DetailedAccounts,
        order::Orders,
        trade::Trades,
        transaction::{RespSendTx, RespSendTxBatch},
        ws::{WsAccountAllOrdersUpdate, WsAccountAllTradesUpdate, WsMessage},
    },
    websocket::client::LighterWebSocketClient,
};

const LIGHTER_MAX_BATCH_TX_COUNT: usize = 50;

#[async_trait]
pub trait LighterExecutionApi: std::fmt::Debug + Send + Sync {
    async fn create_auth_token(
        &self,
        deadline_secs: i64,
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
        deadline_secs: i64,
        api_key_index: Option<u8>,
    ) -> anyhow::Result<String> {
        self.client
            .create_auth_token(deadline_secs, api_key_index)
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
        let api: Arc<dyn LighterExecutionApi> = Arc::new(LighterHttpExecutionApi {
            client: private_client,
        });

        Self::new_with_api(core, config, public_http_client, api)
    }

    pub fn new_with_api(
        core: ExecutionClientCore,
        config: LighterExecClientConfig,
        public_http_client: LighterHttpClient,
        api: Arc<dyn LighterExecutionApi>,
    ) -> anyhow::Result<Self> {
        let ws_client = LighterWebSocketClient::new(config.ws_url(), None);
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

    async fn ensure_auth_token(&self, min_ttl_secs: u64) -> anyhow::Result<String> {
        if let Some((token, expires_at)) = self.auth_token.lock().expect(MUTEX_POISONED).clone()
            && expires_at > Instant::now() + Duration::from_secs(min_ttl_secs)
        {
            return Ok(token);
        }

        let ttl = self.config.default_auth_token_ttl_secs.max(min_ttl_secs);
        let token = self
            .api
            .create_auth_token(ttl as i64, self.config.api_key_index)
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

        let balances = account_balances_from_assets(account.assets.as_deref().unwrap_or(&[]));
        let registry = self.registry.read().await;
        let margins =
            margin_balances_from_positions(account.positions.as_deref().unwrap_or(&[]), &registry);
        self.generate_account_state(balances, margins, true, self.clock.get_time_ns())
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

    fn spawn_cancel_batch_task(
        &self,
        name: &'static str,
        cancels: Vec<(CancelOrder, VenueOrderId)>,
    ) {
        if cancels.is_empty() {
            return;
        }

        let registry = Arc::clone(&self.registry);
        let api = Arc::clone(&self.api);
        let emitter = self.emitter.clone();
        let config = self.config.clone();
        let clock = self.clock;

        self.spawn_task(name, async move {
            let registry = registry.read().await;
            let mut valid_cancels = Vec::new();
            let mut requests = Vec::new();

            for (cancel, venue_order_id) in cancels {
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

                requests.push(LighterCancelOrderRequest {
                    market_index: meta.market_id as i32,
                    order_index,
                    api_key_index: config.api_key_index,
                });
                valid_cancels.push((cancel, venue_order_id));
            }

            for (request_chunk, cancel_chunk) in requests
                .chunks(LIGHTER_MAX_BATCH_TX_COUNT)
                .zip(valid_cancels.chunks(LIGHTER_MAX_BATCH_TX_COUNT))
            {
                let response = api.cancel_order_batch(request_chunk.to_vec()).await?;
                if response.code != 200 {
                    let reason = response
                        .message
                        .as_deref()
                        .unwrap_or("Lighter batch cancel failed");
                    for (cancel, venue_order_id) in cancel_chunk {
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
                                    let report = order_report_from_lighter(
                                        &order,
                                        account_id,
                                        &meta.instrument,
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
                                    if let Some(report) =
                                        crate::common::fill_report_from_lighter_trade(
                                            &trade,
                                            account_index,
                                            account_id,
                                            &meta.instrument,
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
                                            &processed_trade_ids,
                                            &processed_trade_queue,
                                            report,
                                        );
                                    }
                                }
                            }
                        }
                    }
                    "update/account_all"
                    | "subscribed/account_all"
                    | "update/account_all_assets"
                    | "update/account_all_positions"
                    | "subscribed/user_stats"
                    | "update/user_stats" => {
                        let Some((token, _)) = auth_token.lock().expect(MUTEX_POISONED).clone()
                        else {
                            continue;
                        };
                        if let Ok(response) = api.request_account(account_index, &token).await
                            && let Some(account) = response.accounts.first()
                        {
                            let balances = account_balances_from_assets(
                                account.assets.as_deref().unwrap_or(&[]),
                            );
                            let registry = registry.read().await;
                            let margins = margin_balances_from_positions(
                                account.positions.as_deref().unwrap_or(&[]),
                                &registry,
                            );
                            emitter.emit_account_state(
                                balances,
                                margins,
                                true,
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
        for task in self.pending_tasks.lock().expect(MUTEX_POISONED).drain(..) {
            task.abort();
        }
        self.core.set_disconnected();
        self.core.set_stopped();
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.ensure_instruments_initialized().await?;
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
        self.core.set_disconnected();
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let cmd = cmd;
        let order = self.core.get_order(&cmd.client_order_id)?;
        let registry = Arc::clone(&self.registry);
        let api = Arc::clone(&self.api);
        let config = self.config.clone();
        let public_http = self.public_http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let client_order_index = lighter_client_order_index(&cmd.client_order_id);

        self.track_client_order(cmd.client_order_id, client_order_index);
        self.track_order(order.clone());
        emitter.emit_order_submitted(&order);

        self.spawn_task("submit_order", async move {
            let registry = registry.read().await;
            let meta = registry
                .meta_for_instrument_id(&order.instrument_id())
                .ok_or_else(|| anyhow::anyhow!("Lighter metadata missing"))?;
            let request = build_submit_order_request(
                &public_http,
                &order,
                meta,
                client_order_index,
                config.api_key_index,
            )
            .await?;
            let response = api.submit_order(request).await?;

            if response.code != 200 {
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
            Ok(())
        });

        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        let registry = Arc::clone(&self.registry);
        let api = Arc::clone(&self.api);
        let config = self.config.clone();
        let public_http = self.public_http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let mut prepared_orders = Vec::new();
        let orders = self.core.get_orders_for_list(&cmd.order_list)?;
        if orders.iter().any(is_contingent_order) {
            for order in &orders {
                emitter.emit_order_denied(order, "UNSUPPORTED_CONTINGENT_ORDER_LIST");
            }
            return Ok(());
        }

        for order in orders {
            let client_order_index = lighter_client_order_index(&order.client_order_id());
            self.track_client_order(order.client_order_id(), client_order_index);
            self.track_order(order.clone());
            emitter.emit_order_submitted(&order);
            prepared_orders.push((order, client_order_index));
        }

        self.spawn_task("submit_order_list", async move {
            let registry = registry.read().await;
            let mut valid_orders = Vec::new();
            let mut requests = Vec::new();

            for (order, client_order_index) in prepared_orders {
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

                match build_submit_order_request(
                    &public_http,
                    &order,
                    meta,
                    client_order_index,
                    config.api_key_index,
                )
                .await
                {
                    Ok(request) => {
                        valid_orders.push(order);
                        requests.push(request);
                    }
                    Err(error) => {
                        emitter.emit_order_rejected_event(
                            order.strategy_id(),
                            order.instrument_id(),
                            order.client_order_id(),
                            &error.to_string(),
                            clock.get_time_ns(),
                            false,
                        );
                    }
                }
            }

            for (request_chunk, order_chunk) in requests
                .chunks(LIGHTER_MAX_BATCH_TX_COUNT)
                .zip(valid_orders.chunks(LIGHTER_MAX_BATCH_TX_COUNT))
            {
                let response = api.submit_order_batch(request_chunk.to_vec()).await?;
                if response.code != 200 {
                    let reason = response
                        .message
                        .as_deref()
                        .unwrap_or("Lighter batch submission failed");
                    for order in order_chunk {
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
        let config = self.config.clone();
        let clock = self.clock;

        self.spawn_task("modify_order", async move {
            let registry = registry.read().await;
            let meta = registry
                .meta_for_instrument_id(&cmd.instrument_id)
                .ok_or_else(|| anyhow::anyhow!("Lighter metadata missing"))?;
            let request = build_modify_order_request(
                &cmd,
                &order,
                meta,
                &venue_order_id,
                config.api_key_index,
            )?;
            let response = api.modify_order(request).await?;

            if response.code != 200 {
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
            Ok(())
        });

        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let cmd = cmd;
        if let Ok(order) = self.core.get_order(&cmd.client_order_id) {
            self.track_order(order);
        }
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
        let config = self.config.clone();
        let clock = self.clock;

        self.spawn_task("cancel_order", async move {
            let registry = registry.read().await;
            let meta = registry
                .meta_for_instrument_id(&cmd.instrument_id)
                .ok_or_else(|| anyhow::anyhow!("Lighter metadata missing"))?;
            let response = api
                .cancel_order(
                    meta.market_id as i32,
                    venue_order_id.as_str().parse()?,
                    config.api_key_index,
                )
                .await?;

            if response.code != 200 {
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
            cancels.push((cancel, venue_order_id));
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
            registry.market_ids()
        };

        let mut reports = Vec::new();
        for market_id in market_ids {
            let Some(meta) = registry.meta_for_market_id(market_id) else {
                continue;
            };

            let mut orders = self
                .api
                .request_account_active_orders(
                    self.config.account_index.unwrap_or_default(),
                    market_id,
                    &token,
                )
                .await?
                .orders;

            if !cmd.open_only {
                let mut cursor = None;
                loop {
                    let inactive = self
                        .api
                        .request_account_inactive_orders(
                            self.config.account_index.unwrap_or_default(),
                            market_id,
                            &token,
                            cursor.as_deref(),
                        )
                        .await?;
                    orders.extend(inactive.orders);
                    cursor = inactive.cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
            }

            for order in orders {
                let mut report = order_report_from_lighter(
                    &order,
                    self.core.account_id,
                    &meta.instrument,
                    |value| self.resolve_client_order_id(value),
                );
                if cmd.open_only && !report.order_status.is_open() {
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
                    |value| self.resolve_client_order_id(value),
                ) && cmd
                    .venue_order_id
                    .is_none_or(|id| id == report.venue_order_id)
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
        }
        Ok(reports)
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let ts_init = self.clock.get_time_ns();
        let mut status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            venue(),
            ts_init,
            None,
        );

        let order_reports = self
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
            .await?;
        let mut fill_reports = self
            .generate_fill_reports(GenerateFillReports::new(
                UUID4::new(),
                ts_init,
                None,
                None,
                None,
                None,
                None,
                None,
            ))
            .await?;
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

        if let Some(lookback_mins) = lookback_mins {
            let cutoff = UnixNanos::from(
                ts_init
                    .as_u64()
                    .saturating_sub(lookback_mins * 60 * 1_000_000_000),
            );
            fill_reports.retain(|report| report.ts_event >= cutoff);
        }

        status.add_order_reports(order_reports);
        status.add_fill_reports(fill_reports);
        status.add_position_reports(position_reports);
        Ok(Some(status))
    }
}

async fn build_submit_order_request(
    public_http: &LighterHttpClient,
    order: &OrderAny,
    meta: &LighterInstrumentMeta,
    client_order_index: i64,
    api_key_index: Option<u8>,
) -> anyhow::Result<LighterSubmitOrderRequest> {
    let price = submission_price(public_http, order, meta).await?;
    Ok(LighterSubmitOrderRequest {
        market_index: meta.market_id as i32,
        client_order_index,
        base_amount: to_lighter_size(order.quantity().as_decimal(), meta.size_precision),
        price: to_lighter_price(price.as_decimal(), meta.price_precision) as i32,
        is_ask: order.order_side() == OrderSide::Sell,
        order_type: lighter_order_type(order.order_type()),
        time_in_force: lighter_time_in_force(order.time_in_force(), order.is_post_only()),
        reduce_only: order.is_reduce_only(),
        trigger_price: order
            .trigger_price()
            .map(|value| to_lighter_price(value.as_decimal(), meta.price_precision) as i32)
            .unwrap_or_default(),
        order_expiry: lighter_order_expiry(order),
        api_key_index,
    })
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

async fn submission_price(
    public_http: &LighterHttpClient,
    order: &OrderAny,
    meta: &LighterInstrumentMeta,
) -> anyhow::Result<Price> {
    match order.price() {
        Some(price) => Ok(price),
        None if matches!(
            order.order_type(),
            OrderType::Market | OrderType::StopMarket | OrderType::MarketIfTouched
        ) =>
        {
            let snapshot = public_http
                .rest()
                .get_order_book_orders(meta.market_id, 1)
                .await?;
            let raw = if order.order_side() == OrderSide::Buy {
                snapshot.asks.first().map(|item| item.price.clone())
            } else {
                snapshot.bids.first().map(|item| item.price.clone())
            }
            .ok_or_else(|| anyhow::anyhow!("Lighter order book liquidity unavailable"))?;
            Ok(Price::from(raw.as_str()))
        }
        None => Err(anyhow::anyhow!("Lighter order price unavailable")),
    }
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

fn lighter_order_type(order_type: OrderType) -> i32 {
    match order_type {
        OrderType::Limit => 0,
        OrderType::Market => 1,
        OrderType::StopMarket => 2,
        OrderType::StopLimit => 3,
        OrderType::MarketIfTouched => 4,
        OrderType::LimitIfTouched => 5,
        _ => 0,
    }
}

fn lighter_time_in_force(time_in_force: TimeInForce, post_only: bool) -> i32 {
    if post_only {
        2
    } else {
        i32::from(!matches!(
            time_in_force,
            TimeInForce::Ioc | TimeInForce::Fok
        ))
    }
}

fn lighter_order_expiry(order: &OrderAny) -> i64 {
    if matches!(order.time_in_force(), TimeInForce::Ioc | TimeInForce::Fok) {
        0
    } else if let Some(expire_time) = order.expire_time() {
        (expire_time.as_u64() / 1_000_000) as i64
    } else {
        unix_now_ms(30 * 24 * 60 * 60)
    }
}

fn unix_now_ms(offset_secs: u64) -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    now + (offset_secs as i64 * 1_000)
}
