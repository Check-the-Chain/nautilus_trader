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

//! Live execution client implementation for the Alpaca adapter.
//!
//! Order commands go out over the Trading API REST endpoints; order lifecycle
//! events come back over the trade-updates WebSocket stream and are emitted as
//! [`OrderStatusReport`]s (with [`FillReport`]s attached for fills), which the
//! live execution engine reconciles against local order state.

use std::{future::Future, str::FromStr, sync::Mutex};

use anyhow::Context;
use async_trait::async_trait;
use nautilus_common::{
    clients::ExecutionClient,
    live::{get_runtime, runner::get_exec_event_sender},
    messages::execution::{
        CancelAllOrders, CancelOrder, GenerateFillReports, GenerateOrderStatusReport,
        GenerateOrderStatusReports, GenerateOrderStatusReportsBuilder,
        GeneratePositionStatusReports, GeneratePositionStatusReportsBuilder, ModifyOrder,
        QueryAccount, SubmitOrder, SubmitOrderList,
    },
};
use nautilus_core::{
    MUTEX_POISONED, UnixNanos,
    datetime::NANOSECONDS_IN_SECOND,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{
        AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType,
        PositionSideSpecified, TimeInForce, TrailingOffsetType,
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, Symbol, TradeId, Venue, VenueOrderId,
    },
    instruments::Instrument,
    orders::{Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use tokio::task::JoinHandle;
use ustr::Ustr;

use crate::{
    common::{
        consts::ALPACA_VENUE,
        credential::Credential,
        enums::{AlpacaOrderSide, AlpacaOrderType, AlpacaTimeInForce, AlpacaTradeUpdateEvent},
        parse::{parse_price, parse_quantity, parse_rfc3339_timestamp},
    },
    config::AlpacaExecClientConfig,
    http::{
        client::AlpacaHttpClient,
        models::{AlpacaAccount, AlpacaPosition},
        query::{GetOrdersParamsBuilder, PatchOrderParamsBuilder, PostOrderParamsBuilder},
    },
    websocket::{
        client::AlpacaTradeUpdatesWebSocketClient,
        messages::{AlpacaTradeUpdateMsg, AlpacaWsOrder, NautilusWsMessage},
    },
};

/// Alpaca live execution client.
#[derive(Debug)]
pub struct AlpacaExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: AlpacaExecClientConfig,
    emitter: ExecutionEventEmitter,
    http_client: AlpacaHttpClient,
    ws_client: AlpacaTradeUpdatesWebSocketClient,
    ws_stream_handle: Option<JoinHandle<()>>,
    pending_tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl AlpacaExecutionClient {
    /// Creates a new [`AlpacaExecutionClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if credentials cannot be resolved or the HTTP client
    /// fails to initialize.
    pub fn new(core: ExecutionClientCore, config: AlpacaExecClientConfig) -> anyhow::Result<Self> {
        let credential = Credential::resolve(
            config.api_key.clone(),
            config.api_secret.clone(),
            config.environment,
        )?
        .context("missing Alpaca credentials for execution client")?;

        let http_client = AlpacaHttpClient::with_credentials(
            credential.api_key().to_string(),
            credential.api_secret()?.to_string(),
            config.environment,
            config.base_url_http.clone(),
            None,
            config.http_timeout_secs,
            config.proxy_url.clone(),
        )?;

        let ws_client = AlpacaTradeUpdatesWebSocketClient::new(
            Some(config.ws_url()),
            config.environment,
            credential,
            config.transport_backend,
            config.proxy_url.clone(),
        );

        let clock = get_atomic_clock_realtime();
        // Alpaca accounts are margin accounts (multiplier 1x/2x/4x) in USD.
        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            AccountType::Margin,
            Some(Currency::USD()),
        );

        Ok(Self {
            core,
            clock,
            config,
            emitter,
            http_client,
            ws_client,
            ws_stream_handle: None,
            pending_tasks: Mutex::new(Vec::new()),
        })
    }

    /// Spawns an async task for execution operations.
    fn spawn_task<F>(&self, description: &'static str, fut: F)
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let runtime = get_runtime();
        let handle = runtime.spawn(async move {
            if let Err(e) = fut.await {
                log::warn!("{description} failed: {e:?}");
            }
        });

        let mut tasks = self.pending_tasks.lock().expect(MUTEX_POISONED);
        tasks.retain(|handle| !handle.is_finished());
        tasks.push(handle);
    }

    /// Aborts all pending async tasks.
    fn abort_pending_tasks(&self) {
        let mut tasks = self.pending_tasks.lock().expect(MUTEX_POISONED);
        for handle in tasks.drain(..) {
            handle.abort();
        }
    }

    /// Submits a single order to Alpaca.
    ///
    /// This is the core submission logic shared by `submit_order` and
    /// `submit_order_list`.
    fn submit_single_order(&self, order: &OrderAny, task_name: &'static str) {
        if order.is_closed() {
            log::warn!("Cannot submit closed order {}", order.client_order_id());
            return;
        }

        let params = match build_post_order_params(order) {
            Ok(params) => params,
            Err(e) => {
                let ts_event = self.clock.get_time_ns();
                self.emitter.emit_order_rejected_event(
                    order.strategy_id(),
                    order.instrument_id(),
                    order.client_order_id(),
                    &format!("{e}"),
                    ts_event,
                    false,
                );
                return;
            }
        };
        let client_order_id = order.client_order_id();

        log::debug!("OrderSubmitted client_order_id={client_order_id}");
        self.emitter.emit_order_submitted(order);

        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let strategy_id = order.strategy_id();
        let instrument_id = order.instrument_id();

        self.spawn_task(task_name, async move {
            if let Err(e) = http_client.submit_order(&params).await {
                log::error!(
                    "Submit order request failed: task={task_name}, client_order_id={client_order_id}, error={e}"
                );
                let ts_event = clock.get_time_ns();
                emitter.emit_order_rejected_event(
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    &format!("{e}"),
                    ts_event,
                    false,
                );
            }
            Ok(())
        });
    }

    /// Spawns the trade-updates stream consumer.
    fn spawn_stream_handler(
        &mut self,
        mut receiver: tokio::sync::mpsc::UnboundedReceiver<NautilusWsMessage>,
    ) {
        if self.ws_stream_handle.is_some() {
            return;
        }

        let emitter = self.emitter.clone();
        let http_client = self.http_client.clone();
        let account_id = self.core.account_id;
        let clock = self.clock;

        let handle = get_runtime().spawn(async move {
            while let Some(message) = receiver.recv().await {
                dispatch_ws_message(message, &emitter, &http_client, account_id, clock);
            }
            log::debug!("Alpaca trade-updates consumer finished");
        });

        self.ws_stream_handle = Some(handle);
        log::info!("Trade-updates stream handler started");
    }

    /// Fetches the account and emits an account state event.
    async fn refresh_account_state(&self) -> anyhow::Result<()> {
        let account = self
            .http_client
            .get_account()
            .await
            .context("failed to query Alpaca account (check API credentials are valid)")?;

        let balances = parse_account_balances(&account)?;
        let ts_event = self.clock.get_time_ns();
        self.emitter
            .emit_account_state(balances, vec![], true, ts_event);
        Ok(())
    }
}

#[async_trait(?Send)]
impl ExecutionClient for AlpacaExecutionClient {
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
        *ALPACA_VENUE
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
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

        let sender = get_exec_event_sender();
        self.emitter.set_sender(sender);
        self.core.set_started();

        log::info!(
            "Started: client_id={}, account_id={}, environment={}",
            self.core.client_id,
            self.core.account_id,
            self.config.environment,
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }

        self.core.set_stopped();
        self.core.set_disconnected();
        self.abort_pending_tasks();
        log::info!("Stopped: client_id={}", self.core.client_id);
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected() {
            return Ok(());
        }

        if !self.config.has_credentials() {
            anyhow::bail!("Missing API credentials; set Alpaca environment variables");
        }

        if !self.core.instruments_initialized() {
            let instruments = self
                .http_client
                .request_instruments()
                .await
                .context("failed to request Alpaca instruments")?;
            log::info!("Loaded {} Alpaca instruments", instruments.len());
            self.core.set_instruments_initialized();
        }

        self.refresh_account_state().await?;

        self.ws_client
            .connect()
            .await
            .context("failed to connect Alpaca trade-updates stream")?;

        let receiver = self
            .ws_client
            .take_receiver()
            .context("trade-updates receiver already taken")?;
        self.spawn_stream_handler(receiver);

        self.core.set_connected();
        log::info!("Connected: client_id={}", self.core.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if self.core.is_disconnected() {
            return Ok(());
        }

        self.abort_pending_tasks();

        if let Some(handle) = self.ws_stream_handle.take() {
            handle.abort();
        }

        if let Err(e) = self.ws_client.disconnect().await {
            log::warn!("Error closing trade-updates stream: {e}");
        }

        self.core.set_disconnected();
        log::info!("Disconnected: client_id={}", self.core.client_id);
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        self.submit_single_order(&order, "submit_order");
        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        if cmd.order_list.client_order_ids.is_empty() {
            log::debug!("submit_order_list called with empty order list");
            return Ok(());
        }

        let orders = self.core.get_orders_for_list(&cmd.order_list)?;

        // Alpaca has no batch submission endpoint; submit sequentially.
        for order in &orders {
            self.submit_single_order(order, "submit_order_list_item");
        }

        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let order_id = match cmd.venue_order_id.as_ref() {
            Some(venue_order_id) => venue_order_id.to_string(),
            None => {
                return reject_modify_command(
                    &self.emitter,
                    self.clock,
                    &cmd,
                    "venue_order_id required for modify_order",
                );
            }
        };

        let mut builder = PatchOrderParamsBuilder::default();
        if let Some(quantity) = cmd.quantity {
            if quantity.precision > 0 {
                return reject_modify_command(
                    &self.emitter,
                    self.clock,
                    &cmd,
                    "Alpaca order replacement supports whole-share quantities only",
                );
            }
            builder.qty(quantity.to_string());
        }
        if let Some(price) = cmd.price {
            builder.limit_price(price.to_string());
        }
        if let Some(trigger_price) = cmd.trigger_price {
            builder.stop_price(trigger_price.to_string());
        }
        let params = builder
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build patch order params: {e}"))?;

        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let client_order_id = cmd.client_order_id;
        let strategy_id = cmd.strategy_id;
        let instrument_id = cmd.instrument_id;
        let venue_order_id = cmd.venue_order_id;

        log::info!("Modifying order: order_id={order_id}, client_order_id={client_order_id}");

        self.spawn_task("modify_order", async move {
            if let Err(e) = http_client.patch_order(&order_id, &params).await {
                log::error!(
                    "Modify order failed: order_id={order_id}, client_order_id={client_order_id}, error={e}"
                );
                let ts_event = clock.get_time_ns();
                emitter.emit_order_modify_rejected_event(
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    venue_order_id,
                    &format!("{e}"),
                    ts_event,
                );
            }
            Ok(())
        });

        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let order_id = match cmd.venue_order_id.as_ref() {
            Some(venue_order_id) => venue_order_id.to_string(),
            None => {
                log::warn!(
                    "Cannot cancel order {} - no venue_order_id",
                    cmd.client_order_id
                );
                return Ok(());
            }
        };

        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let client_order_id = cmd.client_order_id;
        let strategy_id = cmd.strategy_id;
        let instrument_id = cmd.instrument_id;
        let venue_order_id = cmd.venue_order_id;

        log::info!("Canceling order: order_id={order_id}, client_order_id={client_order_id}");

        self.spawn_task("cancel_order", async move {
            if let Err(e) = http_client.cancel_order(&order_id).await {
                log::error!(
                    "Cancel order failed: order_id={order_id}, client_order_id={client_order_id}, error={e}"
                );
                let ts_event = clock.get_time_ns();
                emitter.emit_order_cancel_rejected_event(
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    venue_order_id,
                    &format!("{e}"),
                    ts_event,
                );
            }
            Ok(())
        });

        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;

        // Alpaca's DELETE /v2/orders cancels every open order account-wide,
        // which is broader than this per-instrument command, so open orders
        // are filtered from the cache and canceled individually.
        let orders_to_cancel: Vec<(String, ClientOrderId)> = {
            let cache = self.core.cache();
            let open_orders = cache.orders_open(None, Some(&instrument_id), None, None, None);

            open_orders
                .into_iter()
                .filter(|order| {
                    cmd.order_side == OrderSide::NoOrderSide || order.order_side() == cmd.order_side
                })
                .filter_map(|order| {
                    let venue_order_id = order.venue_order_id()?;
                    Some((venue_order_id.to_string(), order.client_order_id()))
                })
                .collect()
        };

        if orders_to_cancel.is_empty() {
            log::debug!(
                "No open {} orders to cancel for {instrument_id}",
                cmd.order_side,
            );
            return Ok(());
        }

        log::info!(
            "Canceling {} orders for {instrument_id}",
            orders_to_cancel.len(),
        );

        for (order_id, client_order_id) in orders_to_cancel {
            let http_client = self.http_client.clone();
            self.spawn_task("cancel_all_orders_item", async move {
                if let Err(e) = http_client.cancel_order(&order_id).await {
                    log::error!(
                        "Cancel order failed: order_id={order_id}, client_order_id={client_order_id}, error={e}"
                    );
                }
                Ok(())
            });
        }

        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;

        self.spawn_task("query_account", async move {
            let account = http_client
                .get_account()
                .await
                .context("failed to query account state (check API credentials are valid)")?;
            let balances = parse_account_balances(&account)?;
            emitter.emit_account_state(balances, vec![], true, clock.get_time_ns());
            Ok(())
        });

        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let ts_init = self.clock.get_time_ns();

        if let Some(venue_order_id) = &cmd.venue_order_id {
            let order = self.http_client.get_order(venue_order_id.as_str()).await?;
            let report = order_report_with_precisions(
                &self.http_client,
                &order,
                self.core.account_id,
                ts_init,
            );
            return Ok(report);
        }

        // Search open then all orders for the client order ID.
        if let Some(client_order_id) = &cmd.client_order_id {
            let params = GetOrdersParamsBuilder::default()
                .status("all")
                .limit(500u32)
                .build()
                .map_err(|e| anyhow::anyhow!("failed to build orders params: {e}"))?;
            let orders = self.http_client.get_orders(&params).await?;

            for order in &orders {
                if order.client_order_id.as_deref() == Some(client_order_id.as_str()) {
                    return Ok(order_report_with_precisions(
                        &self.http_client,
                        order,
                        self.core.account_id,
                        ts_init,
                    ));
                }
            }
        }

        Ok(None)
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let ts_init = self.clock.get_time_ns();
        let status = if cmd.open_only { "open" } else { "all" };

        let mut builder = GetOrdersParamsBuilder::default();
        builder.status(status).limit(500u32);
        if let Some(instrument_id) = cmd.instrument_id {
            builder.symbols(instrument_id.symbol.to_string());
        }
        if let Some(start) = cmd.start {
            builder.after(start.to_rfc3339());
        }
        if let Some(end) = cmd.end {
            builder.until(end.to_rfc3339());
        }
        let params = builder
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build orders params: {e}"))?;

        let orders = self.http_client.get_orders(&params).await?;
        let mut reports = Vec::with_capacity(orders.len());
        for order in &orders {
            if let Some(report) = order_report_with_precisions(
                &self.http_client,
                order,
                self.core.account_id,
                ts_init,
            ) {
                reports.push(report);
            }
        }

        Ok(reports)
    }

    /// Alpaca REST does not expose per-execution fills (they are delivered on
    /// the trade-updates stream), so this returns an empty list.
    async fn generate_fill_reports(
        &self,
        _cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        log::warn!(
            "Alpaca REST has no fills endpoint; live fills arrive via the trade-updates stream"
        );
        Ok(Vec::new())
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let ts_init = self.clock.get_time_ns();
        let positions = self.http_client.get_positions().await?;

        let mut reports = Vec::with_capacity(positions.len());
        for position in &positions {
            if let Some(instrument_id) = cmd.instrument_id
                && instrument_id.symbol.inner() != position.symbol
            {
                continue;
            }

            let size_precision = self
                .http_client
                .get_instrument(&position.symbol)
                .map_or(0, |instrument| instrument.size_precision());

            match parse_position_status_report(
                position,
                self.core.account_id,
                size_precision,
                ts_init,
            ) {
                Ok(report) => reports.push(report),
                Err(e) => log::warn!(
                    "Skipping unparseable position for {symbol}: {e}",
                    symbol = position.symbol,
                ),
            }
        }

        Ok(reports)
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        log::info!("Generating ExecutionMassStatus (lookback_mins={lookback_mins:?})");
        let ts_now = self.clock.get_time_ns();
        let start = lookback_mins.map(|mins| {
            let lookback_ns = mins
                .saturating_mul(60)
                .saturating_mul(NANOSECONDS_IN_SECOND);
            UnixNanos::from(ts_now.as_u64().saturating_sub(lookback_ns))
        });

        let order_cmd = GenerateOrderStatusReportsBuilder::default()
            .ts_init(ts_now)
            .open_only(false)
            .start(start)
            .build()
            .context("Failed to build GenerateOrderStatusReports")?;

        let position_cmd = GeneratePositionStatusReportsBuilder::default()
            .ts_init(ts_now)
            .start(start)
            .build()
            .context("Failed to build GeneratePositionStatusReports")?;

        let (order_reports, position_reports) = tokio::try_join!(
            self.generate_order_status_reports(&order_cmd),
            self.generate_position_status_reports(&position_cmd),
        )?;

        log::info!("Received {} OrderStatusReports", order_reports.len());
        log::info!("Received {} PositionReports", position_reports.len());

        let mut mass_status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            *ALPACA_VENUE,
            ts_now,
            None,
        );

        mass_status.add_order_reports(order_reports);
        mass_status.add_position_reports(position_reports);

        Ok(Some(mass_status))
    }
}

/// Dispatches a trade-updates stream message using the event emitter.
fn dispatch_ws_message(
    message: NautilusWsMessage,
    emitter: &ExecutionEventEmitter,
    http_client: &AlpacaHttpClient,
    account_id: AccountId,
    clock: &AtomicTime,
) {
    match message {
        NautilusWsMessage::TradeUpdate(msg) => {
            dispatch_trade_update(&msg, emitter, http_client, account_id, clock);
        }
        NautilusWsMessage::Authenticated => {
            log::debug!("Trade-updates stream authenticated");
        }
        NautilusWsMessage::Error { code, msg } => {
            log::warn!("Trade-updates stream error: code={code:?}, msg={msg}");
        }
        NautilusWsMessage::Reconnected => {
            log::info!("Trade-updates stream reconnected");
        }
        NautilusWsMessage::Trades(_)
        | NautilusWsMessage::Quote(_)
        | NautilusWsMessage::Bar(_)
        | NautilusWsMessage::SubscriptionAck(_) => {
            log::trace!("Ignoring market data message in execution client");
        }
    }
}

fn dispatch_trade_update(
    msg: &AlpacaTradeUpdateMsg,
    emitter: &ExecutionEventEmitter,
    http_client: &AlpacaHttpClient,
    account_id: AccountId,
    clock: &AtomicTime,
) {
    let Some(symbol) = msg.order.symbol.as_deref().filter(|s| !s.is_empty()) else {
        log::warn!(
            "Skipping trade update without symbol (event={event})",
            event = msg.event,
        );
        return;
    };

    let symbol_ustr = Ustr::from(symbol);
    let Some(instrument) = http_client.get_instrument(&symbol_ustr) else {
        log::warn!("No cached instrument for trade update: {symbol}");
        return;
    };

    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();
    let ts_init = clock.get_time_ns();

    let report = match parse_ws_order_status_report(
        &msg.order,
        account_id,
        price_precision,
        size_precision,
        ts_init,
    ) {
        Ok(report) => report,
        Err(e) => {
            log::warn!("Failed to parse trade update order for {symbol}: {e}");
            return;
        }
    };

    match msg.event {
        AlpacaTradeUpdateEvent::Fill | AlpacaTradeUpdateEvent::PartialFill => {
            match parse_ws_fill_report(msg, &report, price_precision, size_precision, ts_init) {
                Ok(fill) => emitter.send_order_with_fills(report, vec![fill]),
                Err(e) => {
                    log::warn!("Failed to parse fill for {symbol}: {e}");
                    emitter.send_order_status_report(report);
                }
            }
        }
        AlpacaTradeUpdateEvent::OrderCancelRejected
        | AlpacaTradeUpdateEvent::OrderReplaceRejected => {
            // The embedded order still carries its live status; the engine
            // resolves the in-flight command against this report.
            log::warn!(
                "Order {event} for {symbol} (venue_order_id={id})",
                event = msg.event,
                id = msg.order.id,
            );
            emitter.send_order_status_report(report);
        }
        _ => emitter.send_order_status_report(report),
    }
}

/// Builds an [`OrderStatusReport`] from a trade-updates stream order object.
///
/// # Errors
///
/// Returns an error if required fields are missing or unparseable.
fn parse_ws_order_status_report(
    order: &AlpacaWsOrder,
    account_id: AccountId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    let symbol = order
        .symbol
        .as_deref()
        .filter(|s| !s.is_empty())
        .context("order has no symbol (multi-leg parent orders are unsupported)")?;
    let instrument_id = InstrumentId::new(Symbol::from(symbol), *ALPACA_VENUE);

    let client_order_id = if order.client_order_id.is_empty() {
        None
    } else {
        Some(ClientOrderId::new(order.client_order_id.as_str()))
    };
    let venue_order_id = VenueOrderId::new(order.id.as_str());

    let order_side: OrderSide = order.side.map(Into::into).context("order has no side")?;
    let order_type: OrderType = order
        .order_type
        .map(Into::into)
        .context("order has no type")?;
    let time_in_force: TimeInForce = order
        .time_in_force
        .map(Into::into)
        .context("order has no time in force")?;
    let order_status: OrderStatus = order.status.into();

    let filled_qty = match order.filled_qty.as_deref() {
        Some(value) => parse_quantity(value, size_precision)?,
        None => Quantity::new(0.0, size_precision),
    };
    let quantity = match order.qty.as_deref() {
        Some(value) => parse_quantity(value, size_precision)?,
        None => filled_qty,
    };

    let ts_accepted = match order
        .submitted_at
        .as_deref()
        .or(order.created_at.as_deref())
    {
        Some(value) => parse_rfc3339_timestamp(value, "order.submitted_at")?,
        None => ts_init,
    };
    let ts_last = match order
        .updated_at
        .as_deref()
        .or(order.filled_at.as_deref())
        .or(order.created_at.as_deref())
    {
        Some(value) => parse_rfc3339_timestamp(value, "order.updated_at")?,
        None => ts_accepted,
    };

    let mut report = OrderStatusReport::new(
        account_id,
        instrument_id,
        client_order_id,
        venue_order_id,
        order_side,
        order_type,
        time_in_force,
        order_status,
        quantity,
        filled_qty,
        ts_accepted,
        ts_last,
        ts_init,
        None,
    );

    if let Some(limit_price) = order.limit_price.as_deref() {
        report = report.with_price(parse_price(limit_price, price_precision)?);
    }

    if let Some(stop_price) = order.stop_price.as_deref() {
        report = report.with_trigger_price(parse_price(stop_price, price_precision)?);
    }

    if let Some(avg_px) = order.filled_avg_price.as_deref()
        && let Ok(decimal) = Decimal::from_str(avg_px)
    {
        report.avg_px = Some(decimal);
    }

    Ok(report)
}

/// Builds a [`FillReport`] from a `fill` / `partial_fill` trade update.
///
/// # Errors
///
/// Returns an error if the execution price or quantity is missing or
/// unparseable.
fn parse_ws_fill_report(
    msg: &AlpacaTradeUpdateMsg,
    report: &OrderStatusReport,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<FillReport> {
    let price = msg.price.as_deref().context("fill event has no price")?;
    let qty = msg.qty.as_deref().context("fill event has no qty")?;

    let last_px: Price = parse_price(price, price_precision)?;
    let last_qty: Quantity = parse_quantity(qty, size_precision)?;

    let trade_id_value = msg
        .execution_id
        .as_deref()
        .or(msg.event_id.as_deref())
        .map_or_else(
            || format!("{}-{}", msg.order.id, report.ts_last),
            str::to_string,
        );
    let trade_id = TradeId::new_checked(trade_id_value).context("invalid Alpaca trade ID")?;

    let ts_event = match msg.timestamp.as_deref().or(msg.at.as_deref()) {
        Some(value) => parse_rfc3339_timestamp(value, "trade_update.timestamp")?,
        None => report.ts_last,
    };

    Ok(FillReport::new(
        report.account_id,
        report.instrument_id,
        report.venue_order_id,
        trade_id,
        report.order_side,
        last_qty,
        last_px,
        // Alpaca is commission-free for US equities; regulatory fees arrive
        // via account activities, not per execution.
        Money::new(0.0, Currency::USD()),
        LiquiditySide::NoLiquiditySide,
        report.client_order_id,
        None,
        ts_event,
        ts_init,
        None,
    ))
}

/// Builds an [`OrderStatusReport`] resolving precisions from the instrument cache.
fn order_report_with_precisions(
    http_client: &AlpacaHttpClient,
    order: &crate::http::models::AlpacaOrder,
    account_id: AccountId,
    ts_init: UnixNanos,
) -> Option<OrderStatusReport> {
    let symbol = order.symbol?;
    let (price_precision, size_precision) = match http_client.get_instrument(&symbol) {
        Some(instrument) => (instrument.price_precision(), instrument.size_precision()),
        None => {
            log::warn!("No cached instrument for order report: {symbol}");
            return None;
        }
    };

    match crate::http::parse::parse_order_status_report(
        order,
        account_id,
        price_precision,
        size_precision,
        ts_init,
    ) {
        Ok(report) => Some(report),
        Err(e) => {
            log::warn!("Skipping unparseable order {id}: {e}", id = order.id);
            None
        }
    }
}

/// Maps an Alpaca account object into Nautilus USD [`AccountBalance`]s.
///
/// # Errors
///
/// Returns an error if the equity field is missing or unparseable.
fn parse_account_balances(account: &AlpacaAccount) -> anyhow::Result<Vec<AccountBalance>> {
    let equity = account.equity.as_deref().context("account has no equity")?;
    let total = Decimal::from_str(equity).context("invalid account equity")?;

    let locked = account
        .initial_margin
        .as_deref()
        .map(Decimal::from_str)
        .transpose()
        .context("invalid account initial_margin")?
        .unwrap_or_default();
    let free = total - locked;

    let currency = Currency::USD();
    let to_money = |value: Decimal| -> anyhow::Result<Money> {
        let value = value.to_f64().context("account balance out of range")?;
        Ok(Money::new(value, currency))
    };

    Ok(vec![AccountBalance::new(
        to_money(total)?,
        to_money(locked)?,
        to_money(free)?,
    )])
}

/// Maps an Alpaca position into a [`PositionStatusReport`].
///
/// # Errors
///
/// Returns an error if the quantity or entry price cannot be parsed.
fn parse_position_status_report(
    position: &AlpacaPosition,
    account_id: AccountId,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<PositionStatusReport> {
    let instrument_id =
        InstrumentId::new(Symbol::from_ustr_unchecked(position.symbol), *ALPACA_VENUE);

    // Short positions report a negative quantity string.
    let qty_decimal = Decimal::from_str(&position.qty).context("invalid position quantity")?;
    let quantity = Quantity::from_decimal_dp(qty_decimal.abs(), size_precision)
        .map_err(|e| anyhow::anyhow!("position quantity out of range: {e}"))?;

    let position_side = match position.side {
        crate::common::enums::AlpacaPositionSide::Long => PositionSideSpecified::Long,
        crate::common::enums::AlpacaPositionSide::Short => PositionSideSpecified::Short,
    };

    let avg_px_open = Decimal::from_str(&position.avg_entry_price).ok();

    Ok(PositionStatusReport::new(
        account_id,
        instrument_id,
        position_side,
        quantity,
        ts_init,
        ts_init,
        None,
        None,
        avg_px_open,
    ))
}

/// Builds Alpaca order submission parameters from a Nautilus order.
///
/// # Errors
///
/// Returns an error for order types, time-in-force values, or execution
/// instructions Alpaca does not support.
fn build_post_order_params(
    order: &OrderAny,
) -> anyhow::Result<crate::http::query::PostOrderParams> {
    if order.is_post_only() {
        anyhow::bail!("Alpaca does not support post-only orders");
    }
    if order.is_reduce_only() {
        anyhow::bail!("Alpaca does not support reduce-only orders");
    }

    let side = AlpacaOrderSide::try_from(order.order_side())?;
    let order_type = AlpacaOrderType::try_from(order.order_type())?;
    let time_in_force = AlpacaTimeInForce::try_from(order.time_in_force())?;

    let mut builder = PostOrderParamsBuilder::default();
    builder
        .symbol(order.instrument_id().symbol.to_string())
        .qty(order.quantity().to_string())
        .side(side)
        .order_type(order_type)
        .time_in_force(time_in_force)
        .client_order_id(order.client_order_id().to_string());

    match order.order_type() {
        OrderType::Limit | OrderType::StopLimit => {
            let price = order.price().context("limit order has no price")?;
            builder.limit_price(price.to_string());
        }
        _ => {}
    }

    match order.order_type() {
        OrderType::StopMarket | OrderType::StopLimit => {
            let trigger_price = order
                .trigger_price()
                .context("stop order has no trigger price")?;
            builder.stop_price(trigger_price.to_string());
        }
        OrderType::TrailingStopMarket => {
            let offset = order
                .trailing_offset()
                .context("trailing-stop order has no trailing offset")?;
            match order.trailing_offset_type() {
                Some(TrailingOffsetType::Price) => {
                    builder.trail_price(offset.to_string());
                }
                Some(TrailingOffsetType::BasisPoints) => {
                    let percent = offset / Decimal::from(100);
                    builder.trail_percent(percent.to_string());
                }
                other => anyhow::bail!(
                    "unsupported trailing offset type for Alpaca: {other:?} (use Price or BasisPoints)"
                ),
            }
        }
        _ => {}
    }

    builder
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build order params: {e}"))
}

fn reject_modify_command(
    emitter: &ExecutionEventEmitter,
    clock: &AtomicTime,
    cmd: &ModifyOrder,
    reason: &str,
) -> anyhow::Result<()> {
    let ts_event = clock.get_time_ns();
    emitter.emit_order_modify_rejected_event(
        cmd.strategy_id,
        cmd.instrument_id,
        cmd.client_order_id,
        cmd.venue_order_id,
        reason,
        ts_event,
    );
    anyhow::bail!("{reason}");
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::TriggerType, identifiers::InstrumentId, orders::builder::OrderTestBuilder,
    };
    use rstest::rstest;

    use super::*;

    fn instrument_id() -> InstrumentId {
        InstrumentId::from("AAPL.ALPACA")
    }

    #[rstest]
    fn test_build_post_order_params_market() {
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument_id())
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100))
            .build();

        let params = build_post_order_params(&order).unwrap();

        assert_eq!(params.symbol, "AAPL");
        assert_eq!(params.qty.as_deref(), Some("100"));
        assert_eq!(params.side, AlpacaOrderSide::Buy);
        assert_eq!(params.order_type, AlpacaOrderType::Market);
        assert_eq!(params.limit_price, None);
        assert_eq!(params.stop_price, None);
        assert!(params.client_order_id.is_some());
    }

    #[rstest]
    fn test_build_post_order_params_limit() {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument_id())
            .side(OrderSide::Sell)
            .quantity(Quantity::from(50))
            .price(Price::new(189.05, 2))
            .build();

        let params = build_post_order_params(&order).unwrap();

        assert_eq!(params.order_type, AlpacaOrderType::Limit);
        assert_eq!(params.limit_price.as_deref(), Some("189.05"));
    }

    #[rstest]
    fn test_build_post_order_params_stop_limit() {
        let order = OrderTestBuilder::new(OrderType::StopLimit)
            .instrument_id(instrument_id())
            .side(OrderSide::Sell)
            .quantity(Quantity::from(10))
            .price(Price::new(180.00, 2))
            .trigger_price(Price::new(182.50, 2))
            .build();

        let params = build_post_order_params(&order).unwrap();

        assert_eq!(params.order_type, AlpacaOrderType::StopLimit);
        assert_eq!(params.limit_price.as_deref(), Some("180.00"));
        assert_eq!(params.stop_price.as_deref(), Some("182.50"));
    }

    #[rstest]
    fn test_build_post_order_params_fractional_qty() {
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument_id())
            .side(OrderSide::Buy)
            .quantity(Quantity::new(0.5, 9))
            .build();

        let params = build_post_order_params(&order).unwrap();

        assert_eq!(params.qty.as_deref(), Some("0.500000000"));
    }

    #[rstest]
    fn test_build_post_order_params_rejects_post_only() {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument_id())
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .price(Price::new(100.00, 2))
            .post_only(true)
            .build();

        let err = build_post_order_params(&order).unwrap_err();
        assert!(err.to_string().contains("post-only"));
    }

    #[rstest]
    fn test_build_post_order_params_rejects_unsupported_tif() {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument_id())
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .price(Price::new(100.00, 2))
            .time_in_force(TimeInForce::Gtd)
            .expire_time(UnixNanos::from(1))
            .build();

        let err = build_post_order_params(&order).unwrap_err();
        assert!(err.to_string().contains("time in force"));
    }

    #[rstest]
    fn test_build_post_order_params_trailing_stop_price_offset() {
        let order = OrderTestBuilder::new(OrderType::TrailingStopMarket)
            .instrument_id(instrument_id())
            .side(OrderSide::Sell)
            .quantity(Quantity::from(10))
            .trigger_price(Price::new(180.00, 2))
            .trigger_type(TriggerType::LastPrice)
            .trailing_offset(Decimal::new(150, 2))
            .trailing_offset_type(TrailingOffsetType::Price)
            .build();

        let params = build_post_order_params(&order).unwrap();

        assert_eq!(params.order_type, AlpacaOrderType::TrailingStop);
        assert_eq!(params.trail_price.as_deref(), Some("1.50"));
        assert_eq!(params.trail_percent, None);
    }

    #[rstest]
    fn test_parse_ws_order_status_report_fill_event() {
        let json = r#"{
            "event": "fill",
            "timestamp": "2026-07-01T14:30:00.123456789Z",
            "price": "189.05",
            "qty": "100",
            "position_qty": "100",
            "execution_id": "5f9b1c2d-1b1a-4d3e-9f2a-1c2d3e4f5a6b",
            "order": {
                "id": "61e69015-8549-4bfd-b9c3-01e75843f47d",
                "client_order_id": "O-20260701-001",
                "status": "filled",
                "symbol": "AAPL",
                "side": "buy",
                "type": "limit",
                "time_in_force": "day",
                "qty": "100",
                "filled_qty": "100",
                "filled_avg_price": "189.05",
                "limit_price": "189.10",
                "created_at": "2026-07-01T14:29:59.000000000Z",
                "submitted_at": "2026-07-01T14:29:59.100000000Z",
                "updated_at": "2026-07-01T14:30:00.123456789Z",
                "filled_at": "2026-07-01T14:30:00.123456789Z"
            }
        }"#;
        let msg: AlpacaTradeUpdateMsg = serde_json::from_str(json).unwrap();
        let account_id = AccountId::from("ALPACA-001");
        let ts_init = UnixNanos::from(1);

        let report = parse_ws_order_status_report(&msg.order, account_id, 2, 0, ts_init).unwrap();

        assert_eq!(report.instrument_id, instrument_id());
        assert_eq!(
            report.client_order_id,
            Some(ClientOrderId::new("O-20260701-001"))
        );
        assert_eq!(
            report.venue_order_id,
            VenueOrderId::new("61e69015-8549-4bfd-b9c3-01e75843f47d")
        );
        assert_eq!(report.order_status, OrderStatus::Filled);
        assert_eq!(report.quantity, Quantity::from(100));
        assert_eq!(report.filled_qty, Quantity::from(100));
        assert_eq!(report.price, Some(Price::new(189.10, 2)));
        assert_eq!(report.avg_px, Some(Decimal::from_str("189.05").unwrap()));

        let fill = parse_ws_fill_report(&msg, &report, 2, 0, ts_init).unwrap();

        assert_eq!(fill.last_px, Price::new(189.05, 2));
        assert_eq!(fill.last_qty, Quantity::from(100));
        assert_eq!(
            fill.trade_id,
            TradeId::new("5f9b1c2d-1b1a-4d3e-9f2a-1c2d3e4f5a6b")
        );
        assert_eq!(fill.order_side, OrderSide::Buy);
        assert_eq!(fill.liquidity_side, LiquiditySide::NoLiquiditySide);
    }

    #[rstest]
    fn test_parse_account_balances() {
        let json = r#"{
            "id": "e29f2b0a-4237-4e15-a5c1-2b3ad4b13c38",
            "status": "ACTIVE",
            "currency": "USD",
            "cash": "25000.50",
            "equity": "100000.25",
            "initial_margin": "40000.00"
        }"#;
        let account: AlpacaAccount = serde_json::from_str(json).unwrap();

        let balances = parse_account_balances(&account).unwrap();

        assert_eq!(balances.len(), 1);
        let balance = &balances[0];
        assert_eq!(balance.total, Money::new(100_000.25, Currency::USD()));
        assert_eq!(balance.locked, Money::new(40_000.00, Currency::USD()));
        assert_eq!(balance.free, Money::new(60_000.25, Currency::USD()));
    }

    #[rstest]
    fn test_parse_position_status_report_short() {
        let json = r#"{
            "asset_id": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
            "symbol": "AAPL",
            "exchange": "NASDAQ",
            "asset_class": "us_equity",
            "avg_entry_price": "190.32",
            "qty": "-25",
            "side": "short"
        }"#;
        let position: AlpacaPosition = serde_json::from_str(json).unwrap();
        let account_id = AccountId::from("ALPACA-001");

        let report =
            parse_position_status_report(&position, account_id, 0, UnixNanos::from(1)).unwrap();

        assert_eq!(report.instrument_id, instrument_id());
        assert_eq!(report.position_side, PositionSideSpecified::Short);
        assert_eq!(report.quantity, Quantity::from(25));
        assert_eq!(
            report.avg_px_open,
            Some(Decimal::from_str("190.32").unwrap())
        );
    }
}
