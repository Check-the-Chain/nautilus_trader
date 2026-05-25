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

//! Lighter live execution probe through a Nautilus strategy and LiveNode.
//!
//! This sends real orders. It uses the configured account with the minimum BTC-PERP order size by
//! default and waits for Nautilus order state events before advancing each step.
//!
//! Run with:
//! `cargo run -p nautilus-lighter --example lighter-node-exec-state-probe --features examples`
//!
//! Required environment variables:
//! - `LIGHTER_ACCOUNT_INDEX`
//! - `LIGHTER_API_KEY_INDEX`
//! - `LIGHTER_PRIVATE_KEY`
//!
//! Optional environment variables:
//! - `LIGHTER_INSTRUMENT_ID`, defaults to `BTC-PERP.LIGHTER`
//! - `LIGHTER_ORDER_QTY`, defaults to `0.00020`
//! - `LIGHTER_SIGNER_LIB_PATH`
//! - `LIGHTER_ENV`, use `testnet` for testnet, defaults to mainnet
//! - `LIGHTER_BASE_URL_HTTP` / `LIGHTER_BASE_URL_WS`, for endpoint overrides
//! - `LIGHTER_PROXY_URL`, for HTTP/WebSocket proxying
//! - `PROBE_TIMEOUT_SECS`, defaults to 90
//! - `PROBE_RECONCILIATION`, defaults to `false`
//! - `PROBE_RUN_TAKER`, defaults to `true`

use std::{
    env,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use log::LevelFilter;
use nautilus_common::{actor::DataActor, enums::Environment, logging::logger::LoggerConfig};
use nautilus_lighter::{
    config::{LighterDataClientConfig, LighterEnvironment, LighterExecClientConfig},
    factories::{
        LighterDataClientFactory, LighterExecFactoryConfig, LighterExecutionClientFactory,
    },
};
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::QuoteTick,
    enums::{OrderSide, TimeInForce},
    events::{
        OrderAccepted, OrderCancelRejected, OrderCanceled, OrderFilled, OrderModifyRejected,
        OrderRejected, OrderSubmitted, OrderUpdated,
    },
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    types::{Price, Quantity},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProbePhase {
    WaitingForQuote,
    FirstSubmitted,
    FirstModifyRequested,
    FirstCancelRequested,
    BatchSubmitted,
    BatchCancelRequested,
    MarketBuySubmitted,
    MarketSellSubmitted,
    Done,
    Failed,
}

#[derive(Debug)]
struct ProbeResult {
    phase: ProbePhase,
    submitted: usize,
    accepted: usize,
    updated: usize,
    canceled: usize,
    filled: usize,
    failure: Option<String>,
}

impl Default for ProbeResult {
    fn default() -> Self {
        Self {
            phase: ProbePhase::WaitingForQuote,
            submitted: 0,
            accepted: 0,
            updated: 0,
            canceled: 0,
            filled: 0,
            failure: None,
        }
    }
}

#[derive(Debug)]
struct LighterNodeExecStateProbe {
    core: StrategyCore,
    client_id: ClientId,
    instrument_id: InstrumentId,
    quantity: Quantity,
    instrument: Option<InstrumentAny>,
    phase: ProbePhase,
    first_order_id: Option<ClientOrderId>,
    batch_order_ids: Vec<ClientOrderId>,
    market_buy_id: Option<ClientOrderId>,
    market_sell_id: Option<ClientOrderId>,
    run_taker: bool,
    result: Arc<Mutex<ProbeResult>>,
}

impl LighterNodeExecStateProbe {
    fn new(config: ProbeConfig, result: Arc<Mutex<ProbeResult>>) -> Self {
        Self {
            core: StrategyCore::new(config.base),
            client_id: config.client_id,
            instrument_id: config.instrument_id,
            quantity: config.quantity,
            instrument: None,
            phase: ProbePhase::WaitingForQuote,
            first_order_id: None,
            batch_order_ids: Vec::new(),
            market_buy_id: None,
            market_sell_id: None,
            run_taker: config.run_taker,
            result,
        }
    }

    fn mark<F>(&self, f: F)
    where
        F: FnOnce(&mut ProbeResult),
    {
        if let Ok(mut result) = self.result.lock() {
            f(&mut result);
        }
    }

    fn set_phase(&mut self, phase: ProbePhase) {
        self.phase = phase;
        self.mark(|result| result.phase = phase);
    }

    fn fail(&mut self, reason: impl Into<String>) {
        let reason = reason.into();
        log::error!("Lighter node exec state probe failed: {reason}");
        self.phase = ProbePhase::Failed;
        self.mark(|result| {
            result.phase = ProbePhase::Failed;
            result.failure = Some(reason);
        });
    }

    fn instrument(&self) -> Option<&InstrumentAny> {
        self.instrument.as_ref()
    }

    fn cached_order(&self, client_order_id: ClientOrderId) -> Option<OrderAny> {
        let cache = self.cache();
        cache.order(&client_order_id).cloned()
    }

    fn safe_limit_price(
        instrument: &InstrumentAny,
        quote: &QuoteTick,
        side: OrderSide,
        distance_ticks: f64,
    ) -> Price {
        let offset = instrument.price_increment().as_f64() * distance_ticks;
        match side {
            OrderSide::Buy => {
                instrument.make_price((quote.bid_price.as_f64() - offset).max(offset))
            }
            OrderSide::Sell => instrument.make_price(quote.ask_price.as_f64() + offset),
            _ => unreachable!("probe only submits buy and sell orders"),
        }
    }

    fn submit_first_limit(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        let Some(instrument) = self.instrument() else {
            return Ok(());
        };
        let price = Self::safe_limit_price(instrument, quote, OrderSide::Buy, 5_000.0);
        let order = self.core.order_factory().limit(
            self.instrument_id,
            OrderSide::Buy,
            self.quantity,
            price,
            Some(TimeInForce::Gtc),
            None,
            Some(true),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        self.first_order_id = Some(order.client_order_id());
        self.submit_order(order, None, Some(self.client_id))?;
        self.set_phase(ProbePhase::FirstSubmitted);
        Ok(())
    }

    fn request_first_modify(&mut self, client_order_id: ClientOrderId) -> anyhow::Result<()> {
        let Some(instrument) = self.instrument() else {
            return Ok(());
        };
        let Some(order) = self.cached_order(client_order_id) else {
            anyhow::bail!("accepted order {client_order_id} not found in cache");
        };
        let Some(price) = order.price() else {
            anyhow::bail!("accepted order {client_order_id} has no limit price");
        };
        let offset = instrument.price_increment().as_f64() * 2_000.0;
        let updated_price = instrument.make_price((price.as_f64() - offset).max(offset));
        self.modify_order(order, None, Some(updated_price), None, Some(self.client_id))?;
        self.set_phase(ProbePhase::FirstModifyRequested);
        Ok(())
    }

    fn request_first_cancel(&mut self, client_order_id: ClientOrderId) -> anyhow::Result<()> {
        let Some(order) = self.cached_order(client_order_id) else {
            anyhow::bail!("updated order {client_order_id} not found in cache");
        };
        self.cancel_order(order, Some(self.client_id))?;
        self.set_phase(ProbePhase::FirstCancelRequested);
        Ok(())
    }

    fn submit_batch_limits(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        let Some(instrument) = self.instrument() else {
            return Ok(());
        };
        let buy_price = Self::safe_limit_price(instrument, quote, OrderSide::Buy, 7_500.0);
        let sell_price = Self::safe_limit_price(instrument, quote, OrderSide::Sell, 7_500.0);
        let buy_order = self.core.order_factory().limit(
            self.instrument_id,
            OrderSide::Buy,
            self.quantity,
            buy_price,
            Some(TimeInForce::Gtc),
            None,
            Some(true),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let sell_order = self.core.order_factory().limit(
            self.instrument_id,
            OrderSide::Sell,
            self.quantity,
            sell_price,
            Some(TimeInForce::Gtc),
            None,
            Some(true),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        self.batch_order_ids = vec![buy_order.client_order_id(), sell_order.client_order_id()];
        self.submit_order_list(vec![buy_order, sell_order], None, Some(self.client_id))?;
        self.set_phase(ProbePhase::BatchSubmitted);
        Ok(())
    }

    fn request_batch_cancel(&mut self) -> anyhow::Result<()> {
        let mut orders = Vec::with_capacity(self.batch_order_ids.len());
        for client_order_id in &self.batch_order_ids {
            let Some(order) = self.cached_order(*client_order_id) else {
                anyhow::bail!("batch order {client_order_id} not found in cache");
            };
            orders.push(order);
        }
        self.cancel_orders(orders, Some(self.client_id), None)?;
        self.set_phase(ProbePhase::BatchCancelRequested);
        Ok(())
    }

    fn submit_market_buy(&mut self) -> anyhow::Result<()> {
        let order = self.core.order_factory().market(
            self.instrument_id,
            OrderSide::Buy,
            self.quantity,
            Some(TimeInForce::Ioc),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        self.market_buy_id = Some(order.client_order_id());
        self.submit_order(order, None, Some(self.client_id))?;
        self.set_phase(ProbePhase::MarketBuySubmitted);
        Ok(())
    }

    fn submit_market_sell(&mut self) -> anyhow::Result<()> {
        let order = self.core.order_factory().market(
            self.instrument_id,
            OrderSide::Sell,
            self.quantity,
            Some(TimeInForce::Ioc),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        self.market_sell_id = Some(order.client_order_id());
        self.submit_order(order, None, Some(self.client_id))?;
        self.set_phase(ProbePhase::MarketSellSubmitted);
        Ok(())
    }

    fn maybe_cancel_batch_after_accept(&mut self) -> anyhow::Result<()> {
        if self.phase != ProbePhase::BatchSubmitted {
            return Ok(());
        }
        let all_accepted = self.batch_order_ids.iter().all(|client_order_id| {
            self.cached_order(*client_order_id)
                .is_some_and(|order| order.is_open())
        });
        if all_accepted {
            self.request_batch_cancel()?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ProbeConfig {
    base: StrategyConfig,
    client_id: ClientId,
    instrument_id: InstrumentId,
    quantity: Quantity,
    run_taker: bool,
}

impl DataActor for LighterNodeExecStateProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        Strategy::on_start(self)?;
        self.subscribe_instrument(self.instrument_id, Some(self.client_id), None);
        self.subscribe_quotes(self.instrument_id, Some(self.client_id), None);
        Ok(())
    }

    fn on_instrument(&mut self, instrument: &InstrumentAny) -> anyhow::Result<()> {
        if instrument.id() == self.instrument_id {
            log::info!("Probe loaded instrument {}", instrument.id());
            self.instrument = Some(instrument.clone());
        }
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        if quote.instrument_id != self.instrument_id {
            return Ok(());
        }

        match self.phase {
            ProbePhase::WaitingForQuote => self.submit_first_limit(quote)?,
            ProbePhase::FirstCancelRequested if self.first_order_id.is_none() => {
                self.submit_batch_limits(quote)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn on_order_canceled(&mut self, event: &OrderCanceled) -> anyhow::Result<()> {
        self.mark(|result| result.canceled += 1);
        if Some(event.client_order_id) == self.first_order_id {
            self.first_order_id = None;
            return Ok(());
        }

        if self.batch_order_ids.contains(&event.client_order_id) {
            let all_canceled = self.batch_order_ids.iter().all(|client_order_id| {
                self.cached_order(*client_order_id)
                    .is_some_and(|order| order.is_closed())
            });
            if all_canceled {
                if self.run_taker {
                    self.submit_market_buy()?;
                } else {
                    self.set_phase(ProbePhase::Done);
                }
            }
        }
        Ok(())
    }

    fn on_order_filled(&mut self, event: &OrderFilled) -> anyhow::Result<()> {
        self.mark(|result| result.filled += 1);
        if Some(event.client_order_id) == self.market_buy_id {
            self.submit_market_sell()?;
        } else if Some(event.client_order_id) == self.market_sell_id {
            self.set_phase(ProbePhase::Done);
        }
        Ok(())
    }
}

nautilus_strategy!(LighterNodeExecStateProbe, {
    fn on_order_submitted(&mut self, event: OrderSubmitted) {
        log::info!("Probe submitted {}", event.client_order_id);
        self.mark(|result| result.submitted += 1);
    }

    fn on_order_accepted(&mut self, event: OrderAccepted) {
        log::info!("Probe accepted {}", event.client_order_id);
        self.mark(|result| result.accepted += 1);
        if Some(event.client_order_id) == self.first_order_id
            && self.phase == ProbePhase::FirstSubmitted
        {
            if let Err(e) = self.request_first_modify(event.client_order_id) {
                self.fail(e.to_string());
            }
        } else if self.batch_order_ids.contains(&event.client_order_id)
            && let Err(e) = self.maybe_cancel_batch_after_accept()
        {
            self.fail(e.to_string());
        }
    }

    fn on_order_updated(&mut self, event: OrderUpdated) {
        log::info!("Probe updated {}", event.client_order_id);
        self.mark(|result| result.updated += 1);
        if Some(event.client_order_id) == self.first_order_id
            && self.phase == ProbePhase::FirstModifyRequested
            && let Err(e) = self.request_first_cancel(event.client_order_id)
        {
            self.fail(e.to_string());
        }
    }

    fn on_order_rejected(&mut self, event: OrderRejected) {
        self.fail(format!(
            "order rejected {} reason={}",
            event.client_order_id, event.reason
        ));
    }

    fn on_order_modify_rejected(&mut self, event: OrderModifyRejected) {
        self.fail(format!(
            "order modify rejected {} reason={}",
            event.client_order_id, event.reason
        ));
    }

    fn on_order_cancel_rejected(&mut self, event: OrderCancelRejected) {
        self.fail(format!(
            "order cancel rejected {} reason={}",
            event.client_order_id, event.reason
        ));
    }
});

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let account_index = parse_env::<i64>("LIGHTER_ACCOUNT_INDEX")?;
    let api_key_index = parse_env::<u8>("LIGHTER_API_KEY_INDEX")?;
    let private_key = env::var("LIGHTER_PRIVATE_KEY").context("missing LIGHTER_PRIVATE_KEY")?;
    let instrument_id = env::var("LIGHTER_INSTRUMENT_ID")
        .unwrap_or_else(|_| "BTC-PERP.LIGHTER".to_string())
        .parse::<InstrumentId>()?;
    let quantity = env::var("LIGHTER_ORDER_QTY")
        .unwrap_or_else(|_| "0.00020".to_string())
        .parse::<Quantity>()
        .map_err(|e| anyhow::anyhow!("invalid LIGHTER_ORDER_QTY: {e}"))?;
    let timeout_secs = env::var("PROBE_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(90);
    let reconciliation = bool_env("PROBE_RECONCILIATION", false);
    let run_taker = bool_env("PROBE_RUN_TAKER", true);
    let lighter_environment = match env::var("LIGHTER_ENV")
        .unwrap_or_else(|_| "mainnet".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "testnet" => LighterEnvironment::Testnet,
        _ => LighterEnvironment::Mainnet,
    };
    let trader_id = TraderId::from("LIGHTER-PROBE-001");
    let client_id = ClientId::from("LIGHTER");
    let account_id = AccountId::from(format!("LIGHTER-{account_index}"));
    let data_config = LighterDataClientConfig {
        base_url_http: env::var("LIGHTER_BASE_URL_HTTP").ok(),
        base_url_ws: env::var("LIGHTER_BASE_URL_WS").ok(),
        proxy_url: env::var("LIGHTER_PROXY_URL").ok(),
        environment: lighter_environment,
        ..Default::default()
    };
    let exec_config = LighterExecFactoryConfig {
        trader_id,
        account_id,
        config: LighterExecClientConfig {
            account_index: Some(account_index),
            private_key: Some(private_key),
            api_key_index: Some(api_key_index),
            signer_lib_path: env::var("LIGHTER_SIGNER_LIB_PATH").ok(),
            base_url_http: env::var("LIGHTER_BASE_URL_HTTP").ok(),
            base_url_ws: env::var("LIGHTER_BASE_URL_WS").ok(),
            proxy_url: env::var("LIGHTER_PROXY_URL").ok(),
            environment: lighter_environment,
            ..Default::default()
        },
    };

    let result = Arc::new(Mutex::new(ProbeResult::default()));
    let probe_config = ProbeConfig {
        base: StrategyConfig {
            strategy_id: Some(StrategyId::from("LIGHTER-EXEC-PROBE-001")),
            external_order_claims: Some(vec![instrument_id]),
            use_hyphens_in_client_order_ids: true,
            ..Default::default()
        },
        client_id,
        instrument_id,
        quantity,
        run_taker,
    };
    let probe = LighterNodeExecStateProbe::new(probe_config, Arc::clone(&result));

    let log_config = LoggerConfig {
        stdout_level: LevelFilter::Info,
        ..Default::default()
    };
    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_name("LIGHTER-NODE-EXEC-STATE-PROBE".to_string())
        .with_logging(log_config)
        .with_delay_post_stop_secs(10)
        .add_data_client(
            None,
            Box::new(LighterDataClientFactory::new()),
            Box::new(data_config),
        )?
        .add_exec_client(
            None,
            Box::new(LighterExecutionClientFactory::new()),
            Box::new(exec_config),
        )?
        .with_reconciliation(reconciliation)
        .build()?;

    node.add_strategy(probe)?;
    let handle = node.handle();
    let stopper_result = Arc::clone(&result);
    tokio::spawn(async move {
        let timeout = tokio::time::sleep(Duration::from_secs(timeout_secs));
        tokio::pin!(timeout);
        loop {
            let should_stop = stopper_result
                .lock()
                .is_ok_and(|result| matches!(result.phase, ProbePhase::Done | ProbePhase::Failed));
            if should_stop {
                handle.stop();
                return;
            }

            tokio::select! {
                () = &mut timeout => {
                    handle.stop();
                    return;
                }
                () = tokio::time::sleep(Duration::from_millis(250)) => {}
            }
        }
    });

    node.run().await?;

    let result = result.lock().expect("probe result lock poisoned");
    println!(
        "lighter_node_exec_state_probe: phase={:?} submitted={} accepted={} updated={} canceled={} filled={}",
        result.phase,
        result.submitted,
        result.accepted,
        result.updated,
        result.canceled,
        result.filled
    );
    if let Some(failure) = &result.failure {
        anyhow::bail!("probe failed: {failure}");
    }
    if result.phase != ProbePhase::Done {
        anyhow::bail!(
            "probe timed out before completion: phase={:?}",
            result.phase
        );
    }
    Ok(())
}

fn bool_env(name: &str, default: bool) -> bool {
    env::var(name).map_or(default, |value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "y" | "on"
        )
    })
}

fn parse_env<T>(name: &str) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = env::var(name).with_context(|| format!("missing {name}"))?;
    value
        .parse::<T>()
        .map_err(|e| anyhow::anyhow!("invalid {name}: {e}"))
}
