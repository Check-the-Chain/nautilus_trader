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

//! Quote-driven limit-chasing execution algorithm.
//!
//! The limit chaser expects primary LIMIT orders and maintains a single working
//! order per parent sequence. It can optionally slice larger parent orders into
//! interim child orders and uses modify-in-place for the final parent submit so
//! the sequence stays aligned with the execution-algorithm quantity accounting.

use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

use ahash::{AHashMap, AHashSet};
use nautilus_common::{
    actor::{DataActor, DataActorCore},
    timer::TimeEvent,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::QuoteTick,
    enums::{OrderStatus, OrderType, TimeInForce},
    identifiers::{ClientOrderId, ExecAlgorithmId, InstrumentId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    types::{Price, Quantity},
};
use serde::{Deserialize, Serialize};

use super::{ExecutionAlgorithm, ExecutionAlgorithmConfig, ExecutionAlgorithmCore};

fn default_exec_algorithm_id() -> Option<ExecAlgorithmId> {
    Some(ExecAlgorithmId::new("LIMIT_CHASER"))
}

const fn default_reprice_interval_ms() -> u64 {
    250
}

/// Configuration for [`LimitChaserAlgorithm`].
#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.trading", from_py_object)
)]
pub struct LimitChaserAlgorithmConfig {
    /// The unique ID for the execution algorithm.
    #[serde(default = "default_exec_algorithm_id")]
    pub exec_algorithm_id: Option<ExecAlgorithmId>,
    /// If events should be logged by the algorithm.
    #[serde(default = "nautilus_core::serialization::default_true")]
    pub log_events: bool,
    /// If commands should be logged by the algorithm.
    #[serde(default = "nautilus_core::serialization::default_true")]
    pub log_commands: bool,
    /// Passive offset from same-side top of book.
    #[serde(default)]
    pub follow_offset_ticks: u32,
    /// Aggressive offset from opposite-side top of book after deadline.
    #[serde(default)]
    pub aggressive_offset_ticks: u32,
    /// Optional time after which the sequence becomes aggressive.
    #[serde(default)]
    pub aggressive_after_secs: Option<f64>,
    /// Optional maximum quantity for interim spawned child orders.
    #[serde(default)]
    pub max_child_quantity: Option<f64>,
    /// Minimum time between repricing attempts.
    #[serde(default = "default_reprice_interval_ms")]
    pub reprice_interval_ms: u64,
    /// Minimum change required before sending a reprice.
    #[serde(default = "default_min_reprice_delta_ticks")]
    pub min_reprice_delta_ticks: u32,
}

const fn default_min_reprice_delta_ticks() -> u32 {
    1
}

impl Default for LimitChaserAlgorithmConfig {
    fn default() -> Self {
        Self {
            exec_algorithm_id: default_exec_algorithm_id(),
            log_events: true,
            log_commands: true,
            follow_offset_ticks: 0,
            aggressive_offset_ticks: 0,
            aggressive_after_secs: None,
            max_child_quantity: None,
            reprice_interval_ms: default_reprice_interval_ms(),
            min_reprice_delta_ticks: default_min_reprice_delta_ticks(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct LimitChaserSettings {
    follow_offset_ticks: u32,
    aggressive_offset_ticks: u32,
    aggressive_after_secs: Option<f64>,
    max_child_quantity: Option<Quantity>,
    reprice_interval_ms: u64,
    min_reprice_delta_ticks: u32,
}

#[derive(Clone, Debug)]
struct LimitChaserSequence {
    primary_order_id: ClientOrderId,
    instrument_id: InstrumentId,
    started_at_ns: UnixNanos,
    limit_price: Price,
    settings: LimitChaserSettings,
    working_order_id: Option<ClientOrderId>,
    last_reprice_ns: UnixNanos,
    cancel_requested: bool,
    pending_quantity: Option<Quantity>,
    pending_reduce_primary: bool,
}

/// Quote-driven native limit chaser execution algorithm.
#[derive(Debug)]
pub struct LimitChaserAlgorithm {
    /// The algorithm core.
    pub core: ExecutionAlgorithmCore,
    config: LimitChaserAlgorithmConfig,
    sequences: AHashMap<ClientOrderId, LimitChaserSequence>,
    instrument_sequences: AHashMap<InstrumentId, AHashSet<ClientOrderId>>,
    subscribed_instruments: AHashSet<InstrumentId>,
}

impl LimitChaserAlgorithm {
    /// Creates a new [`LimitChaserAlgorithm`] instance.
    #[must_use]
    pub fn new(config: LimitChaserAlgorithmConfig) -> Self {
        let exec_algorithm_id = config.exec_algorithm_id.or_else(default_exec_algorithm_id);
        let core_config = ExecutionAlgorithmConfig {
            exec_algorithm_id,
            log_events: config.log_events,
            log_commands: config.log_commands,
        };
        let config = LimitChaserAlgorithmConfig {
            exec_algorithm_id,
            ..config
        };

        Self {
            core: ExecutionAlgorithmCore::new(core_config),
            config,
            sequences: AHashMap::new(),
            instrument_sequences: AHashMap::new(),
            subscribed_instruments: AHashSet::new(),
        }
    }

    fn working_order(&self, sequence: &LimitChaserSequence) -> Option<OrderAny> {
        let working_order_id = sequence.working_order_id?;
        self.core.cache().order(&working_order_id).cloned()
    }

    fn primary_id_for_order(order: &OrderAny) -> ClientOrderId {
        order.exec_spawn_id().unwrap_or(order.client_order_id())
    }

    fn refresh_for_order(
        &mut self,
        client_order_id: ClientOrderId,
        force: bool,
    ) -> anyhow::Result<()> {
        let order = {
            let cache = self.core.cache();
            cache.order(&client_order_id).cloned()
        };

        let Some(order) = order else {
            return Ok(());
        };

        self.refresh_sequence(Self::primary_id_for_order(&order), force)
    }

    fn refresh_sequence(
        &mut self,
        primary_order_id: ClientOrderId,
        force: bool,
    ) -> anyhow::Result<()> {
        let Some(sequence) = self.sequences.get(&primary_order_id).cloned() else {
            return Ok(());
        };

        let primary_order = {
            let cache = self.core.cache();
            cache.order(&primary_order_id).cloned()
        };

        let Some(primary_order) = primary_order else {
            self.cleanup_sequence(primary_order_id);
            return Ok(());
        };

        if sequence.cancel_requested {
            let working_order = self.working_order(&sequence);
            if working_order.is_none() || working_order.as_ref().is_some_and(Order::is_closed) {
                self.cleanup_sequence(primary_order_id);
            }
            return Ok(());
        }

        let working_order = self.working_order(&sequence);
        if let Some(working_order) = working_order.clone()
            && working_order.is_closed()
        {
            self.handle_closed_working_order(sequence, primary_order, working_order)?;
            return Ok(());
        }

        if working_order.is_none() {
            let quote = {
                let cache = self.core.cache();
                cache.quote(&sequence.instrument_id).copied()
            };
            self.submit_pending_or_next(sequence, primary_order, quote)?;
            return Ok(());
        }

        let Some(quote) = self.core.cache().quote(&sequence.instrument_id).copied() else {
            return Ok(());
        };

        if let Some(working_order) = working_order {
            let now = self.core.clock().timestamp_ns();
            let min_reprice_ns =
                sequence.last_reprice_ns + sequence.settings.reprice_interval_ms * 1_000_000;

            if !force && now < min_reprice_ns {
                return Ok(());
            }

            if working_order.is_inflight()
                || working_order.is_pending_update()
                || working_order.is_pending_cancel()
            {
                return Ok(());
            }

            let Some(current_price) = working_order.price() else {
                return Ok(());
            };

            let target_price = self.target_price(&primary_order, &quote, &sequence)?;

            let instrument = {
                let cache = self.core.cache();
                cache.instrument(&sequence.instrument_id).cloned()
            };
            let Some(instrument) = instrument else {
                anyhow::bail!(
                    "Cannot refresh order: instrument {} not found",
                    sequence.instrument_id
                );
            };

            if !self.should_reprice(
                &instrument,
                current_price,
                target_price,
                sequence.settings.min_reprice_delta_ticks,
            ) {
                return Ok(());
            }

            let mut working_order = working_order;
            self.modify_order(&mut working_order, None, Some(target_price), None, None)?;
            if let Some(sequence_mut) = self.sequences.get_mut(&primary_order_id) {
                sequence_mut.last_reprice_ns = now;
            }
        }

        Ok(())
    }

    fn handle_closed_working_order(
        &mut self,
        sequence: LimitChaserSequence,
        primary_order: OrderAny,
        working_order: OrderAny,
    ) -> anyhow::Result<()> {
        let primary_order_id = sequence.primary_order_id;

        if let Some(sequence_mut) = self.sequences.get_mut(&primary_order_id) {
            sequence_mut.working_order_id = None;
        }

        if sequence.cancel_requested {
            self.cleanup_sequence(primary_order_id);
            return Ok(());
        }

        if working_order.client_order_id() == primary_order_id {
            self.cleanup_sequence(primary_order_id);
            return Ok(());
        }

        if matches!(
            working_order.status(),
            OrderStatus::Canceled | OrderStatus::Expired
        ) && working_order.leaves_qty() > Quantity::from(0)
            && let Some(sequence_mut) = self.sequences.get_mut(&primary_order_id)
        {
            sequence_mut.pending_quantity = Some(working_order.leaves_qty());
            sequence_mut.pending_reduce_primary = false;
        }

        let quote = {
            let cache = self.core.cache();
            cache.quote(&sequence.instrument_id).copied()
        };
        let sequence = self
            .sequences
            .get(&primary_order_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Unknown sequence for {primary_order_id}"))?;
        self.submit_pending_or_next(sequence, primary_order, quote)
    }

    fn submit_pending_or_next(
        &mut self,
        sequence: LimitChaserSequence,
        primary_order: OrderAny,
        quote: Option<QuoteTick>,
    ) -> anyhow::Result<()> {
        let Some(quote) = quote else {
            return Ok(());
        };

        if let Some(quantity) = sequence.pending_quantity {
            let reduce_primary = sequence.pending_reduce_primary;
            if let Some(sequence_mut) = self.sequences.get_mut(&sequence.primary_order_id) {
                sequence_mut.pending_quantity = None;
                sequence_mut.pending_reduce_primary = true;
            }

            return self.submit_spawned_order(
                sequence.primary_order_id,
                primary_order,
                quantity,
                quote,
                reduce_primary,
            );
        }

        let instrument = {
            let cache = self.core.cache();
            cache.instrument(&sequence.instrument_id).cloned()
        };
        let Some(next_quantity) = self.next_slice_quantity(
            instrument.as_ref(),
            primary_order.quantity(),
            &sequence.settings,
        )?
        else {
            return Ok(());
        };

        if next_quantity == primary_order.quantity() {
            return self.submit_primary_order(sequence.primary_order_id, primary_order, quote);
        }

        self.submit_spawned_order(
            sequence.primary_order_id,
            primary_order,
            next_quantity,
            quote,
            true,
        )
    }

    fn submit_primary_order(
        &mut self,
        primary_order_id: ClientOrderId,
        primary_order: OrderAny,
        quote: QuoteTick,
    ) -> anyhow::Result<()> {
        let sequence = self
            .sequences
            .get(&primary_order_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Unknown sequence for {primary_order_id}"))?;
        let target_price = self.target_price(&primary_order, &quote, &sequence)?;

        let mut primary_order = primary_order;
        self.modify_order_in_place(&mut primary_order, None, Some(target_price), None)?;
        self.submit_order(primary_order, None, None)?;

        if let Some(sequence_mut) = self.sequences.get_mut(&primary_order_id) {
            sequence_mut.working_order_id = Some(primary_order_id);
            sequence_mut.last_reprice_ns = self.core.clock().timestamp_ns();
        }

        Ok(())
    }

    fn submit_spawned_order(
        &mut self,
        primary_order_id: ClientOrderId,
        primary_order: OrderAny,
        quantity: Quantity,
        quote: QuoteTick,
        reduce_primary: bool,
    ) -> anyhow::Result<()> {
        let sequence = self
            .sequences
            .get(&primary_order_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Unknown sequence for {primary_order_id}"))?;
        let target_price = self.target_price(&primary_order, &quote, &sequence)?;

        let display_qty = self.child_display_qty(&primary_order, quantity);
        let tags = primary_order.tags().map(|tags| tags.to_vec());
        let time_in_force = primary_order.time_in_force();
        let expire_time = primary_order.expire_time();
        let post_only = primary_order.is_post_only();
        let reduce_only = primary_order.is_reduce_only();
        let mut primary_order = primary_order;
        let spawned = self.spawn_limit(
            &mut primary_order,
            quantity,
            target_price,
            time_in_force,
            expire_time,
            post_only,
            reduce_only,
            display_qty,
            None,
            tags,
            reduce_primary,
        );
        let spawned_id = spawned.client_order_id;
        self.submit_order(spawned.into(), None, None)?;

        if let Some(sequence_mut) = self.sequences.get_mut(&primary_order_id) {
            sequence_mut.working_order_id = Some(spawned_id);
            sequence_mut.last_reprice_ns = self.core.clock().timestamp_ns();
        }

        Ok(())
    }

    fn next_slice_quantity(
        &self,
        instrument: Option<&InstrumentAny>,
        primary_quantity: Quantity,
        settings: &LimitChaserSettings,
    ) -> anyhow::Result<Option<Quantity>> {
        let Some(instrument) = instrument else {
            anyhow::bail!("Cannot determine next slice quantity: instrument not found");
        };

        let Some(max_child_quantity) = settings.max_child_quantity else {
            return Ok(Some(primary_quantity));
        };

        if max_child_quantity >= primary_quantity {
            return Ok(Some(primary_quantity));
        }

        if max_child_quantity < instrument.size_increment() {
            anyhow::bail!(
                "Configured max_child_quantity {max_child_quantity} was below size increment {}",
                instrument.size_increment()
            );
        }

        Ok(Some(max_child_quantity))
    }

    fn target_price(
        &self,
        primary_order: &OrderAny,
        quote: &QuoteTick,
        sequence: &LimitChaserSequence,
    ) -> anyhow::Result<Price> {
        let instrument = {
            let cache = self.core.cache();
            cache.instrument(&primary_order.instrument_id()).cloned()
        };
        let Some(instrument) = instrument else {
            anyhow::bail!(
                "Cannot determine target price: instrument {} not found",
                primary_order.instrument_id()
            );
        };

        let tick_size = instrument.price_increment().as_f64();
        let mut target = if primary_order.is_buy() {
            if self.is_aggressive(sequence) {
                if primary_order.is_post_only() {
                    quote.bid_price.as_f64()
                } else {
                    quote.ask_price.as_f64()
                        + tick_size * f64::from(sequence.settings.aggressive_offset_ticks)
                }
            } else {
                quote.bid_price.as_f64()
                    - tick_size * f64::from(sequence.settings.follow_offset_ticks)
            }
        } else if self.is_aggressive(sequence) {
            if primary_order.is_post_only() {
                quote.ask_price.as_f64()
            } else {
                quote.bid_price.as_f64()
                    - tick_size * f64::from(sequence.settings.aggressive_offset_ticks)
            }
        } else {
            quote.ask_price.as_f64() + tick_size * f64::from(sequence.settings.follow_offset_ticks)
        };

        if primary_order.is_buy() {
            target = target.min(sequence.limit_price.as_f64());
        } else {
            target = target.max(sequence.limit_price.as_f64());
        }

        if !target.is_finite() || target <= 0.0 {
            anyhow::bail!("Computed invalid target price {target}");
        }

        instrument.try_make_price(target)
    }

    fn is_aggressive(&self, sequence: &LimitChaserSequence) -> bool {
        let Some(aggressive_after_secs) = sequence.settings.aggressive_after_secs else {
            return false;
        };

        let aggressive_after_ns = (aggressive_after_secs * 1_000_000_000.0) as u64;
        self.core.clock_rc().borrow().timestamp_ns() >= sequence.started_at_ns + aggressive_after_ns
    }

    fn should_reprice(
        &self,
        instrument: &InstrumentAny,
        current_price: Price,
        target_price: Price,
        min_delta_ticks: u32,
    ) -> bool {
        if current_price == target_price {
            return false;
        }

        if min_delta_ticks == 0 {
            return true;
        }

        let min_delta = instrument.price_increment().as_f64() * f64::from(min_delta_ticks);
        (target_price.as_f64() - current_price.as_f64()).abs() >= min_delta
    }

    fn child_display_qty(&self, primary_order: &OrderAny, quantity: Quantity) -> Option<Quantity> {
        let display_qty = primary_order.display_qty()?;
        Some(display_qty.min(quantity))
    }

    fn cleanup_sequence(&mut self, primary_order_id: ClientOrderId) {
        let Some(sequence) = self.sequences.remove(&primary_order_id) else {
            return;
        };

        let timer_name = primary_order_id.as_str();
        if self.core.clock().timer_names().contains(&timer_name) {
            self.core.clock().cancel_timer(timer_name);
        }

        if let Some(primary_ids) = self.instrument_sequences.get_mut(&sequence.instrument_id) {
            primary_ids.remove(&primary_order_id);
            if primary_ids.is_empty() {
                self.instrument_sequences.remove(&sequence.instrument_id);
                if self.subscribed_instruments.remove(&sequence.instrument_id) {
                    self.unsubscribe_quotes(sequence.instrument_id, None, None);
                }
            }
        }

        log::info!("Completed limit-chaser execution for {primary_order_id}");
    }

    fn resolve_settings(
        &self,
        order: &OrderAny,
        instrument: &InstrumentAny,
    ) -> anyhow::Result<LimitChaserSettings> {
        let config = &self.config;
        let params = order.exec_algorithm_params();

        let follow_offset_ticks =
            self.parse_u32_param(params, "follow_offset_ticks", config.follow_offset_ticks)?;
        let aggressive_offset_ticks = self.parse_u32_param(
            params,
            "aggressive_offset_ticks",
            config.aggressive_offset_ticks,
        )?;
        let aggressive_after_secs = self.parse_optional_f64_param(
            params,
            "aggressive_after_secs",
            config.aggressive_after_secs,
        )?;
        let reprice_interval_ms =
            self.parse_u64_param(params, "reprice_interval_ms", config.reprice_interval_ms)?;
        let min_reprice_delta_ticks = self.parse_u32_param(
            params,
            "min_reprice_delta_ticks",
            config.min_reprice_delta_ticks,
        )?;
        let max_child_quantity_value =
            self.parse_optional_f64_param(params, "max_child_quantity", config.max_child_quantity)?;

        anyhow::ensure!(reprice_interval_ms > 0, "reprice_interval_ms must be > 0");
        if let Some(aggressive_after_secs) = aggressive_after_secs {
            anyhow::ensure!(
                aggressive_after_secs.is_finite() && aggressive_after_secs > 0.0,
                "aggressive_after_secs must be finite and > 0"
            );
        }

        let max_child_quantity = match max_child_quantity_value {
            Some(value) => {
                anyhow::ensure!(
                    value.is_finite() && value > 0.0,
                    "max_child_quantity must be finite and > 0"
                );
                let quantity = instrument.try_make_qty(value, None)?;
                anyhow::ensure!(
                    quantity >= instrument.size_increment(),
                    "max_child_quantity {quantity} was smaller than size increment {}",
                    instrument.size_increment()
                );
                if let Some(min_quantity) = instrument.min_quantity() {
                    anyhow::ensure!(
                        quantity >= min_quantity,
                        "max_child_quantity {quantity} was smaller than min quantity {min_quantity}"
                    );
                }
                Some(quantity)
            }
            None => None,
        };

        Ok(LimitChaserSettings {
            follow_offset_ticks,
            aggressive_offset_ticks,
            aggressive_after_secs,
            max_child_quantity,
            reprice_interval_ms,
            min_reprice_delta_ticks,
        })
    }

    fn parse_u32_param(
        &self,
        params: Option<&indexmap::IndexMap<ustr::Ustr, ustr::Ustr>>,
        key: &str,
        default: u32,
    ) -> anyhow::Result<u32> {
        match params.and_then(|p| p.get(&ustr::Ustr::from(key))) {
            Some(value) => value
                .as_str()
                .parse::<u32>()
                .map_err(|e| anyhow::anyhow!("Invalid `{key}` value `{value}`: {e}")),
            None => Ok(default),
        }
    }

    fn parse_u64_param(
        &self,
        params: Option<&indexmap::IndexMap<ustr::Ustr, ustr::Ustr>>,
        key: &str,
        default: u64,
    ) -> anyhow::Result<u64> {
        match params.and_then(|p| p.get(&ustr::Ustr::from(key))) {
            Some(value) => value
                .as_str()
                .parse::<u64>()
                .map_err(|e| anyhow::anyhow!("Invalid `{key}` value `{value}`: {e}")),
            None => Ok(default),
        }
    }

    fn parse_optional_f64_param(
        &self,
        params: Option<&indexmap::IndexMap<ustr::Ustr, ustr::Ustr>>,
        key: &str,
        default: Option<f64>,
    ) -> anyhow::Result<Option<f64>> {
        match params.and_then(|p| p.get(&ustr::Ustr::from(key))) {
            Some(value) => {
                let parsed = value
                    .as_str()
                    .parse::<f64>()
                    .map_err(|e| anyhow::anyhow!("Invalid `{key}` value `{value}`: {e}"))?;
                Ok(Some(parsed))
            }
            None => Ok(default),
        }
    }
}

impl Deref for LimitChaserAlgorithm {
    type Target = DataActorCore;

    fn deref(&self) -> &Self::Target {
        &self.core.actor
    }
}

impl DerefMut for LimitChaserAlgorithm {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.core.actor
    }
}

impl DataActor for LimitChaserAlgorithm {
    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        let primary_ids: Vec<ClientOrderId> = self
            .instrument_sequences
            .get(&quote.instrument_id)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();

        for primary_order_id in primary_ids {
            if let Err(e) = self.refresh_sequence(primary_order_id, false) {
                log::error!(
                    "Failed to refresh limit-chaser sequence {primary_order_id} on quote: {e}"
                );
            }
        }

        Ok(())
    }
}

impl ExecutionAlgorithm for LimitChaserAlgorithm {
    fn core_mut(&mut self) -> &mut ExecutionAlgorithmCore {
        &mut self.core
    }

    fn on_order(&mut self, order: OrderAny) -> anyhow::Result<()> {
        let primary_order_id = order.client_order_id();
        if self.sequences.contains_key(&primary_order_id) {
            log::error!("Order {primary_order_id} is already being executed");
            return Ok(());
        }

        if order.order_type() != OrderType::Limit {
            log::error!(
                "Cannot execute order: only implemented for limit orders, order_type={:?}",
                order.order_type()
            );
            return Ok(());
        }

        if matches!(order.time_in_force(), TimeInForce::Fok | TimeInForce::Ioc) {
            log::error!(
                "Cannot execute order: unsupported time_in_force={:?} for limit chasing",
                order.time_in_force()
            );
            return Ok(());
        }

        let Some(limit_price) = order.price() else {
            log::error!("Cannot execute order: limit order had no price");
            return Ok(());
        };

        let instrument = {
            let cache = self.core.cache();
            cache.instrument(&order.instrument_id()).cloned()
        };
        let Some(instrument) = instrument else {
            log::error!(
                "Cannot execute order: instrument {} not found",
                order.instrument_id()
            );
            return Ok(());
        };

        let settings = match self.resolve_settings(&order, &instrument) {
            Ok(settings) => settings,
            Err(e) => {
                log::error!("Cannot execute order {primary_order_id}: {e}");
                return Ok(());
            }
        };

        let sequence = LimitChaserSequence {
            primary_order_id,
            instrument_id: order.instrument_id(),
            started_at_ns: self.core.clock().timestamp_ns(),
            limit_price,
            settings,
            working_order_id: None,
            last_reprice_ns: UnixNanos::default(),
            cancel_requested: false,
            pending_quantity: None,
            pending_reduce_primary: true,
        };

        self.sequences.insert(primary_order_id, sequence);
        self.instrument_sequences
            .entry(order.instrument_id())
            .or_default()
            .insert(primary_order_id);

        if self.subscribed_instruments.insert(order.instrument_id()) {
            self.subscribe_quotes(order.instrument_id(), None, None);
        }

        self.core.clock().set_timer(
            primary_order_id.as_str(),
            Duration::from_millis(settings.reprice_interval_ms),
            None,
            None,
            None,
            None,
            None,
        )?;

        self.refresh_sequence(primary_order_id, true)
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        self.refresh_sequence(ClientOrderId::new(event.name.as_str()), true)
    }

    fn on_order_accepted(&mut self, event: nautilus_model::events::OrderAccepted) {
        if let Err(e) = self.refresh_for_order(event.client_order_id, false) {
            log::error!(
                "Failed to refresh limit-chaser sequence after accept for {}: {e}",
                event.client_order_id
            );
        }
    }

    fn on_algo_order_filled(&mut self, event: nautilus_model::events::OrderFilled) {
        if let Err(e) = self.refresh_for_order(event.client_order_id, true) {
            log::error!(
                "Failed to refresh limit-chaser sequence after fill for {}: {e}",
                event.client_order_id
            );
        }
    }

    fn on_order_rejected(&mut self, event: nautilus_model::events::OrderRejected) {
        if let Err(e) = self.refresh_for_order(event.client_order_id, true) {
            log::error!(
                "Failed to refresh limit-chaser sequence after reject for {}: {e}",
                event.client_order_id
            );
        }
    }

    fn on_order_denied(&mut self, event: nautilus_model::events::OrderDenied) {
        if let Err(e) = self.refresh_for_order(event.client_order_id, true) {
            log::error!(
                "Failed to refresh limit-chaser sequence after denial for {}: {e}",
                event.client_order_id
            );
        }
    }

    fn on_algo_order_canceled(&mut self, event: nautilus_model::events::OrderCanceled) {
        let order = {
            let cache = self.core.cache();
            cache.order(&event.client_order_id).cloned()
        };
        let Some(order) = order else {
            return;
        };

        let primary_order_id = Self::primary_id_for_order(&order);

        if let Some(sequence) = self.sequences.get_mut(&primary_order_id)
            && order.client_order_id() == primary_order_id
        {
            sequence.cancel_requested = true;

            if let Some(working_order_id) = sequence.working_order_id
                && working_order_id != primary_order_id
            {
                let working_order = {
                    let cache = self.core.cache();
                    cache.order(&working_order_id).cloned()
                };
                if let Some(mut working_order) = working_order
                    && !working_order.is_closed()
                    && !working_order.is_pending_cancel()
                    && let Err(e) = self.cancel_order(&mut working_order, None)
                {
                    log::error!(
                        "Failed to cancel spawned working order {} after primary cancel: {e}",
                        working_order.client_order_id()
                    );
                    return;
                }
            }
        }

        if let Err(e) = self.refresh_sequence(primary_order_id, true) {
            log::error!(
                "Failed to refresh limit-chaser sequence after cancel for {}: {e}",
                event.client_order_id
            );
        }
    }

    fn on_order_expired(&mut self, event: nautilus_model::events::OrderExpired) {
        if let Err(e) = self.refresh_for_order(event.client_order_id, true) {
            log::error!(
                "Failed to refresh limit-chaser sequence after expiry for {}: {e}",
                event.client_order_id
            );
        }
    }

    fn on_order_modify_rejected(&mut self, event: nautilus_model::events::OrderModifyRejected) {
        if let Err(e) = self.refresh_for_order(event.client_order_id, true) {
            log::error!(
                "Failed to refresh limit-chaser sequence after modify reject for {}: {e}",
                event.client_order_id
            );
        }
    }

    fn on_order_cancel_rejected(&mut self, event: nautilus_model::events::OrderCancelRejected) {
        if let Err(e) = self.refresh_for_order(event.client_order_id, true) {
            log::error!(
                "Failed to refresh limit-chaser sequence after cancel reject for {}: {e}",
                event.client_order_id
            );
        }
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.core.clock().cancel_timers();
        Ok(())
    }

    fn on_reset(&mut self) -> anyhow::Result<()> {
        self.core.clock().cancel_timers();
        let instrument_ids: Vec<InstrumentId> =
            self.subscribed_instruments.iter().copied().collect();
        for instrument_id in instrument_ids {
            self.unsubscribe_quotes(instrument_id, None, None);
        }
        self.sequences.clear();
        self.instrument_sequences.clear();
        self.subscribed_instruments.clear();
        self.unsubscribe_all_strategy_events();
        self.core.reset();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        clock::{Clock, TestClock},
        component::Component,
        enums::ComponentTrigger,
    };
    use nautilus_core::UUID4;
    use nautilus_model::{
        data::stubs::quote_ethusdt_binance,
        identifiers::{StrategyId, TraderId},
        instruments::{InstrumentAny, stubs::crypto_perpetual_ethusdt},
        orders::{
            LimitOrder, OrderAny,
            stubs::{TestOrderEventStubs, TestOrderStubs},
        },
    };
    use rstest::rstest;

    use super::*;

    fn create_limit_chaser_algorithm() -> LimitChaserAlgorithm {
        let unique_id = format!("LIMIT_CHASER-{}", UUID4::new());
        let config = LimitChaserAlgorithmConfig {
            exec_algorithm_id: Some(ExecAlgorithmId::new(&unique_id)),
            ..Default::default()
        };
        LimitChaserAlgorithm::new(config)
    }

    fn register_algorithm(algo: &mut LimitChaserAlgorithm) -> Rc<RefCell<TestClock>> {
        use nautilus_common::timer::TimeEventCallback;

        let trader_id = TraderId::from("TRADER-001");
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        clock
            .borrow_mut()
            .register_default_handler(TimeEventCallback::Rust(std::sync::Arc::new(|_| {})));
        algo.core.register(trader_id, clock.clone(), cache).unwrap();
        algo.transition_state(ComponentTrigger::Initialize).unwrap();
        algo.transition_state(ComponentTrigger::Start).unwrap();
        algo.transition_state(ComponentTrigger::StartCompleted)
            .unwrap();
        clock
    }

    fn add_instrument_to_cache(algo: &mut LimitChaserAlgorithm) {
        let instrument = crypto_perpetual_ethusdt();
        let cache_rc = algo.core.cache_rc();
        let mut cache = cache_rc.borrow_mut();
        cache
            .add_instrument(InstrumentAny::CryptoPerpetual(instrument))
            .unwrap();
    }

    fn add_quote_to_cache(algo: &mut LimitChaserAlgorithm, quote: QuoteTick) {
        let cache_rc = algo.core.cache_rc();
        let mut cache = cache_rc.borrow_mut();
        cache.add_quote(quote).unwrap();
    }

    fn add_order_to_cache(algo: &mut LimitChaserAlgorithm, order: OrderAny) {
        let cache_rc = algo.core.cache_rc();
        let mut cache = cache_rc.borrow_mut();
        cache.add_order(order, None, None, false).unwrap();
    }

    fn create_primary_limit_order(
        exec_algorithm_id: ExecAlgorithmId,
        quantity: Quantity,
        price: Price,
        post_only: bool,
    ) -> OrderAny {
        OrderAny::Limit(LimitOrder::new(
            TraderId::from("TRADER-001"),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("ETHUSDT-PERP.BINANCE"),
            ClientOrderId::from("O-001"),
            nautilus_model::enums::OrderSide::Buy,
            quantity,
            price,
            TimeInForce::Gtc,
            None,
            post_only,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(exec_algorithm_id),
            None,
            None,
            None,
            UUID4::new(),
            0.into(),
        ))
    }

    #[rstest]
    fn test_limit_chaser_creation() {
        let algo = create_limit_chaser_algorithm();
        assert!(
            algo.core
                .exec_algorithm_id
                .inner()
                .starts_with("LIMIT_CHASER")
        );
        assert!(algo.sequences.is_empty());
    }

    #[rstest]
    fn test_limit_chaser_rejects_non_limit_orders() {
        let mut algo = create_limit_chaser_algorithm();
        register_algorithm(&mut algo);
        add_instrument_to_cache(&mut algo);
        add_quote_to_cache(&mut algo, quote_ethusdt_binance());

        let order = OrderAny::Market(nautilus_model::orders::MarketOrder::new(
            TraderId::from("TRADER-001"),
            StrategyId::from("STRAT-001"),
            InstrumentId::from("ETHUSDT-PERP.BINANCE"),
            ClientOrderId::from("O-001"),
            nautilus_model::enums::OrderSide::Buy,
            Quantity::from("1.0"),
            TimeInForce::Gtc,
            UUID4::new(),
            0.into(),
            false,
            false,
            None,
            None,
            None,
            None,
            Some(algo.id()),
            None,
            None,
            None,
        ));
        add_order_to_cache(&mut algo, order.clone());

        algo.on_order(order).unwrap();

        assert!(algo.sequences.is_empty());
    }

    #[rstest]
    fn test_limit_chaser_submits_primary_at_best_bid() {
        let mut algo = create_limit_chaser_algorithm();
        register_algorithm(&mut algo);
        add_instrument_to_cache(&mut algo);
        add_quote_to_cache(&mut algo, quote_ethusdt_binance());

        let order = create_primary_limit_order(
            algo.id(),
            Quantity::from("1.0"),
            Price::from("10005.0000"),
            false,
        );
        add_order_to_cache(&mut algo, order.clone());

        algo.on_order(order).unwrap();

        let cache = algo.core.cache();
        let primary = cache.order(&ClientOrderId::from("O-001")).unwrap();
        assert_eq!(primary.price(), Some(Price::from("10000.0000")));
        assert_eq!(
            algo.sequences
                .get(&ClientOrderId::from("O-001"))
                .and_then(|s| s.working_order_id),
            Some(ClientOrderId::from("O-001"))
        );
    }

    #[rstest]
    fn test_limit_chaser_spawns_child_and_reduces_primary_for_sliced_order() {
        let mut algo = create_limit_chaser_algorithm();
        register_algorithm(&mut algo);
        add_instrument_to_cache(&mut algo);
        add_quote_to_cache(&mut algo, quote_ethusdt_binance());

        let mut order = create_primary_limit_order(
            algo.id(),
            Quantity::from("2.0"),
            Price::from("10005.0000"),
            false,
        );
        if let OrderAny::Limit(ref mut limit) = order {
            limit.exec_algorithm_params = Some(indexmap::indexmap! {
                ustr::Ustr::from("max_child_quantity") => ustr::Ustr::from("1.0"),
            });
        }
        add_order_to_cache(&mut algo, order.clone());

        algo.on_order(order).unwrap();

        let cache = algo.core.cache();
        let primary = cache.order(&ClientOrderId::from("O-001")).unwrap();
        let child = cache.order(&ClientOrderId::from("O-001-E1")).unwrap();
        assert_eq!(primary.quantity(), Quantity::from("1.0"));
        assert_eq!(child.quantity(), Quantity::from("1.0"));
        assert_eq!(child.price(), Some(Price::from("10000.0000")));
        assert_eq!(
            algo.sequences
                .get(&ClientOrderId::from("O-001"))
                .and_then(|s| s.working_order_id),
            Some(ClientOrderId::from("O-001-E1"))
        );
    }

    #[rstest]
    fn test_limit_chaser_reprices_after_quote_move() {
        let mut algo = create_limit_chaser_algorithm();
        let clock = register_algorithm(&mut algo);
        add_instrument_to_cache(&mut algo);
        add_quote_to_cache(&mut algo, quote_ethusdt_binance());

        let order = create_primary_limit_order(
            algo.id(),
            Quantity::from("1.0"),
            Price::from("10005.0000"),
            false,
        );
        add_order_to_cache(&mut algo, order.clone());
        algo.on_order(order).unwrap();

        let accepted_primary = {
            let cache = algo.core.cache();
            let primary = cache.order(&ClientOrderId::from("O-001")).unwrap().clone();
            TestOrderStubs::make_accepted_order(&primary)
        };
        {
            let cache_rc = algo.core.cache_rc();
            let mut cache = cache_rc.borrow_mut();
            cache.update_order(&accepted_primary).unwrap();
        }
        algo.handle_order_event(accepted_primary.last_event().clone());

        let mut moved_quote = quote_ethusdt_binance();
        moved_quote.bid_price = Price::from("10002.0000");
        moved_quote.ask_price = Price::from("10003.0000");
        add_quote_to_cache(&mut algo, moved_quote);
        clock
            .borrow_mut()
            .advance_time(UnixNanos::from(300_000_000), true);

        algo.on_quote(&moved_quote).unwrap();

        let cache = algo.core.cache();
        let primary = cache.order(&ClientOrderId::from("O-001")).unwrap();
        assert_eq!(primary.price(), Some(Price::from("10000.0000")));
        assert!(primary.is_pending_update());
    }

    #[rstest]
    fn test_limit_chaser_turns_aggressive_after_deadline() {
        let mut algo = create_limit_chaser_algorithm();
        let clock = register_algorithm(&mut algo);
        add_instrument_to_cache(&mut algo);
        add_quote_to_cache(&mut algo, quote_ethusdt_binance());

        let mut order = create_primary_limit_order(
            algo.id(),
            Quantity::from("1.0"),
            Price::from("10005.0000"),
            false,
        );
        if let OrderAny::Limit(ref mut limit) = order {
            limit.exec_algorithm_params = Some(indexmap::indexmap! {
                ustr::Ustr::from("aggressive_after_secs") => ustr::Ustr::from("1.0"),
            });
        }
        add_order_to_cache(&mut algo, order.clone());
        algo.on_order(order).unwrap();

        clock
            .borrow_mut()
            .advance_time(UnixNanos::from(1_500_000_000), true);

        let mut moved_quote = quote_ethusdt_binance();
        moved_quote.bid_price = Price::from("10006.0000");
        moved_quote.ask_price = Price::from("10007.0000");
        add_quote_to_cache(&mut algo, moved_quote);
        let cache = algo.core.cache();
        let primary = cache.order(&ClientOrderId::from("O-001")).unwrap().clone();
        let sequence = algo
            .sequences
            .get(&ClientOrderId::from("O-001"))
            .unwrap()
            .clone();
        let target = algo
            .target_price(&primary, &moved_quote, &sequence)
            .unwrap();
        assert_eq!(target, Price::from("10005.0000"));
    }

    #[rstest]
    fn test_limit_chaser_resubmits_remaining_after_child_cancel() {
        let mut algo = create_limit_chaser_algorithm();
        register_algorithm(&mut algo);
        add_instrument_to_cache(&mut algo);
        add_quote_to_cache(&mut algo, quote_ethusdt_binance());

        let mut order = create_primary_limit_order(
            algo.id(),
            Quantity::from("2.0"),
            Price::from("10005.0000"),
            false,
        );
        if let OrderAny::Limit(ref mut limit) = order {
            limit.exec_algorithm_params = Some(indexmap::indexmap! {
                ustr::Ustr::from("max_child_quantity") => ustr::Ustr::from("1.0"),
            });
        }
        add_order_to_cache(&mut algo, order.clone());
        algo.on_order(order).unwrap();

        let accepted_child = {
            let cache = algo.core.cache();
            let child = cache
                .order(&ClientOrderId::from("O-001-E1"))
                .unwrap()
                .clone();
            TestOrderStubs::make_accepted_order(&child)
        };
        {
            let cache_rc = algo.core.cache_rc();
            let mut cache = cache_rc.borrow_mut();
            cache.update_order(&accepted_child).unwrap();
        }
        algo.handle_order_event(accepted_child.last_event().clone());

        let mut canceled_child = accepted_child.clone();
        let canceled = TestOrderEventStubs::canceled(
            &canceled_child,
            nautilus_model::identifiers::AccountId::from("SIM-001"),
            canceled_child.venue_order_id(),
        );
        canceled_child.apply(canceled.clone()).unwrap();
        {
            let cache_rc = algo.core.cache_rc();
            let mut cache = cache_rc.borrow_mut();
            cache.update_order(&canceled_child).unwrap();
        }
        algo.handle_order_event(canceled);

        let cache = algo.core.cache();
        let child2 = cache.order(&ClientOrderId::from("O-001-E2")).unwrap();
        assert_eq!(child2.quantity(), Quantity::from("1.0"));
    }
}
