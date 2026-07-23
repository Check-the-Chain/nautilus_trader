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

//! Pure quote-critical state mirroring for static-fee, zero-hook Uniswap v4 pools.

use std::collections::BTreeMap;

use alloy::{
    primitives::{Address, B256, I256, U160, aliases::I24, aliases::U24, keccak256},
    sol_types::SolValue,
};
use nautilus_model::defi::tick_map::{
    tick::PoolTick,
    tick_bitmap::TickBitmap,
    tick_math::{MAX_SQRT_RATIO, MIN_SQRT_RATIO, get_sqrt_ratio_at_tick, get_tick_at_sqrt_ratio},
};
use thiserror::Error;

use crate::{
    contracts::uniswap_v4_state_view::{UniswapV4PoolState, UniswapV4TickLiquidityState},
    events::{
        modify_liquidity::ModifyLiquidityEvent, protocol_fee_updated::ProtocolFeeUpdatedEvent,
        uniswap_v4_swap::UniswapV4SwapEvent,
    },
};

const DYNAMIC_FEE_FLAG: u32 = 0x80_0000;
const MAX_LP_FEE: u32 = 1_000_000;
const MAX_PROTOCOL_FEE: u32 = 1_000;
const FEE_DENOMINATOR: u64 = 1_000_000;
const PROTOCOL_FEE_MASK: u32 = 0x00ff_ffff;
const PROTOCOL_FEE_LANE_MASK: u32 = 0x0fff;
const MAX_TICK_SPACING: i32 = i16::MAX as i32;

/// Immutable identity and pool-key fields for a mirror.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UniswapV4MirrorConfig {
    pool_id: B256,
    currency0: Address,
    currency1: Address,
    tick_spacing: i32,
    static_lp_fee: u32,
    hooks: Address,
}

impl UniswapV4MirrorConfig {
    /// Creates a configuration for a static-fee pool without hooks.
    ///
    /// # Errors
    ///
    /// Returns an error for nonzero hooks, dynamic or invalid fees, or tick spacing outside the
    /// range accepted by Uniswap v4.
    pub fn new(
        pool_id: B256,
        currency0: Address,
        currency1: Address,
        tick_spacing: i32,
        static_lp_fee: u32,
        hooks: Address,
    ) -> Result<Self, UniswapV4MirrorError> {
        validate_tick_spacing(tick_spacing)?;
        if currency0 >= currency1 {
            return Err(UniswapV4MirrorError::CurrenciesOutOfOrder {
                currency0,
                currency1,
            });
        }
        if hooks != Address::ZERO {
            return Err(UniswapV4MirrorError::NonzeroHooks { hooks });
        }
        if static_lp_fee & DYNAMIC_FEE_FLAG != 0 {
            return Err(UniswapV4MirrorError::DynamicFeePool { fee: static_lp_fee });
        }
        if static_lp_fee > MAX_LP_FEE {
            return Err(UniswapV4MirrorError::InvalidStaticLpFee { fee: static_lp_fee });
        }

        let encoded_tick_spacing = I24::try_from(tick_spacing)
            .map_err(|_| UniswapV4MirrorError::InvalidTickSpacing { tick_spacing })?;
        let calculated_pool_id = keccak256(
            (
                currency0,
                currency1,
                U24::from(static_lp_fee),
                encoded_tick_spacing,
                hooks,
            )
                .abi_encode(),
        );
        if calculated_pool_id != pool_id {
            return Err(UniswapV4MirrorError::PoolKeyMismatch {
                expected: pool_id,
                calculated: calculated_pool_id,
            });
        }

        Ok(Self {
            pool_id,
            currency0,
            currency1,
            tick_spacing,
            static_lp_fee,
            hooks,
        })
    }

    #[cfg(test)]
    pub(crate) const fn new_unchecked_for_test(
        pool_id: B256,
        tick_spacing: i32,
        static_lp_fee: u32,
    ) -> Self {
        Self {
            pool_id,
            currency0: Address::ZERO,
            currency1: Address::ZERO,
            tick_spacing,
            static_lp_fee,
            hooks: Address::ZERO,
        }
    }

    #[must_use]
    pub const fn pool_id(&self) -> B256 {
        self.pool_id
    }

    #[must_use]
    pub const fn currency0(&self) -> Address {
        self.currency0
    }

    #[must_use]
    pub const fn currency1(&self) -> Address {
        self.currency1
    }

    #[must_use]
    pub const fn tick_spacing(&self) -> i32 {
        self.tick_spacing
    }

    #[must_use]
    pub const fn static_lp_fee(&self) -> u32 {
        self.static_lp_fee
    }

    #[must_use]
    pub const fn hooks(&self) -> Address {
        self.hooks
    }
}

/// Lexicographic EVM log position used to reject gaps, duplicates, and reordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct UniswapV4EventPosition {
    pub block_number: u64,
    pub transaction_index: u32,
    pub log_index: u32,
}

impl UniswapV4EventPosition {
    #[must_use]
    pub const fn new(block_number: u64, transaction_index: u32, log_index: u32) -> Self {
        Self {
            block_number,
            transaction_index,
            log_index,
        }
    }

    const fn end_of_block(block_number: u64) -> Self {
        Self::new(block_number, u32::MAX, u32::MAX)
    }
}

/// Quote-relevant state for one initialized tick.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UniswapV4MirrorTick {
    pub liquidity_gross: u128,
    pub liquidity_net: i128,
}

/// A supported event borrowed for dispatch through [`UniswapV4Mirror::apply`].
#[derive(Debug, Clone, Copy)]
pub enum UniswapV4MirrorEvent<'a> {
    Swap(&'a UniswapV4SwapEvent),
    ModifyLiquidity(&'a ModifyLiquidityEvent),
    ProtocolFeeUpdated(&'a ProtocolFeeUpdatedEvent),
}

/// Fail-closed mirror construction, event application, and validation errors.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum UniswapV4MirrorError {
    #[error("tick spacing must be in 1..={MAX_TICK_SPACING}, was {tick_spacing}")]
    InvalidTickSpacing { tick_spacing: i32 },
    #[error("PoolKey currencies must be strictly ordered: {currency0} >= {currency1}")]
    CurrenciesOutOfOrder {
        currency0: Address,
        currency1: Address,
    },
    #[error("PoolKey hash mismatch: expected {expected}, calculated {calculated}")]
    PoolKeyMismatch { expected: B256, calculated: B256 },
    #[error("zero-hook mirror cannot use hooks address {hooks}")]
    NonzeroHooks { hooks: Address },
    #[error("dynamic LP fee flag is unsupported for fee {fee}")]
    DynamicFeePool { fee: u32 },
    #[error("static LP fee exceeds 100%: {fee}")]
    InvalidStaticLpFee { fee: u32 },
    #[error("StateView pool is uninitialized")]
    UninitializedPool,
    #[error("StateView pool ID mismatch: expected {expected}, received {actual}")]
    PoolIdMismatch { expected: B256, actual: B256 },
    #[error("StateView tick spacing mismatch: expected {expected}, received {actual}")]
    TickSpacingMismatch { expected: i32, actual: i32 },
    #[error("StateView LP fee mismatch: expected static fee {expected}, received {actual}")]
    StaticLpFeeMismatch { expected: u32, actual: u32 },
    #[error("invalid packed protocol fee {protocol_fee:#08x}")]
    InvalidProtocolFee { protocol_fee: u32 },
    #[error("invalid stored tick {tick}")]
    InvalidStoredTick { tick: i32 },
    #[error("invalid initialized tick {tick}: {reason}")]
    InvalidTickState { tick: i32, reason: &'static str },
    #[error("event belongs to pool {actual}, expected {expected}")]
    WrongPool { expected: B256, actual: B256 },
    #[error("event position {position:?} is not after watermark {watermark:?}")]
    NonIncreasingPosition {
        watermark: UniswapV4EventPosition,
        position: UniswapV4EventPosition,
    },
    #[error("ambiguous Swap deltas: amount0={amount0}, amount1={amount1}")]
    AmbiguousSwapDirection { amount0: I256, amount1: I256 },
    #[error("Swap effective fee mismatch: expected {expected}, received {actual}")]
    EffectiveFeeMismatch { expected: u32, actual: u32 },
    #[error("invalid Swap sqrt price: zero denotes an uninitialized pool")]
    InvalidSwapSqrtPrice,
    #[error("invalid liquidity range [{tick_lower}, {tick_upper}): {reason}")]
    InvalidTickRange {
        tick_lower: i32,
        tick_upper: i32,
        reason: &'static str,
    },
    #[error("liquidity delta does not fit int128: {delta}")]
    LiquidityDeltaOutOfRange { delta: I256 },
    #[error("liquidity arithmetic failed at tick {tick} for {field}")]
    LiquidityArithmetic { tick: i32, field: &'static str },
    #[error("liquidity gross at tick {tick} exceeds per-tick maximum {maximum}: {actual}")]
    TickLiquidityLimit {
        tick: i32,
        maximum: u128,
        actual: u128,
    },
    #[error("active liquidity arithmetic failed")]
    ActiveLiquidityArithmetic,
    #[error("snapshot block {snapshot_block} does not match watermark block {watermark_block}")]
    SnapshotBlockMismatch {
        watermark_block: u64,
        snapshot_block: u64,
    },
    #[error("snapshot differs from mirror in {field}")]
    SnapshotStateMismatch { field: &'static str },
}

/// Pure state machine containing only state needed by a Uniswap v4 quote implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniswapV4Mirror {
    config: UniswapV4MirrorConfig,
    sqrt_price_x96: U160,
    tick: i32,
    liquidity: u128,
    protocol_fee: u32,
    lp_fee: u32,
    ticks: BTreeMap<i32, UniswapV4MirrorTick>,
    tick_bitmap: TickBitmap,
    watermark: UniswapV4EventPosition,
}

impl UniswapV4Mirror {
    /// Bootstraps a mirror at the end of the snapshot's exact block.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot does not describe the configured initialized,
    /// static-fee, zero-hook pool or contains malformed quote state.
    pub fn bootstrap(
        config: UniswapV4MirrorConfig,
        snapshot: &UniswapV4PoolState,
    ) -> Result<Self, UniswapV4MirrorError> {
        validate_snapshot(&config, snapshot)?;

        let mut ticks = BTreeMap::new();
        let mut tick_bitmap = TickBitmap::new(config.tick_spacing as u32);
        for tick in &snapshot.ticks {
            ticks.insert(
                tick.tick,
                UniswapV4MirrorTick {
                    liquidity_gross: tick.liquidity_gross,
                    liquidity_net: tick.liquidity_net,
                },
            );
            tick_bitmap.flip_tick(tick.tick);
        }

        Ok(Self {
            config,
            sqrt_price_x96: snapshot.slot0.sqrt_price_x96,
            tick: snapshot.slot0.tick,
            liquidity: snapshot.liquidity,
            protocol_fee: snapshot.slot0.protocol_fee,
            lp_fee: snapshot.slot0.lp_fee,
            ticks,
            tick_bitmap,
            watermark: UniswapV4EventPosition::end_of_block(snapshot.block_number),
        })
    }

    #[must_use]
    pub const fn config(&self) -> &UniswapV4MirrorConfig {
        &self.config
    }

    #[must_use]
    pub const fn sqrt_price_x96(&self) -> U160 {
        self.sqrt_price_x96
    }

    #[must_use]
    pub const fn tick(&self) -> i32 {
        self.tick
    }

    #[must_use]
    pub const fn liquidity(&self) -> u128 {
        self.liquidity
    }

    #[must_use]
    pub const fn protocol_fee(&self) -> u32 {
        self.protocol_fee
    }

    #[must_use]
    pub const fn lp_fee(&self) -> u32 {
        self.lp_fee
    }

    #[must_use]
    pub const fn watermark(&self) -> UniswapV4EventPosition {
        self.watermark
    }

    #[must_use]
    pub fn ticks(&self) -> &BTreeMap<i32, UniswapV4MirrorTick> {
        &self.ticks
    }

    #[must_use]
    pub fn tick_liquidity(&self, tick: i32) -> Option<&UniswapV4MirrorTick> {
        self.ticks.get(&tick)
    }

    #[must_use]
    pub(crate) const fn tick_bitmap(&self) -> &TickBitmap {
        &self.tick_bitmap
    }

    /// Dispatches one supported event.
    ///
    /// # Errors
    ///
    /// Returns an error without changing any state if the event fails validation.
    pub fn apply(&mut self, event: UniswapV4MirrorEvent<'_>) -> Result<(), UniswapV4MirrorError> {
        match event {
            UniswapV4MirrorEvent::Swap(event) => self.apply_swap(event),
            UniswapV4MirrorEvent::ModifyLiquidity(event) => self.apply_modify_liquidity(event),
            UniswapV4MirrorEvent::ProtocolFeeUpdated(event) => {
                self.apply_protocol_fee_updated(event)
            }
        }
    }

    /// Applies the authoritative post-swap price, stored tick, and active liquidity.
    ///
    /// # Errors
    ///
    /// Returns an error without changing state for a wrong pool, stale position, ambiguous deltas,
    /// malformed state, or an effective fee inconsistent with this static pool.
    pub fn apply_swap(&mut self, event: &UniswapV4SwapEvent) -> Result<(), UniswapV4MirrorError> {
        let position = event_position(event.block_number, event.transaction_index, event.log_index);
        self.validate_event_header(event.pool_id, position)?;

        let zero_for_one = if event.amount0 < I256::ZERO && event.amount1 >= I256::ZERO {
            true
        } else if event.amount1 < I256::ZERO && event.amount0 >= I256::ZERO {
            false
        } else if event.amount0 == I256::ZERO
            && event.amount1 == I256::ZERO
            && event.sqrt_price_x96 < self.sqrt_price_x96
        {
            true
        } else if event.amount0 == I256::ZERO
            && event.amount1 == I256::ZERO
            && event.sqrt_price_x96 > self.sqrt_price_x96
        {
            false
        } else {
            return Err(UniswapV4MirrorError::AmbiguousSwapDirection {
                amount0: event.amount0,
                amount1: event.amount1,
            });
        };
        let directional_protocol_fee = if zero_for_one {
            self.protocol_fee & PROTOCOL_FEE_LANE_MASK
        } else {
            (self.protocol_fee >> 12) & PROTOCOL_FEE_LANE_MASK
        };
        let expected_fee = effective_swap_fee(self.lp_fee, directional_protocol_fee);
        if event.fee != expected_fee {
            return Err(UniswapV4MirrorError::EffectiveFeeMismatch {
                expected: expected_fee,
                actual: event.fee,
            });
        }
        if !valid_price_and_tick(event.sqrt_price_x96, event.tick) {
            return Err(UniswapV4MirrorError::InvalidSwapSqrtPrice);
        }

        self.sqrt_price_x96 = event.sqrt_price_x96;
        self.tick = event.tick;
        self.liquidity = event.liquidity;
        self.watermark = position;
        Ok(())
    }

    /// Applies aggregate gross/net tick liquidity and current in-range liquidity.
    ///
    /// This intentionally does not track positions or fee growth. A zero delta is accepted as a
    /// poke after validating its range and advances only the watermark.
    ///
    /// # Errors
    ///
    /// Returns an error without changing state for invalid identity/order/range, an int256 delta
    /// outside int128, or any gross, net, active, or per-tick liquidity arithmetic failure.
    pub fn apply_modify_liquidity(
        &mut self,
        event: &ModifyLiquidityEvent,
    ) -> Result<(), UniswapV4MirrorError> {
        let position = event_position(event.block_number, event.transaction_index, event.log_index);
        self.validate_event_header(event.pool_id, position)?;
        validate_tick_range(event.tick_lower, event.tick_upper, self.config.tick_spacing)?;
        let delta = i128::try_from(event.liquidity_delta).map_err(|_| {
            UniswapV4MirrorError::LiquidityDeltaOutOfRange {
                delta: event.liquidity_delta,
            }
        })?;

        if delta == 0 {
            self.watermark = position;
            return Ok(());
        }

        let lower_before = self
            .ticks
            .get(&event.tick_lower)
            .copied()
            .unwrap_or_default();
        let upper_before = self
            .ticks
            .get(&event.tick_upper)
            .copied()
            .unwrap_or_default();
        let lower_after = update_tick(event.tick_lower, lower_before, delta, false)?;
        let upper_after = update_tick(event.tick_upper, upper_before, delta, true)?;

        if delta > 0 {
            let maximum = max_liquidity_per_tick(self.config.tick_spacing);
            validate_tick_liquidity_limit(event.tick_lower, lower_after, maximum)?;
            validate_tick_liquidity_limit(event.tick_upper, upper_after, maximum)?;
        }

        let liquidity_after = if self.tick >= event.tick_lower && self.tick < event.tick_upper {
            add_liquidity_delta(self.liquidity, delta)
                .ok_or(UniswapV4MirrorError::ActiveLiquidityArithmetic)?
        } else {
            self.liquidity
        };

        if (lower_before.liquidity_gross == 0) != (lower_after.liquidity_gross == 0) {
            self.tick_bitmap.flip_tick(event.tick_lower);
        }
        if (upper_before.liquidity_gross == 0) != (upper_after.liquidity_gross == 0) {
            self.tick_bitmap.flip_tick(event.tick_upper);
        }
        set_tick(&mut self.ticks, event.tick_lower, lower_after);
        set_tick(&mut self.ticks, event.tick_upper, upper_after);
        self.liquidity = liquidity_after;
        self.watermark = position;
        Ok(())
    }

    /// Applies a validated packed directional protocol fee.
    ///
    /// # Errors
    ///
    /// Returns an error without changing state for a wrong pool, stale position, high bits, or a
    /// 12-bit directional lane greater than 1000 pips.
    pub fn apply_protocol_fee_updated(
        &mut self,
        event: &ProtocolFeeUpdatedEvent,
    ) -> Result<(), UniswapV4MirrorError> {
        let position = event_position(event.block_number, event.transaction_index, event.log_index);
        self.validate_event_header(event.pool_id, position)?;
        validate_protocol_fee(event.protocol_fee)?;

        self.protocol_fee = event.protocol_fee;
        self.watermark = position;
        Ok(())
    }

    /// Validates exact quote-state equality against a fresh StateView snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed or mismatched snapshot metadata, a different block, or any
    /// difference in price, tick, liquidity, fees, or initialized ticks.
    pub fn validate_against_snapshot(
        &self,
        snapshot: &UniswapV4PoolState,
    ) -> Result<(), UniswapV4MirrorError> {
        validate_snapshot(&self.config, snapshot)?;
        if snapshot.block_number != self.watermark.block_number {
            return Err(UniswapV4MirrorError::SnapshotBlockMismatch {
                watermark_block: self.watermark.block_number,
                snapshot_block: snapshot.block_number,
            });
        }
        if snapshot.slot0.sqrt_price_x96 != self.sqrt_price_x96 {
            return Err(UniswapV4MirrorError::SnapshotStateMismatch {
                field: "sqrt_price_x96",
            });
        }
        if snapshot.slot0.tick != self.tick {
            return Err(UniswapV4MirrorError::SnapshotStateMismatch { field: "tick" });
        }
        if snapshot.liquidity != self.liquidity {
            return Err(UniswapV4MirrorError::SnapshotStateMismatch { field: "liquidity" });
        }
        if snapshot.slot0.protocol_fee != self.protocol_fee {
            return Err(UniswapV4MirrorError::SnapshotStateMismatch {
                field: "protocol_fee",
            });
        }
        if snapshot.slot0.lp_fee != self.lp_fee {
            return Err(UniswapV4MirrorError::SnapshotStateMismatch { field: "lp_fee" });
        }

        let snapshot_ticks = snapshot
            .ticks
            .iter()
            .map(|tick| {
                (
                    tick.tick,
                    UniswapV4MirrorTick {
                        liquidity_gross: tick.liquidity_gross,
                        liquidity_net: tick.liquidity_net,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        if snapshot_ticks != self.ticks {
            return Err(UniswapV4MirrorError::SnapshotStateMismatch { field: "ticks" });
        }
        Ok(())
    }

    fn validate_event_header(
        &self,
        pool_id: B256,
        position: UniswapV4EventPosition,
    ) -> Result<(), UniswapV4MirrorError> {
        if pool_id != self.config.pool_id {
            return Err(UniswapV4MirrorError::WrongPool {
                expected: self.config.pool_id,
                actual: pool_id,
            });
        }
        if position <= self.watermark {
            return Err(UniswapV4MirrorError::NonIncreasingPosition {
                watermark: self.watermark,
                position,
            });
        }
        Ok(())
    }
}

fn validate_tick_spacing(tick_spacing: i32) -> Result<(), UniswapV4MirrorError> {
    if !(1..=MAX_TICK_SPACING).contains(&tick_spacing) {
        return Err(UniswapV4MirrorError::InvalidTickSpacing { tick_spacing });
    }
    Ok(())
}

fn validate_snapshot(
    config: &UniswapV4MirrorConfig,
    snapshot: &UniswapV4PoolState,
) -> Result<(), UniswapV4MirrorError> {
    if snapshot.pool_id != config.pool_id {
        return Err(UniswapV4MirrorError::PoolIdMismatch {
            expected: config.pool_id,
            actual: snapshot.pool_id,
        });
    }
    if snapshot.tick_spacing != config.tick_spacing {
        return Err(UniswapV4MirrorError::TickSpacingMismatch {
            expected: config.tick_spacing,
            actual: snapshot.tick_spacing,
        });
    }
    if snapshot.slot0.lp_fee != config.static_lp_fee {
        return Err(UniswapV4MirrorError::StaticLpFeeMismatch {
            expected: config.static_lp_fee,
            actual: snapshot.slot0.lp_fee,
        });
    }
    if snapshot.slot0.sqrt_price_x96 == U160::ZERO {
        return Err(UniswapV4MirrorError::UninitializedPool);
    }
    if !valid_price_and_tick(snapshot.slot0.sqrt_price_x96, snapshot.slot0.tick) {
        return Err(UniswapV4MirrorError::SnapshotStateMismatch {
            field: "slot0 sqrt price/tick",
        });
    }
    validate_protocol_fee(snapshot.slot0.protocol_fee)?;

    let maximum = max_liquidity_per_tick(config.tick_spacing);
    let mut previous_tick = None;
    for tick in &snapshot.ticks {
        if previous_tick.is_some_and(|previous| tick.tick <= previous) {
            return Err(UniswapV4MirrorError::InvalidTickState {
                tick: tick.tick,
                reason: "ticks must be strictly increasing",
            });
        }
        validate_initialized_tick(tick, config.tick_spacing, maximum)?;
        previous_tick = Some(tick.tick);
    }
    Ok(())
}

fn valid_price_and_tick(sqrt_price_x96: U160, tick: i32) -> bool {
    if sqrt_price_x96 < MIN_SQRT_RATIO
        || sqrt_price_x96 >= MAX_SQRT_RATIO
        || !(PoolTick::MIN_TICK..=PoolTick::MAX_TICK).contains(&tick)
    {
        return false;
    }

    let price_tick = get_tick_at_sqrt_ratio(sqrt_price_x96);
    tick == price_tick
        || (price_tick.checked_sub(1) == Some(tick)
            && sqrt_price_x96 == get_sqrt_ratio_at_tick(price_tick))
}

fn validate_initialized_tick(
    tick: &UniswapV4TickLiquidityState,
    tick_spacing: i32,
    maximum: u128,
) -> Result<(), UniswapV4MirrorError> {
    if !(PoolTick::MIN_TICK..=PoolTick::MAX_TICK).contains(&tick.tick) {
        return Err(UniswapV4MirrorError::InvalidTickState {
            tick: tick.tick,
            reason: "outside Uniswap tick bounds",
        });
    }
    if tick.tick % tick_spacing != 0 {
        return Err(UniswapV4MirrorError::InvalidTickState {
            tick: tick.tick,
            reason: "not aligned to tick spacing",
        });
    }
    if tick.liquidity_gross == 0 {
        return Err(UniswapV4MirrorError::InvalidTickState {
            tick: tick.tick,
            reason: "initialized tick has zero gross liquidity",
        });
    }
    if tick.liquidity_gross > maximum {
        return Err(UniswapV4MirrorError::TickLiquidityLimit {
            tick: tick.tick,
            maximum,
            actual: tick.liquidity_gross,
        });
    }
    Ok(())
}

fn validate_protocol_fee(protocol_fee: u32) -> Result<(), UniswapV4MirrorError> {
    let zero_for_one = protocol_fee & PROTOCOL_FEE_LANE_MASK;
    let one_for_zero = (protocol_fee >> 12) & PROTOCOL_FEE_LANE_MASK;
    if protocol_fee & !PROTOCOL_FEE_MASK != 0
        || zero_for_one > MAX_PROTOCOL_FEE
        || one_for_zero > MAX_PROTOCOL_FEE
    {
        return Err(UniswapV4MirrorError::InvalidProtocolFee { protocol_fee });
    }
    Ok(())
}

fn effective_swap_fee(lp_fee: u32, protocol_fee: u32) -> u32 {
    let lp_fee = u64::from(lp_fee);
    let protocol_fee = u64::from(protocol_fee);
    u32::try_from(protocol_fee + lp_fee - protocol_fee * lp_fee / FEE_DENOMINATOR)
        .expect("validated fees always fit u32")
}

fn validate_tick_range(
    tick_lower: i32,
    tick_upper: i32,
    tick_spacing: i32,
) -> Result<(), UniswapV4MirrorError> {
    let reason = if tick_lower >= tick_upper {
        Some("lower tick must be below upper tick")
    } else if tick_lower < PoolTick::MIN_TICK || tick_upper > PoolTick::MAX_TICK {
        Some("range is outside Uniswap tick bounds")
    } else if tick_lower % tick_spacing != 0 || tick_upper % tick_spacing != 0 {
        Some("ticks are not aligned to tick spacing")
    } else {
        None
    };
    if let Some(reason) = reason {
        return Err(UniswapV4MirrorError::InvalidTickRange {
            tick_lower,
            tick_upper,
            reason,
        });
    }
    Ok(())
}

fn update_tick(
    tick: i32,
    before: UniswapV4MirrorTick,
    delta: i128,
    upper: bool,
) -> Result<UniswapV4MirrorTick, UniswapV4MirrorError> {
    let liquidity_gross = add_liquidity_delta(before.liquidity_gross, delta).ok_or(
        UniswapV4MirrorError::LiquidityArithmetic {
            tick,
            field: "liquidity_gross",
        },
    )?;
    let liquidity_net = if upper {
        before.liquidity_net.checked_sub(delta)
    } else {
        before.liquidity_net.checked_add(delta)
    }
    .ok_or(UniswapV4MirrorError::LiquidityArithmetic {
        tick,
        field: "liquidity_net",
    })?;

    Ok(UniswapV4MirrorTick {
        liquidity_gross,
        liquidity_net,
    })
}

fn add_liquidity_delta(liquidity: u128, delta: i128) -> Option<u128> {
    if delta < 0 {
        liquidity.checked_sub(delta.unsigned_abs())
    } else {
        liquidity.checked_add(delta as u128)
    }
}

fn max_liquidity_per_tick(tick_spacing: i32) -> u128 {
    let min_tick = PoolTick::MIN_TICK.div_euclid(tick_spacing);
    let max_tick = PoolTick::MAX_TICK / tick_spacing;
    let number_of_ticks = u128::try_from(max_tick - min_tick + 1)
        .expect("validated tick spacing produces a positive tick count");
    u128::MAX / number_of_ticks
}

fn validate_tick_liquidity_limit(
    tick: i32,
    state: UniswapV4MirrorTick,
    maximum: u128,
) -> Result<(), UniswapV4MirrorError> {
    if state.liquidity_gross > maximum {
        return Err(UniswapV4MirrorError::TickLiquidityLimit {
            tick,
            maximum,
            actual: state.liquidity_gross,
        });
    }
    Ok(())
}

fn set_tick(ticks: &mut BTreeMap<i32, UniswapV4MirrorTick>, tick: i32, state: UniswapV4MirrorTick) {
    if state.liquidity_gross == 0 {
        ticks.remove(&tick);
    } else {
        ticks.insert(tick, state);
    }
}

const fn event_position(
    block_number: u64,
    transaction_index: u32,
    log_index: u32,
) -> UniswapV4EventPosition {
    UniswapV4EventPosition::new(block_number, transaction_index, log_index)
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, I256, U160};

    use super::*;
    use crate::{contracts::uniswap_v4_state_view::UniswapV4Slot0State, exchanges::base};

    const BOOTSTRAP_BLOCK: u64 = 10;
    const TICK_SPACING: i32 = 60;
    const STATIC_LP_FEE: u32 = 3_000;

    fn currency0() -> Address {
        Address::repeat_byte(0x11)
    }

    fn currency1() -> Address {
        Address::repeat_byte(0x22)
    }

    fn pool_id() -> B256 {
        keccak256(
            (
                currency0(),
                currency1(),
                U24::from(STATIC_LP_FEE),
                I24::try_from(TICK_SPACING).unwrap(),
                Address::ZERO,
            )
                .abi_encode(),
        )
    }

    fn config() -> UniswapV4MirrorConfig {
        UniswapV4MirrorConfig::new(
            pool_id(),
            currency0(),
            currency1(),
            TICK_SPACING,
            STATIC_LP_FEE,
            Address::ZERO,
        )
        .unwrap()
    }

    fn snapshot() -> UniswapV4PoolState {
        UniswapV4PoolState {
            pool_id: pool_id(),
            tick_spacing: TICK_SPACING,
            block_number: BOOTSTRAP_BLOCK,
            slot0: UniswapV4Slot0State {
                sqrt_price_x96: U160::from(1_u8) << 96,
                tick: 0,
                protocol_fee: (200 << 12) | 100,
                lp_fee: STATIC_LP_FEE,
            },
            liquidity: 1_000,
            ticks: vec![
                UniswapV4TickLiquidityState {
                    tick: -120,
                    liquidity_gross: 1_000,
                    liquidity_net: 1_000,
                },
                UniswapV4TickLiquidityState {
                    tick: 120,
                    liquidity_gross: 1_000,
                    liquidity_net: -1_000,
                },
            ],
        }
    }

    fn mirror() -> UniswapV4Mirror {
        UniswapV4Mirror::bootstrap(config(), &snapshot()).unwrap()
    }

    fn swap_event(
        pool_id: B256,
        position: UniswapV4EventPosition,
        amount0: i128,
        amount1: i128,
        fee: u32,
    ) -> UniswapV4SwapEvent {
        UniswapV4SwapEvent {
            dex: base::UNISWAP_V4.dex.clone(),
            pool_id,
            block_number: position.block_number,
            transaction_hash: "0xswap".to_string(),
            transaction_index: position.transaction_index,
            log_index: position.log_index,
            sender: Address::repeat_byte(0x22),
            amount0: I256::try_from(amount0).unwrap(),
            amount1: I256::try_from(amount1).unwrap(),
            sqrt_price_x96: U160::from(2_u8) << 96,
            liquidity: 2_000,
            tick: 13_863,
            fee,
        }
    }

    fn modify_event(
        pool_id: B256,
        position: UniswapV4EventPosition,
        tick_lower: i32,
        tick_upper: i32,
        delta: I256,
    ) -> ModifyLiquidityEvent {
        ModifyLiquidityEvent {
            dex: base::UNISWAP_V4.dex.clone(),
            pool_id,
            block_number: position.block_number,
            transaction_hash: "0xmodify".to_string(),
            transaction_index: position.transaction_index,
            log_index: position.log_index,
            sender: Address::repeat_byte(0x22),
            tick_lower,
            tick_upper,
            liquidity_delta: delta,
            salt: B256::ZERO,
        }
    }

    fn protocol_event(
        pool_id: B256,
        position: UniswapV4EventPosition,
        protocol_fee: u32,
    ) -> ProtocolFeeUpdatedEvent {
        ProtocolFeeUpdatedEvent {
            dex: base::UNISWAP_V4.dex.clone(),
            pool_id,
            block_number: position.block_number,
            transaction_hash: "0xprotocol".to_string(),
            transaction_index: position.transaction_index,
            log_index: position.log_index,
            protocol_fee,
        }
    }

    fn position(transaction_index: u32, log_index: u32) -> UniswapV4EventPosition {
        UniswapV4EventPosition::new(BOOTSTRAP_BLOCK + 1, transaction_index, log_index)
    }

    fn i256(value: i128) -> I256 {
        I256::try_from(value).unwrap()
    }

    fn state_view_from_mirror(mirror: &UniswapV4Mirror) -> UniswapV4PoolState {
        UniswapV4PoolState {
            pool_id: mirror.config.pool_id,
            tick_spacing: mirror.config.tick_spacing,
            block_number: mirror.watermark.block_number,
            slot0: UniswapV4Slot0State {
                sqrt_price_x96: mirror.sqrt_price_x96,
                tick: mirror.tick,
                protocol_fee: mirror.protocol_fee,
                lp_fee: mirror.lp_fee,
            },
            liquidity: mirror.liquidity,
            ticks: mirror
                .ticks
                .iter()
                .map(|(&tick, state)| UniswapV4TickLiquidityState {
                    tick,
                    liquidity_gross: state.liquidity_gross,
                    liquidity_net: state.liquidity_net,
                })
                .collect(),
        }
    }

    #[test]
    fn rejects_unsupported_configs_and_mismatched_bootstrap() {
        assert!(matches!(
            UniswapV4MirrorConfig::new(
                pool_id(),
                currency1(),
                currency0(),
                TICK_SPACING,
                STATIC_LP_FEE,
                Address::ZERO,
            ),
            Err(UniswapV4MirrorError::CurrenciesOutOfOrder { .. })
        ));
        assert!(matches!(
            UniswapV4MirrorConfig::new(
                B256::ZERO,
                currency0(),
                currency1(),
                TICK_SPACING,
                STATIC_LP_FEE,
                Address::ZERO,
            ),
            Err(UniswapV4MirrorError::PoolKeyMismatch { .. })
        ));
        assert!(matches!(
            UniswapV4MirrorConfig::new(
                pool_id(),
                currency0(),
                currency1(),
                0,
                STATIC_LP_FEE,
                Address::ZERO,
            ),
            Err(UniswapV4MirrorError::InvalidTickSpacing { .. })
        ));
        assert!(matches!(
            UniswapV4MirrorConfig::new(
                pool_id(),
                currency0(),
                currency1(),
                TICK_SPACING,
                DYNAMIC_FEE_FLAG,
                Address::ZERO,
            ),
            Err(UniswapV4MirrorError::DynamicFeePool { .. })
        ));
        assert!(matches!(
            UniswapV4MirrorConfig::new(
                pool_id(),
                currency0(),
                currency1(),
                TICK_SPACING,
                STATIC_LP_FEE,
                Address::repeat_byte(1)
            ),
            Err(UniswapV4MirrorError::NonzeroHooks { .. })
        ));

        let mut state = snapshot();
        state.slot0.sqrt_price_x96 = U160::ZERO;
        assert!(matches!(
            UniswapV4Mirror::bootstrap(config(), &state),
            Err(UniswapV4MirrorError::UninitializedPool)
        ));
        state = snapshot();
        state.pool_id = B256::repeat_byte(2);
        assert!(matches!(
            UniswapV4Mirror::bootstrap(config(), &state),
            Err(UniswapV4MirrorError::PoolIdMismatch { .. })
        ));
        state = snapshot();
        state.tick_spacing = 10;
        assert!(matches!(
            UniswapV4Mirror::bootstrap(config(), &state),
            Err(UniswapV4MirrorError::TickSpacingMismatch { .. })
        ));
        state = snapshot();
        state.slot0.lp_fee += 1;
        assert!(matches!(
            UniswapV4Mirror::bootstrap(config(), &state),
            Err(UniswapV4MirrorError::StaticLpFeeMismatch { .. })
        ));
    }

    #[test]
    fn bootstrap_sets_end_of_block_watermark_and_sorted_ticks() {
        let mirror = mirror();
        assert_eq!(
            mirror.watermark(),
            UniswapV4EventPosition::new(BOOTSTRAP_BLOCK, u32::MAX, u32::MAX)
        );
        assert_eq!(
            mirror.ticks().keys().copied().collect::<Vec<_>>(),
            [-120, 120]
        );
        assert_eq!(mirror.config().hooks(), Address::ZERO);
        assert!(mirror.tick_bitmap.is_initialized(-120));
        assert!(mirror.tick_bitmap.is_initialized(120));
        assert!(!mirror.tick_bitmap.is_initialized(-60));
    }

    #[test]
    fn swap_uses_directional_effective_fee_and_authoritative_state() {
        let mut mirror = mirror();
        assert_eq!(effective_swap_fee(500_000, 1_000), 500_500);
        let zero_for_one_fee = effective_swap_fee(STATIC_LP_FEE, 100);
        let event = swap_event(pool_id(), position(0, 0), -10, 9, zero_for_one_fee);
        mirror.apply_swap(&event).unwrap();
        assert_eq!(mirror.sqrt_price_x96(), event.sqrt_price_x96);
        assert_eq!(mirror.tick(), event.tick);
        assert_eq!(mirror.liquidity(), event.liquidity);

        let one_for_zero_fee = effective_swap_fee(STATIC_LP_FEE, 200);
        let event = swap_event(pool_id(), position(0, 1), 9, -10, one_for_zero_fee);
        mirror.apply(UniswapV4MirrorEvent::Swap(&event)).unwrap();

        let zero_output = swap_event(pool_id(), position(0, 2), -10, 0, zero_for_one_fee);
        mirror.apply_swap(&zero_output).unwrap();

        let mut zero_delta_mirror = UniswapV4Mirror::bootstrap(config(), &snapshot()).unwrap();
        let zero_delta = swap_event(pool_id(), position(0, 0), 0, 0, one_for_zero_fee);
        zero_delta_mirror.apply_swap(&zero_delta).unwrap();

        let before = mirror.clone();
        let bad_fee = swap_event(pool_id(), position(0, 3), -10, 9, one_for_zero_fee);
        assert!(matches!(
            mirror.apply_swap(&bad_fee),
            Err(UniswapV4MirrorError::EffectiveFeeMismatch { .. })
        ));
        assert_eq!(mirror, before);

        let ambiguous = swap_event(pool_id(), position(0, 3), 10, 9, zero_for_one_fee);
        assert!(matches!(
            mirror.apply_swap(&ambiguous),
            Err(UniswapV4MirrorError::AmbiguousSwapDirection { .. })
        ));
        assert_eq!(mirror, before);
    }

    #[test]
    fn modify_liquidity_handles_active_and_out_of_range_changes() {
        let mut mirror = mirror();
        mirror
            .apply_modify_liquidity(&modify_event(pool_id(), position(0, 0), -60, 60, i256(100)))
            .unwrap();
        assert_eq!(mirror.liquidity(), 1_100);
        assert_eq!(
            mirror.tick_liquidity(-60),
            Some(&UniswapV4MirrorTick {
                liquidity_gross: 100,
                liquidity_net: 100,
            })
        );
        assert_eq!(
            mirror.tick_liquidity(60),
            Some(&UniswapV4MirrorTick {
                liquidity_gross: 100,
                liquidity_net: -100,
            })
        );
        assert!(mirror.tick_bitmap.is_initialized(-60));
        assert!(mirror.tick_bitmap.is_initialized(60));
        assert!(mirror.tick_bitmap.is_initialized(-120));

        mirror
            .apply_modify_liquidity(&modify_event(pool_id(), position(0, 1), -60, 60, i256(50)))
            .unwrap();
        assert!(mirror.tick_bitmap.is_initialized(-60));
        assert!(mirror.tick_bitmap.is_initialized(60));
        mirror
            .apply_modify_liquidity(&modify_event(pool_id(), position(0, 2), -60, 60, i256(-50)))
            .unwrap();
        assert!(mirror.tick_bitmap.is_initialized(-60));
        assert!(mirror.tick_bitmap.is_initialized(60));

        mirror
            .apply_modify_liquidity(&modify_event(pool_id(), position(0, 3), 60, 180, i256(200)))
            .unwrap();
        mirror
            .apply_modify_liquidity(&modify_event(
                pool_id(),
                position(0, 4),
                -240,
                -180,
                i256(300),
            ))
            .unwrap();
        assert_eq!(mirror.liquidity(), 1_100);
    }

    #[test]
    fn negative_removal_updates_net_and_removes_zero_gross_ticks() {
        let mut mirror = mirror();
        let event = modify_event(pool_id(), position(0, 0), -120, 120, i256(-1_000));
        mirror.apply_modify_liquidity(&event).unwrap();

        assert_eq!(mirror.liquidity(), 0);
        assert!(mirror.ticks().is_empty());
        assert!(!mirror.tick_bitmap.is_initialized(-120));
        assert!(!mirror.tick_bitmap.is_initialized(120));
    }

    #[test]
    fn zero_delta_poke_validates_range_and_only_advances_watermark() {
        let mut mirror = mirror();
        let ticks = mirror.ticks.clone();
        let event = modify_event(pool_id(), position(0, 0), -60, 60, I256::ZERO);
        mirror.apply_modify_liquidity(&event).unwrap();
        assert_eq!(mirror.ticks, ticks);
        assert_eq!(mirror.liquidity(), 1_000);
        assert_eq!(mirror.watermark(), position(0, 0));

        let before = mirror.clone();
        let unaligned = modify_event(pool_id(), position(0, 1), -61, 60, I256::ZERO);
        assert!(matches!(
            mirror.apply_modify_liquidity(&unaligned),
            Err(UniswapV4MirrorError::InvalidTickRange { .. })
        ));
        assert_eq!(mirror, before);
    }

    #[test]
    fn liquidity_conversion_underflow_and_overflow_are_atomic() {
        let mut mirror = mirror();
        let before = mirror.clone();
        let too_wide = I256::try_from(u128::MAX).unwrap();
        let event = modify_event(pool_id(), position(0, 0), -120, 120, too_wide);
        assert!(matches!(
            mirror.apply_modify_liquidity(&event),
            Err(UniswapV4MirrorError::LiquidityDeltaOutOfRange { .. })
        ));
        assert_eq!(mirror, before);

        let event = modify_event(pool_id(), position(0, 0), -120, 120, i256(-1_001));
        assert!(matches!(
            mirror.apply_modify_liquidity(&event),
            Err(UniswapV4MirrorError::LiquidityArithmetic { .. })
        ));
        assert_eq!(mirror, before);

        mirror.liquidity = u128::MAX;
        let before = mirror.clone();
        let event = modify_event(pool_id(), position(0, 0), -60, 60, i256(1));
        assert_eq!(
            mirror.apply_modify_liquidity(&event),
            Err(UniswapV4MirrorError::ActiveLiquidityArithmetic)
        );
        assert_eq!(mirror, before);
    }

    #[test]
    fn wrong_pool_duplicate_and_out_of_order_events_are_rejected_atomically() {
        let mut mirror = mirror();
        let wrong = protocol_event(B256::repeat_byte(0x99), position(0, 0), 0);
        let before = mirror.clone();
        assert!(matches!(
            mirror.apply_protocol_fee_updated(&wrong),
            Err(UniswapV4MirrorError::WrongPool { .. })
        ));
        assert_eq!(mirror, before);

        let accepted = protocol_event(pool_id(), position(1, 2), 0);
        mirror.apply_protocol_fee_updated(&accepted).unwrap();
        let before = mirror.clone();
        assert!(matches!(
            mirror.apply_protocol_fee_updated(&accepted),
            Err(UniswapV4MirrorError::NonIncreasingPosition { .. })
        ));
        let older = protocol_event(pool_id(), position(1, 1), 0);
        assert!(matches!(
            mirror.apply_protocol_fee_updated(&older),
            Err(UniswapV4MirrorError::NonIncreasingPosition { .. })
        ));
        assert_eq!(mirror, before);
    }

    #[test]
    fn protocol_fee_lanes_are_validated() {
        let mut mirror = mirror();
        let invalid = protocol_event(pool_id(), position(0, 0), 1_001);
        let before = mirror.clone();
        assert!(matches!(
            mirror.apply_protocol_fee_updated(&invalid),
            Err(UniswapV4MirrorError::InvalidProtocolFee { .. })
        ));
        assert_eq!(mirror, before);

        let invalid = protocol_event(pool_id(), position(0, 0), 1_001 << 12);
        assert!(matches!(
            mirror.apply_protocol_fee_updated(&invalid),
            Err(UniswapV4MirrorError::InvalidProtocolFee { .. })
        ));
        assert_eq!(mirror, before);

        assert_eq!(mirror, before);
    }

    #[test]
    fn fresh_state_view_snapshot_is_compared_exactly() {
        let mut mirror = mirror();
        mirror
            .apply_modify_liquidity(&modify_event(pool_id(), position(0, 0), -60, 60, i256(25)))
            .unwrap();
        let fresh = state_view_from_mirror(&mirror);
        mirror.validate_against_snapshot(&fresh).unwrap();

        let mut drifted = fresh.clone();
        drifted.ticks[1].liquidity_net += 1;
        assert_eq!(
            mirror.validate_against_snapshot(&drifted),
            Err(UniswapV4MirrorError::SnapshotStateMismatch { field: "ticks" })
        );

        let mut wrong_block = fresh;
        wrong_block.block_number += 1;
        assert!(matches!(
            mirror.validate_against_snapshot(&wrong_block),
            Err(UniswapV4MirrorError::SnapshotBlockMismatch { .. })
        ));
    }
}
