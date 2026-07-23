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

//! Exact, read-only quotes for the static-fee, zero-hook [`UniswapV4Mirror`].

use alloy::primitives::{I256, U160, U256};
use nautilus_model::defi::tick_map::{
    full_math::FullMath,
    sqrt_price_math::{
        get_amount0_delta, get_amount1_delta, get_next_sqrt_price_from_input,
        get_next_sqrt_price_from_output,
    },
    tick::PoolTick,
    tick_math::{MAX_SQRT_RATIO, MIN_SQRT_RATIO, get_sqrt_ratio_at_tick, get_tick_at_sqrt_ratio},
};
use thiserror::Error;

use super::uniswap_v4_mirror::{UniswapV4EventPosition, UniswapV4Mirror};

const FEE_DENOMINATOR: u64 = 1_000_000;
const PROTOCOL_FEE_LANE_MASK: u32 = 0x0fff;
const MAX_BALANCE_DELTA: U256 = U256::from_limbs([u64::MAX, 0x7fff_ffff_ffff_ffff, 0, 0]);
const MAX_SWAP_FEE: U256 = U256::from_limbs([FEE_DENOMINATOR, 0, 0, 0]);

/// Token direction for a quote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniswapV4QuoteDirection {
    /// Token0 is input and token1 is output; price moves down.
    ZeroForOne,
    /// Token1 is input and token0 is output; price moves up.
    OneForZero,
}

impl UniswapV4QuoteDirection {
    const fn zero_for_one(self) -> bool {
        matches!(self, Self::ZeroForOne)
    }
}

/// Explicit amount semantics for a quote request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniswapV4QuoteAmount {
    /// Spend exactly this raw input-token amount.
    ExactInput(U256),
    /// Receive exactly this raw output-token amount.
    ExactOutput(U256),
}

impl UniswapV4QuoteAmount {
    const fn raw(self) -> U256 {
        match self {
            Self::ExactInput(amount) | Self::ExactOutput(amount) => amount,
        }
    }

    const fn exact_input(self) -> bool {
        matches!(self, Self::ExactInput(_))
    }
}

/// One exact quote request. A missing limit uses the V4Quoter directional default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UniswapV4QuoteRequest {
    pub direction: UniswapV4QuoteDirection,
    pub amount: UniswapV4QuoteAmount,
    pub sqrt_price_limit_x96: Option<U160>,
}

impl UniswapV4QuoteRequest {
    #[must_use]
    pub const fn exact_input(
        direction: UniswapV4QuoteDirection,
        amount: U256,
        sqrt_price_limit_x96: Option<U160>,
    ) -> Self {
        Self {
            direction,
            amount: UniswapV4QuoteAmount::ExactInput(amount),
            sqrt_price_limit_x96,
        }
    }

    #[must_use]
    pub const fn exact_output(
        direction: UniswapV4QuoteDirection,
        amount: U256,
        sqrt_price_limit_x96: Option<U160>,
    ) -> Self {
        Self {
            direction,
            amount: UniswapV4QuoteAmount::ExactOutput(amount),
            sqrt_price_limit_x96,
        }
    }
}

/// V4-specific result in raw token units.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniswapV4QuoteResult {
    pub amount_in: U256,
    pub amount_out: U256,
    /// Negative input and positive output, matching the PoolManager Swap event.
    pub amount0: I256,
    /// Negative input and positive output, matching the PoolManager Swap event.
    pub amount1: I256,
    pub sqrt_price_x96_before: U160,
    pub sqrt_price_x96_after: U160,
    pub tick_before: i32,
    pub tick_after: i32,
    pub liquidity_before: u128,
    pub liquidity_after: u128,
    pub effective_fee_pips: u32,
    pub lp_fee_amount: U256,
    pub protocol_fee_amount: U256,
    pub initialized_ticks_crossed: Vec<i32>,
    pub mirror_watermark: UniswapV4EventPosition,
}

/// Fail-closed validation and exact traversal errors.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum UniswapV4QuoteError {
    #[error("quote amount must be nonzero")]
    ZeroAmount,
    #[error("quote amount exceeds the PoolManager int128 balance-delta range: {amount}")]
    AmountTooLarge { amount: U256 },
    #[error("invalid sqrt-price limit {limit} for {direction:?} from current price {current}")]
    InvalidSqrtPriceLimit {
        direction: UniswapV4QuoteDirection,
        current: U160,
        limit: U160,
    },
    #[error("exact-output quote is impossible at a 100% effective fee")]
    ExactOutputAtFullFee,
    #[error(
        "not enough liquidity to fully fill quote: {amount_remaining} raw amount remains at sqrt price {sqrt_price_x96}"
    )]
    InsufficientLiquidity {
        amount_remaining: U256,
        sqrt_price_x96: U160,
    },
    #[error("initialized bitmap tick {tick} has no mirror tick state")]
    MissingInitializedTick { tick: i32 },
    #[error("liquidity arithmetic failed while crossing tick {tick}")]
    TickLiquidityArithmetic { tick: i32 },
    #[error("quote arithmetic failed during {operation}")]
    Arithmetic { operation: &'static str },
}

/// Runtime-pluggable quote interface. Implementations incur one virtual call per quote when boxed.
pub trait UniswapV4QuoteEngine: Send + Sync {
    /// Quotes one swap against immutable mirror state.
    ///
    /// # Errors
    ///
    /// Returns an implementation-specific error when the request is invalid, cannot be fully
    /// filled, or quote arithmetic detects inconsistent state.
    fn quote(
        &self,
        mirror: &UniswapV4Mirror,
        request: &UniswapV4QuoteRequest,
    ) -> Result<UniswapV4QuoteResult, UniswapV4QuoteError>;
}

/// Exact Uniswap v4 core traversal for the mirror's supported pool subset.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ExactUniswapV4QuoteEngine;

impl UniswapV4QuoteEngine for ExactUniswapV4QuoteEngine {
    fn quote(
        &self,
        mirror: &UniswapV4Mirror,
        request: &UniswapV4QuoteRequest,
    ) -> Result<UniswapV4QuoteResult, UniswapV4QuoteError> {
        quote_exact(mirror, request)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct V4SwapStepResult {
    sqrt_price_next_x96: U160,
    amount_in: U256,
    amount_out: U256,
    fee_amount: U256,
}

fn compute_v4_swap_step(
    sqrt_price_current_x96: U160,
    sqrt_price_target_x96: U160,
    liquidity: u128,
    amount_remaining: U256,
    exact_input: bool,
    fee_pips: u32,
) -> Result<V4SwapStepResult, UniswapV4QuoteError> {
    let fee_pips = U256::from(fee_pips);
    let fee_complement =
        MAX_SWAP_FEE
            .checked_sub(fee_pips)
            .ok_or(UniswapV4QuoteError::Arithmetic {
                operation: "swap fee complement",
            })?;
    let zero_for_one = sqrt_price_current_x96 >= sqrt_price_target_x96;

    let (sqrt_price_next_x96, amount_in, amount_out, fee_amount) = if exact_input {
        let amount_remaining_less_fee =
            FullMath::mul_div(amount_remaining, fee_complement, MAX_SWAP_FEE).map_err(|_| {
                UniswapV4QuoteError::Arithmetic {
                    operation: "exact-input fee adjustment",
                }
            })?;
        let amount_in_to_target = if zero_for_one {
            get_amount0_delta(
                sqrt_price_target_x96,
                sqrt_price_current_x96,
                liquidity,
                true,
            )
        } else {
            get_amount1_delta(
                sqrt_price_current_x96,
                sqrt_price_target_x96,
                liquidity,
                true,
            )
        };

        let (sqrt_price_next_x96, amount_in, fee_amount) =
            if amount_remaining_less_fee >= amount_in_to_target {
                let fee_amount = if fee_pips == MAX_SWAP_FEE {
                    amount_in_to_target
                } else {
                    FullMath::mul_div_rounding_up(amount_in_to_target, fee_pips, fee_complement)
                        .map_err(|_| UniswapV4QuoteError::Arithmetic {
                            operation: "target-step fee",
                        })?
                };
                (sqrt_price_target_x96, amount_in_to_target, fee_amount)
            } else {
                let sqrt_price_next_x96 = get_next_sqrt_price_from_input(
                    sqrt_price_current_x96,
                    liquidity,
                    amount_remaining_less_fee,
                    zero_for_one,
                );
                let fee_amount = amount_remaining
                    .checked_sub(amount_remaining_less_fee)
                    .ok_or(UniswapV4QuoteError::Arithmetic {
                        operation: "partial-step fee",
                    })?;
                (sqrt_price_next_x96, amount_remaining_less_fee, fee_amount)
            };
        let amount_out = if zero_for_one {
            get_amount1_delta(
                sqrt_price_next_x96,
                sqrt_price_current_x96,
                liquidity,
                false,
            )
        } else {
            get_amount0_delta(
                sqrt_price_current_x96,
                sqrt_price_next_x96,
                liquidity,
                false,
            )
        };
        (sqrt_price_next_x96, amount_in, amount_out, fee_amount)
    } else {
        let amount_out_to_target = if zero_for_one {
            get_amount1_delta(
                sqrt_price_target_x96,
                sqrt_price_current_x96,
                liquidity,
                false,
            )
        } else {
            get_amount0_delta(
                sqrt_price_current_x96,
                sqrt_price_target_x96,
                liquidity,
                false,
            )
        };
        let (sqrt_price_next_x96, amount_out) = if amount_remaining >= amount_out_to_target {
            (sqrt_price_target_x96, amount_out_to_target)
        } else {
            (
                get_next_sqrt_price_from_output(
                    sqrt_price_current_x96,
                    liquidity,
                    amount_remaining,
                    zero_for_one,
                ),
                amount_remaining,
            )
        };
        let amount_in = if zero_for_one {
            get_amount0_delta(sqrt_price_next_x96, sqrt_price_current_x96, liquidity, true)
        } else {
            get_amount1_delta(sqrt_price_current_x96, sqrt_price_next_x96, liquidity, true)
        };
        let fee_amount = FullMath::mul_div_rounding_up(amount_in, fee_pips, fee_complement)
            .map_err(|_| UniswapV4QuoteError::Arithmetic {
                operation: "exact-output fee",
            })?;
        (sqrt_price_next_x96, amount_in, amount_out, fee_amount)
    };

    Ok(V4SwapStepResult {
        sqrt_price_next_x96,
        amount_in,
        amount_out,
        fee_amount,
    })
}

fn quote_exact(
    mirror: &UniswapV4Mirror,
    request: &UniswapV4QuoteRequest,
) -> Result<UniswapV4QuoteResult, UniswapV4QuoteError> {
    let specified_amount = request.amount.raw();
    if specified_amount.is_zero() {
        return Err(UniswapV4QuoteError::ZeroAmount);
    }
    if specified_amount > MAX_BALANCE_DELTA {
        return Err(UniswapV4QuoteError::AmountTooLarge {
            amount: specified_amount,
        });
    }

    let zero_for_one = request.direction.zero_for_one();
    let protocol_fee = directional_protocol_fee(mirror.protocol_fee(), zero_for_one);
    let effective_fee_pips = effective_swap_fee(mirror.lp_fee(), protocol_fee);
    if !request.amount.exact_input() && effective_fee_pips == FEE_DENOMINATOR as u32 {
        return Err(UniswapV4QuoteError::ExactOutputAtFullFee);
    }

    let sqrt_price_limit_x96 = request.sqrt_price_limit_x96.unwrap_or_else(|| {
        if zero_for_one {
            MIN_SQRT_RATIO + U160::from(1)
        } else {
            MAX_SQRT_RATIO - U160::from(1)
        }
    });
    let current_sqrt_price = mirror.sqrt_price_x96();
    let valid_limit = if zero_for_one {
        sqrt_price_limit_x96 > MIN_SQRT_RATIO && sqrt_price_limit_x96 < current_sqrt_price
    } else {
        sqrt_price_limit_x96 < MAX_SQRT_RATIO && sqrt_price_limit_x96 > current_sqrt_price
    };
    if !valid_limit {
        return Err(UniswapV4QuoteError::InvalidSqrtPriceLimit {
            direction: request.direction,
            current: current_sqrt_price,
            limit: sqrt_price_limit_x96,
        });
    }

    let exact_input = request.amount.exact_input();
    let mut amount_remaining = specified_amount;
    let mut amount_in = U256::ZERO;
    let mut amount_out = U256::ZERO;
    let mut lp_fee_amount = U256::ZERO;
    let mut protocol_fee_amount = U256::ZERO;
    let mut sqrt_price_x96 = current_sqrt_price;
    let mut tick = mirror.tick();
    let mut liquidity = mirror.liquidity();
    let mut initialized_ticks_crossed = Vec::new();

    while !amount_remaining.is_zero() && sqrt_price_x96 != sqrt_price_limit_x96 {
        let sqrt_price_start_x96 = sqrt_price_x96;
        let (tick_next_unclamped, initialized) = mirror
            .tick_bitmap()
            .next_initialized_tick_within_one_word(tick, zero_for_one);
        let tick_next = tick_next_unclamped.clamp(PoolTick::MIN_TICK, PoolTick::MAX_TICK);
        let sqrt_price_next_x96 = get_sqrt_ratio_at_tick(tick_next);
        let sqrt_price_target_x96 = if (zero_for_one && sqrt_price_next_x96 < sqrt_price_limit_x96)
            || (!zero_for_one && sqrt_price_next_x96 > sqrt_price_limit_x96)
        {
            sqrt_price_limit_x96
        } else {
            sqrt_price_next_x96
        };

        let step = compute_v4_swap_step(
            sqrt_price_x96,
            sqrt_price_target_x96,
            liquidity,
            amount_remaining,
            exact_input,
            effective_fee_pips,
        )?;
        sqrt_price_x96 = step.sqrt_price_next_x96;

        let step_amount_in =
            step.amount_in
                .checked_add(step.fee_amount)
                .ok_or(UniswapV4QuoteError::Arithmetic {
                    operation: "step input accumulation",
                })?;
        if exact_input {
            amount_remaining = amount_remaining.checked_sub(step_amount_in).ok_or(
                UniswapV4QuoteError::Arithmetic {
                    operation: "exact-input remainder",
                },
            )?;
            amount_in =
                amount_in
                    .checked_add(step_amount_in)
                    .ok_or(UniswapV4QuoteError::Arithmetic {
                        operation: "total input",
                    })?;
            amount_out =
                amount_out
                    .checked_add(step.amount_out)
                    .ok_or(UniswapV4QuoteError::Arithmetic {
                        operation: "total output",
                    })?;
        } else {
            amount_remaining = amount_remaining.checked_sub(step.amount_out).ok_or(
                UniswapV4QuoteError::Arithmetic {
                    operation: "exact-output remainder",
                },
            )?;
            amount_out =
                amount_out
                    .checked_add(step.amount_out)
                    .ok_or(UniswapV4QuoteError::Arithmetic {
                        operation: "total output",
                    })?;
            amount_in =
                amount_in
                    .checked_add(step_amount_in)
                    .ok_or(UniswapV4QuoteError::Arithmetic {
                        operation: "total input",
                    })?;
        }

        let step_protocol_fee = if protocol_fee == 0 {
            U256::ZERO
        } else if effective_fee_pips == protocol_fee {
            step.fee_amount
        } else {
            FullMath::mul_div(
                step_amount_in,
                U256::from(protocol_fee),
                U256::from(FEE_DENOMINATOR),
            )
            .map_err(|_| UniswapV4QuoteError::Arithmetic {
                operation: "protocol fee split",
            })?
        };
        let step_lp_fee = step.fee_amount.checked_sub(step_protocol_fee).ok_or(
            UniswapV4QuoteError::Arithmetic {
                operation: "LP fee split",
            },
        )?;
        protocol_fee_amount = protocol_fee_amount.checked_add(step_protocol_fee).ok_or(
            UniswapV4QuoteError::Arithmetic {
                operation: "protocol fee accumulation",
            },
        )?;
        lp_fee_amount =
            lp_fee_amount
                .checked_add(step_lp_fee)
                .ok_or(UniswapV4QuoteError::Arithmetic {
                    operation: "LP fee accumulation",
                })?;

        if sqrt_price_x96 == sqrt_price_next_x96 {
            if initialized {
                let tick_state = mirror
                    .tick_liquidity(tick_next)
                    .ok_or(UniswapV4QuoteError::MissingInitializedTick { tick: tick_next })?;
                liquidity = cross_liquidity(liquidity, tick_state.liquidity_net, zero_for_one)
                    .ok_or(UniswapV4QuoteError::TickLiquidityArithmetic { tick: tick_next })?;
                initialized_ticks_crossed.push(tick_next);
            }
            tick = if zero_for_one {
                tick_next - 1
            } else {
                tick_next
            };
        } else if sqrt_price_x96 != sqrt_price_start_x96 {
            tick = get_tick_at_sqrt_ratio(sqrt_price_x96);
        }
    }

    if !amount_remaining.is_zero() {
        return Err(UniswapV4QuoteError::InsufficientLiquidity {
            amount_remaining,
            sqrt_price_x96,
        });
    }
    if amount_in > MAX_BALANCE_DELTA || amount_out > MAX_BALANCE_DELTA {
        return Err(UniswapV4QuoteError::AmountTooLarge {
            amount: amount_in.max(amount_out),
        });
    }

    let signed_input = -I256::from_raw(amount_in);
    let signed_output = I256::from_raw(amount_out);
    let (amount0, amount1) = if zero_for_one {
        (signed_input, signed_output)
    } else {
        (signed_output, signed_input)
    };

    Ok(UniswapV4QuoteResult {
        amount_in,
        amount_out,
        amount0,
        amount1,
        sqrt_price_x96_before: current_sqrt_price,
        sqrt_price_x96_after: sqrt_price_x96,
        tick_before: mirror.tick(),
        tick_after: tick,
        liquidity_before: mirror.liquidity(),
        liquidity_after: liquidity,
        effective_fee_pips,
        lp_fee_amount,
        protocol_fee_amount,
        initialized_ticks_crossed,
        mirror_watermark: mirror.watermark(),
    })
}

const fn directional_protocol_fee(protocol_fee: u32, zero_for_one: bool) -> u32 {
    if zero_for_one {
        protocol_fee & PROTOCOL_FEE_LANE_MASK
    } else {
        (protocol_fee >> 12) & PROTOCOL_FEE_LANE_MASK
    }
}

fn effective_swap_fee(lp_fee: u32, protocol_fee: u32) -> u32 {
    let lp_fee = u64::from(lp_fee);
    let protocol_fee = u64::from(protocol_fee);
    (protocol_fee + lp_fee - protocol_fee * lp_fee / FEE_DENOMINATOR) as u32
}

fn cross_liquidity(liquidity: u128, liquidity_net: i128, zero_for_one: bool) -> Option<u128> {
    let add = (liquidity_net < 0) == zero_for_one;
    if add {
        liquidity.checked_add(liquidity_net.unsigned_abs())
    } else {
        liquidity.checked_sub(liquidity_net.unsigned_abs())
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use alloy::{
        primitives::{
            Address, B256, Bytes, address,
            aliases::{I24, U24},
        },
        sol,
        sol_types::SolCall,
    };

    use super::*;
    use crate::{
        contracts::{
            base::BaseContract,
            uniswap_v4_state_view::{
                UniswapV4PoolState, UniswapV4Slot0State, UniswapV4StateViewContract,
                UniswapV4TickLiquidityState,
            },
        },
        rpc::http::BlockchainHttpRpcClient,
        services::uniswap_v4_mirror::UniswapV4MirrorConfig,
    };

    sol! {
        struct QuoterPoolKey {
            address currency0;
            address currency1;
            uint24 fee;
            int24 tickSpacing;
            address hooks;
        }

        struct QuoteExactSingleParams {
            QuoterPoolKey poolKey;
            bool zeroForOne;
            uint128 exactAmount;
            bytes hookData;
        }

        function quoteExactInputSingle(QuoteExactSingleParams memory params)
            external returns (uint256 amountOut, uint256 gasEstimate);

        function quoteExactOutputSingle(QuoteExactSingleParams memory params)
            external returns (uint256 amountIn, uint256 gasEstimate);
    }

    const LIQUIDITY: u128 = 1_000_000_000;

    fn mirror_with(
        tick_spacing: i32,
        lp_fee: u32,
        protocol_fee: u32,
        liquidity: u128,
        ticks: Vec<UniswapV4TickLiquidityState>,
    ) -> UniswapV4Mirror {
        let pool_id = B256::repeat_byte(0x44);
        let config = UniswapV4MirrorConfig::new_unchecked_for_test(pool_id, tick_spacing, lp_fee);
        UniswapV4Mirror::bootstrap(
            config,
            &UniswapV4PoolState {
                pool_id,
                tick_spacing,
                block_number: 42,
                slot0: UniswapV4Slot0State {
                    sqrt_price_x96: U160::from(1_u8) << 96,
                    tick: 0,
                    protocol_fee,
                    lp_fee,
                },
                liquidity,
                ticks,
            },
        )
        .unwrap()
    }

    fn simple_mirror(lp_fee: u32, protocol_fee: u32) -> UniswapV4Mirror {
        mirror_with(
            1,
            lp_fee,
            protocol_fee,
            LIQUIDITY,
            vec![
                tick(-600, LIQUIDITY, LIQUIDITY as i128),
                tick(600, LIQUIDITY, -(LIQUIDITY as i128)),
            ],
        )
    }

    const fn tick(
        tick: i32,
        liquidity_gross: u128,
        liquidity_net: i128,
    ) -> UniswapV4TickLiquidityState {
        UniswapV4TickLiquidityState {
            tick,
            liquidity_gross,
            liquidity_net,
        }
    }

    fn quote(
        mirror: &UniswapV4Mirror,
        request: UniswapV4QuoteRequest,
    ) -> Result<UniswapV4QuoteResult, UniswapV4QuoteError> {
        ExactUniswapV4QuoteEngine.quote(mirror, &request)
    }

    #[test]
    fn exact_input_both_directions_without_crossing_has_pool_manager_signs() {
        let mirror = simple_mirror(3_000, 0);
        let before = mirror.clone();

        let zero_for_one = quote(
            &mirror,
            UniswapV4QuoteRequest::exact_input(
                UniswapV4QuoteDirection::ZeroForOne,
                U256::from(1_000),
                None,
            ),
        )
        .unwrap();
        assert_eq!(zero_for_one.amount_in, U256::from(1_000));
        assert!(zero_for_one.amount_out > U256::ZERO);
        assert_eq!(zero_for_one.amount0, I256::try_from(-1_000).unwrap());
        assert_eq!(
            zero_for_one.amount1,
            I256::from_raw(zero_for_one.amount_out)
        );
        assert!(zero_for_one.sqrt_price_x96_after < zero_for_one.sqrt_price_x96_before);
        assert!(zero_for_one.initialized_ticks_crossed.is_empty());

        let one_for_zero = quote(
            &mirror,
            UniswapV4QuoteRequest::exact_input(
                UniswapV4QuoteDirection::OneForZero,
                U256::from(1_000),
                None,
            ),
        )
        .unwrap();
        assert_eq!(one_for_zero.amount1, I256::try_from(-1_000).unwrap());
        assert_eq!(
            one_for_zero.amount0,
            I256::from_raw(one_for_zero.amount_out)
        );
        assert!(one_for_zero.sqrt_price_x96_after > one_for_zero.sqrt_price_x96_before);
        assert_eq!(mirror, before);
    }

    #[test]
    fn exact_output_both_directions_without_crossing_is_fully_filled() {
        let mirror = simple_mirror(3_000, 0);
        for direction in [
            UniswapV4QuoteDirection::ZeroForOne,
            UniswapV4QuoteDirection::OneForZero,
        ] {
            let result = quote(
                &mirror,
                UniswapV4QuoteRequest::exact_output(direction, U256::from(1_000), None),
            )
            .unwrap();
            assert_eq!(result.amount_out, U256::from(1_000));
            assert!(result.amount_in > result.amount_out);
            if direction == UniswapV4QuoteDirection::ZeroForOne {
                assert!(result.amount0.is_negative());
                assert_eq!(result.amount1, I256::try_from(1_000).unwrap());
            } else {
                assert_eq!(result.amount0, I256::try_from(1_000).unwrap());
                assert!(result.amount1.is_negative());
            }
        }
    }

    #[test]
    fn crosses_initialized_ticks_and_applies_zero_for_one_tick_next_minus_one() {
        let unit = 1_000_000_u128;
        let mirror = mirror_with(
            60,
            3_000,
            0,
            2 * unit,
            vec![
                tick(-600, unit, unit as i128),
                tick(-60, unit, unit as i128),
                tick(60, unit, -(unit as i128)),
                tick(600, unit, -(unit as i128)),
            ],
        );
        let result = quote(
            &mirror,
            UniswapV4QuoteRequest::exact_input(
                UniswapV4QuoteDirection::ZeroForOne,
                U256::from(10_000),
                None,
            ),
        )
        .unwrap();

        assert_eq!(result.initialized_ticks_crossed, [-60]);
        assert_eq!(result.liquidity_after, unit);
        assert!(result.tick_after < -60);
    }

    #[test]
    fn traverses_empty_bitmap_word_boundary_before_initialized_tick() {
        let unit = 1_000_000_u128;
        let mirror = mirror_with(
            1,
            0,
            0,
            2 * unit,
            vec![
                tick(-600, unit, unit as i128),
                tick(-300, unit, unit as i128),
                tick(300, unit, -(unit as i128)),
                tick(600, unit, -(unit as i128)),
            ],
        );
        let result = quote(
            &mirror,
            UniswapV4QuoteRequest::exact_input(
                UniswapV4QuoteDirection::ZeroForOne,
                U256::from(40_000),
                None,
            ),
        )
        .unwrap();

        assert_eq!(result.initialized_ticks_crossed, [-300]);
        assert!(result.tick_after < -300);
    }

    #[test]
    fn crosses_multiple_initialized_ticks_exactly() {
        let unit = 1_000_000_u128;
        let mirror = mirror_with(
            60,
            3_000,
            0,
            4 * unit,
            vec![
                tick(-600, unit, unit as i128),
                tick(-180, unit, unit as i128),
                tick(-120, unit, unit as i128),
                tick(-60, unit, unit as i128),
                tick(60, unit, -(unit as i128)),
                tick(120, unit, -(unit as i128)),
                tick(180, unit, -(unit as i128)),
                tick(600, unit, -(unit as i128)),
            ],
        );
        let result = quote(
            &mirror,
            UniswapV4QuoteRequest::exact_input(
                UniswapV4QuoteDirection::ZeroForOne,
                U256::from(30_000),
                None,
            ),
        )
        .unwrap();

        assert_eq!(result.initialized_ticks_crossed, [-60, -120, -180]);
        assert_eq!(result.liquidity_after, unit);
    }

    #[test]
    fn uses_directional_protocol_lanes_and_v4_fee_split() {
        let mirror = simple_mirror(3_000, (200 << 12) | 100);
        let amount = U256::from(1_000_000);
        let zero_for_one = quote(
            &mirror,
            UniswapV4QuoteRequest::exact_input(UniswapV4QuoteDirection::ZeroForOne, amount, None),
        )
        .unwrap();
        let one_for_zero = quote(
            &mirror,
            UniswapV4QuoteRequest::exact_input(UniswapV4QuoteDirection::OneForZero, amount, None),
        )
        .unwrap();

        assert_eq!(zero_for_one.effective_fee_pips, 3_100);
        assert_eq!(one_for_zero.effective_fee_pips, 3_200);
        assert_eq!(zero_for_one.protocol_fee_amount, U256::from(100));
        assert_eq!(one_for_zero.protocol_fee_amount, U256::from(200));
        assert!(zero_for_one.lp_fee_amount > U256::ZERO);

        let protocol_only = simple_mirror(0, 500);
        let result = quote(
            &protocol_only,
            UniswapV4QuoteRequest::exact_input(UniswapV4QuoteDirection::ZeroForOne, amount, None),
        )
        .unwrap();
        assert_eq!(result.lp_fee_amount, U256::ZERO);
        assert!(result.protocol_fee_amount > U256::ZERO);
    }

    #[test]
    fn default_limits_match_explicit_v4_quoter_limits_and_invalid_limits_fail() {
        let mirror = simple_mirror(3_000, 0);
        let default = quote(
            &mirror,
            UniswapV4QuoteRequest::exact_input(
                UniswapV4QuoteDirection::ZeroForOne,
                U256::from(1_000),
                None,
            ),
        )
        .unwrap();
        let explicit = quote(
            &mirror,
            UniswapV4QuoteRequest::exact_input(
                UniswapV4QuoteDirection::ZeroForOne,
                U256::from(1_000),
                Some(MIN_SQRT_RATIO + U160::from(1)),
            ),
        )
        .unwrap();
        assert_eq!(default, explicit);

        let default = quote(
            &mirror,
            UniswapV4QuoteRequest::exact_input(
                UniswapV4QuoteDirection::OneForZero,
                U256::from(1_000),
                None,
            ),
        )
        .unwrap();
        let explicit = quote(
            &mirror,
            UniswapV4QuoteRequest::exact_input(
                UniswapV4QuoteDirection::OneForZero,
                U256::from(1_000),
                Some(MAX_SQRT_RATIO - U160::from(1)),
            ),
        )
        .unwrap();
        assert_eq!(default, explicit);

        assert!(matches!(
            quote(
                &mirror,
                UniswapV4QuoteRequest::exact_input(
                    UniswapV4QuoteDirection::ZeroForOne,
                    U256::from(1),
                    Some(mirror.sqrt_price_x96()),
                ),
            ),
            Err(UniswapV4QuoteError::InvalidSqrtPriceLimit { .. })
        ));
        assert!(matches!(
            quote(
                &mirror,
                UniswapV4QuoteRequest::exact_input(
                    UniswapV4QuoteDirection::OneForZero,
                    U256::from(1),
                    Some(MAX_SQRT_RATIO),
                ),
            ),
            Err(UniswapV4QuoteError::InvalidSqrtPriceLimit { .. })
        ));
    }

    #[test]
    fn requires_full_fill_and_rejects_zero_or_oversized_amounts() {
        let mirror = simple_mirror(3_000, 0);
        assert!(matches!(
            quote(
                &mirror,
                UniswapV4QuoteRequest::exact_output(
                    UniswapV4QuoteDirection::ZeroForOne,
                    U256::from(100_000_000),
                    None,
                ),
            ),
            Err(UniswapV4QuoteError::InsufficientLiquidity { .. })
        ));
        assert_eq!(
            quote(
                &mirror,
                UniswapV4QuoteRequest::exact_input(
                    UniswapV4QuoteDirection::ZeroForOne,
                    U256::ZERO,
                    None,
                ),
            ),
            Err(UniswapV4QuoteError::ZeroAmount)
        );
        let oversized = MAX_BALANCE_DELTA + U256::from(1);
        assert_eq!(
            quote(
                &mirror,
                UniswapV4QuoteRequest::exact_input(
                    UniswapV4QuoteDirection::ZeroForOne,
                    oversized,
                    None,
                ),
            ),
            Err(UniswapV4QuoteError::AmountTooLarge { amount: oversized })
        );
    }

    #[test]
    fn full_fee_exact_input_is_all_fee_and_exact_output_is_rejected() {
        let mirror = simple_mirror(1_000_000, 0);
        let result = quote(
            &mirror,
            UniswapV4QuoteRequest::exact_input(
                UniswapV4QuoteDirection::ZeroForOne,
                U256::from(100),
                None,
            ),
        )
        .unwrap();
        assert_eq!(result.amount_in, U256::from(100));
        assert_eq!(result.amount_out, U256::ZERO);
        assert_eq!(result.lp_fee_amount, U256::from(100));
        assert_eq!(result.sqrt_price_x96_after, result.sqrt_price_x96_before);

        assert_eq!(
            quote(
                &mirror,
                UniswapV4QuoteRequest::exact_output(
                    UniswapV4QuoteDirection::ZeroForOne,
                    U256::from(1),
                    None,
                ),
            ),
            Err(UniswapV4QuoteError::ExactOutputAtFullFee)
        );
    }

    #[test]
    fn full_fee_crosses_zero_liquidity_before_consuming_input() {
        let unit = 1_000_000_u128;
        let mirror = mirror_with(
            1,
            1_000_000,
            0,
            0,
            vec![
                tick(300, unit, unit as i128),
                tick(600, unit, -(unit as i128)),
            ],
        );
        let result = quote(
            &mirror,
            UniswapV4QuoteRequest::exact_input(
                UniswapV4QuoteDirection::OneForZero,
                U256::from(100),
                None,
            ),
        )
        .unwrap();

        assert_eq!(result.amount_in, U256::from(100));
        assert_eq!(result.amount_out, U256::ZERO);
        assert_eq!(result.lp_fee_amount, U256::from(100));
        assert_eq!(result.initialized_ticks_crossed, [300]);
        assert_eq!(result.sqrt_price_x96_after, get_sqrt_ratio_at_tick(300));
        assert_eq!(result.tick_after, 300);
        assert_eq!(result.liquidity_after, unit);
    }

    #[test]
    fn v4_partial_exact_input_does_not_turn_rounding_into_fee() {
        let current = U160::from(1_u8) << 96;
        let target = get_sqrt_ratio_at_tick(100);
        let liquidity = (1_u128 << 97) + 1;
        let result =
            compute_v4_swap_step(current, target, liquidity, U256::from(10), true, 0).unwrap();

        assert_eq!(result.amount_in, U256::from(10));
        assert_eq!(result.fee_amount, U256::ZERO);
        assert_eq!(result.sqrt_price_next_x96, current + U160::from(4));
    }

    #[test]
    fn trait_object_can_substitute_engine_with_one_quote_dispatch() {
        struct FakeEngine;

        impl UniswapV4QuoteEngine for FakeEngine {
            fn quote(
                &self,
                _mirror: &UniswapV4Mirror,
                _request: &UniswapV4QuoteRequest,
            ) -> Result<UniswapV4QuoteResult, UniswapV4QuoteError> {
                Err(UniswapV4QuoteError::ZeroAmount)
            }
        }

        let engine: Box<dyn UniswapV4QuoteEngine> = Box::new(FakeEngine);
        let mirror = simple_mirror(3_000, 0);
        let request = UniswapV4QuoteRequest::exact_input(
            UniswapV4QuoteDirection::ZeroForOne,
            U256::from(1),
            None,
        );
        assert_eq!(
            engine.quote(&mirror, &request),
            Err(UniswapV4QuoteError::ZeroAmount)
        );
    }

    #[tokio::test]
    #[ignore = "requires ALCHEMY_ROBINHOOD_HTTP_URL and live Robinhood Chain access"]
    async fn live_exact_engine_matches_robinhood_v4_quoter_at_finalized_block() {
        let http_url = std::env::var("ALCHEMY_ROBINHOOD_HTTP_URL")
            .expect("ALCHEMY_ROBINHOOD_HTTP_URL must be set");
        let client = Arc::new(BlockchainHttpRpcClient::new(http_url, Some(2), None));
        let block = client.finalized_block().await.unwrap();
        let state_view = address!("F3334192D15450CdD385c8B70e03f9A6bD9E673b");
        let quoter = address!("8Dc178eFB8111BB0973Dd9d722ebeFF267c98F94");
        let pool_id =
            B256::from_str("0x3bb34a44f1b2b5f32c034c38a53065a521a47b199700fa9bd19d60985ff24bf1")
                .unwrap();
        let config = UniswapV4MirrorConfig::new(
            pool_id,
            address!("5fc5360D0400a0Fd4f2af552ADD042D716F1d168"),
            address!("d0601CE157Db5BDc3162BbaC2a2C8aF5320D9EEC"),
            60,
            3_000,
            Address::ZERO,
        )
        .unwrap();
        let snapshot = UniswapV4StateViewContract::new(Arc::clone(&client), 100)
            .fetch_pool_state(&state_view, pool_id, 60, block)
            .await
            .unwrap();
        let mirror = UniswapV4Mirror::bootstrap(config, &snapshot).unwrap();
        let contract = BaseContract::new(client);

        for (direction, amount) in [
            (UniswapV4QuoteDirection::ZeroForOne, 1_000_u128),
            (UniswapV4QuoteDirection::OneForZero, 1_000_u128),
            (UniswapV4QuoteDirection::ZeroForOne, 1_000_000_u128),
            (UniswapV4QuoteDirection::OneForZero, 1_000_000_u128),
        ] {
            let params = QuoteExactSingleParams {
                poolKey: QuoterPoolKey {
                    currency0: config.currency0(),
                    currency1: config.currency1(),
                    fee: U24::from(config.static_lp_fee()),
                    tickSpacing: I24::try_from(config.tick_spacing()).unwrap(),
                    hooks: config.hooks(),
                },
                zeroForOne: direction.zero_for_one(),
                exactAmount: amount,
                hookData: Bytes::new(),
            };

            let local_input = quote(
                &mirror,
                UniswapV4QuoteRequest::exact_input(direction, U256::from(amount), None),
            )
            .unwrap();
            let call = quoteExactInputSingleCall {
                params: params.clone(),
            };
            let raw = contract
                .execute_call(&quoter, &call.abi_encode(), Some(block))
                .await
                .unwrap();
            let on_chain = quoteExactInputSingleCall::abi_decode_returns(&raw).unwrap();
            assert_eq!(local_input.amount_out, on_chain.amountOut);

            let local_output = quote(
                &mirror,
                UniswapV4QuoteRequest::exact_output(direction, U256::from(amount), None),
            )
            .unwrap();
            let call = quoteExactOutputSingleCall { params };
            let raw = contract
                .execute_call(&quoter, &call.abi_encode(), Some(block))
                .await
                .unwrap();
            let on_chain = quoteExactOutputSingleCall::abi_decode_returns(&raw).unwrap();
            assert_eq!(local_output.amount_in, on_chain.amountIn);
        }
    }
}
