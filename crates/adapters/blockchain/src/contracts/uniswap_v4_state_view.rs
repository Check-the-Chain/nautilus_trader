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

use std::sync::Arc;

use alloy::{
    primitives::{Address, B256, U160, U256},
    sol,
    sol_types::{SolCall, private::primitives::aliases::I24},
};
use nautilus_core::hex;
use nautilus_model::defi::tick_map::tick::PoolTick;
use thiserror::Error;

use super::base::{BaseContract, ContractCall, Multicall3};
use crate::rpc::{error::BlockchainRpcClientError, http::BlockchainHttpRpcClient};

sol! {
    #[sol(rpc)]
    contract UniswapV4StateView {
        function poolManager() external view returns (address);
        function getSlot0(bytes32 poolId) external view returns (
            uint160 sqrtPriceX96,
            int24 tick,
            uint24 protocolFee,
            uint24 lpFee
        );
        function getLiquidity(bytes32 poolId) external view returns (uint128 liquidity);
        function getTickBitmap(bytes32 poolId, int16 tick) external view returns (uint256 tickBitmap);
        function getTickLiquidity(bytes32 poolId, int24 tick) external view returns (
            uint128 liquidityGross,
            int128 liquidityNet
        );
    }
}

/// Decoded Uniswap v4 slot0 state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UniswapV4Slot0State {
    /// Current square-root price in Q64.96 form.
    pub sqrt_price_x96: U160,
    /// Current pool tick.
    pub tick: i32,
    /// Packed protocol fee returned by StateView.
    pub protocol_fee: u32,
    /// Current LP fee in hundredths of a basis point.
    pub lp_fee: u32,
}

/// Liquidity state for one initialized Uniswap v4 tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UniswapV4TickLiquidityState {
    /// Initialized tick index.
    pub tick: i32,
    /// Total position liquidity referencing the tick.
    pub liquidity_gross: u128,
    /// Net liquidity change when crossing the tick from left to right.
    pub liquidity_net: i128,
}

/// Exact-block state needed to bootstrap a Uniswap v4 pool for quoting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniswapV4PoolState {
    /// Pool ID supplied to StateView.
    pub pool_id: B256,
    /// Tick spacing used to decode the pool bitmap.
    pub tick_spacing: i32,
    /// Block number used for every RPC call in this snapshot.
    pub block_number: u64,
    /// Current slot0 state.
    pub slot0: UniswapV4Slot0State,
    /// Current in-range pool liquidity.
    pub liquidity: u128,
    /// All initialized ticks, sorted by tick index.
    pub ticks: Vec<UniswapV4TickLiquidityState>,
}

/// Errors returned while reading Uniswap v4 StateView state.
#[derive(Debug, Error)]
pub enum UniswapV4StateViewError {
    /// Underlying RPC or multicall failure.
    #[error("RPC error: {0}")]
    RpcError(#[from] BlockchainRpcClientError),
    /// Tick spacing is zero or negative.
    #[error("Tick spacing must be positive, was {tick_spacing}")]
    InvalidTickSpacing { tick_spacing: i32 },
    /// A value could not be represented safely or violated pool bounds.
    #[error("Invalid {field}: {reason}")]
    InvalidValue { field: String, reason: String },
    /// A multicall returned a different number of results than requested.
    #[error("Malformed {operation} result count: expected {expected}, received {actual}")]
    MalformedResultCount {
        operation: String,
        expected: usize,
        actual: usize,
    },
    /// One StateView subcall failed.
    #[error("StateView call failed for {field}")]
    CallFailed { field: String },
    /// A successful StateView subcall returned invalid ABI data.
    #[error("Failed to decode {field}: {reason} (raw data: {raw_data})")]
    DecodingError {
        field: String,
        reason: String,
        raw_data: String,
    },
}

/// Reader for quote-critical Uniswap v4 StateView bootstrap state.
///
/// Every public read requires an explicit block number and all multicall chunks are pinned to that
/// number. `BaseContract` does not currently support EIP-1898 block-hash selectors, so a chain
/// reorganization that replaces the requested number while a multi-request read is in progress
/// cannot be detected by this slice.
#[derive(Debug)]
pub struct UniswapV4StateViewContract {
    base: BaseContract,
}

impl UniswapV4StateViewContract {
    /// Creates a StateView reader with an explicit per-multicall request limit.
    #[must_use]
    pub fn new(client: Arc<BlockchainHttpRpcClient>, multicall_calls_per_rpc_request: u32) -> Self {
        Self {
            base: BaseContract::new_with_multicall_limit(client, multicall_calls_per_rpc_request),
        }
    }

    /// Returns the PoolManager bound to this StateView deployment at an explicit block.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC call fails or the response cannot be decoded.
    pub async fn pool_manager(
        &self,
        state_view_address: &Address,
        block_number: u64,
    ) -> Result<Address, UniswapV4StateViewError> {
        let call_data = UniswapV4StateView::poolManagerCall {}.abi_encode();
        let result = self
            .base
            .execute_call(state_view_address, &call_data, Some(block_number))
            .await?;
        UniswapV4StateView::poolManagerCall::abi_decode_returns(&result)
            .map_err(|error| decode_error("poolManager", error, &result))
    }

    /// Fetches all StateView state required to bootstrap one pool for quoting.
    ///
    /// The first multicall reads slot0, global liquidity, and every bitmap word covering
    /// [`PoolTick::MIN_TICK`] through [`PoolTick::MAX_TICK`]. A second multicall batch reads gross
    /// and net liquidity for every initialized tick found in those words. `BaseContract` chunks
    /// both batches according to `multicall_calls_per_rpc_request` while preserving call order.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid tick spacing, RPC or subcall failures, malformed result counts,
    /// ABI decoding failures, or values which cannot be converted without loss.
    pub async fn fetch_pool_state(
        &self,
        state_view_address: &Address,
        pool_id: B256,
        tick_spacing: i32,
        block_number: u64,
    ) -> Result<UniswapV4PoolState, UniswapV4StateViewError> {
        let word_positions = bitmap_word_positions(tick_spacing)?;
        let expected_result_count = word_positions.len().checked_add(2).ok_or_else(|| {
            UniswapV4StateViewError::InvalidValue {
                field: "bitmap request count".to_string(),
                reason: "overflow".to_string(),
            }
        })?;

        let mut calls = Vec::with_capacity(expected_result_count);
        calls.push(ContractCall {
            target: *state_view_address,
            allow_failure: false,
            call_data: UniswapV4StateView::getSlot0Call { poolId: pool_id }.abi_encode(),
        });
        calls.push(ContractCall {
            target: *state_view_address,
            allow_failure: false,
            call_data: UniswapV4StateView::getLiquidityCall { poolId: pool_id }.abi_encode(),
        });
        for &word_position in &word_positions {
            calls.push(ContractCall {
                target: *state_view_address,
                allow_failure: false,
                call_data: UniswapV4StateView::getTickBitmapCall {
                    poolId: pool_id,
                    tick: word_position,
                }
                .abi_encode(),
            });
        }

        let results = self
            .base
            .execute_multicall(calls, Some(block_number))
            .await?;
        validate_result_count("slot0/liquidity/bitmap", expected_result_count, &results)?;

        let slot0_result = successful_result("getSlot0", &results[0])?;
        let slot0 = UniswapV4StateView::getSlot0Call::abi_decode_returns(slot0_result)
            .map_err(|error| decode_error("getSlot0", error, slot0_result))?;
        let tick =
            i32::try_from(slot0.tick).map_err(|error| UniswapV4StateViewError::InvalidValue {
                field: "getSlot0.tick".to_string(),
                reason: error.to_string(),
            })?;
        let protocol_fee = u32::try_from(slot0.protocolFee).map_err(|error| {
            UniswapV4StateViewError::InvalidValue {
                field: "getSlot0.protocolFee".to_string(),
                reason: error.to_string(),
            }
        })?;
        let lp_fee =
            u32::try_from(slot0.lpFee).map_err(|error| UniswapV4StateViewError::InvalidValue {
                field: "getSlot0.lpFee".to_string(),
                reason: error.to_string(),
            })?;
        let slot0 = UniswapV4Slot0State {
            sqrt_price_x96: slot0.sqrtPriceX96,
            tick,
            protocol_fee,
            lp_fee,
        };

        let liquidity_result = successful_result("getLiquidity", &results[1])?;
        let liquidity = UniswapV4StateView::getLiquidityCall::abi_decode_returns(liquidity_result)
            .map_err(|error| decode_error("getLiquidity", error, liquidity_result))?;

        let mut bitmap_words = Vec::with_capacity(word_positions.len());
        for (&word_position, result) in word_positions.iter().zip(&results[2..]) {
            let field = format!("getTickBitmap({word_position})");
            let raw_result = successful_result(&field, result)?;
            let bitmap = UniswapV4StateView::getTickBitmapCall::abi_decode_returns(raw_result)
                .map_err(|error| decode_error(&field, error, raw_result))?;
            bitmap_words.push((word_position, bitmap));
        }
        let initialized_ticks = decode_initialized_ticks(&bitmap_words, tick_spacing)?;
        let ticks = self
            .fetch_tick_liquidity(
                state_view_address,
                pool_id,
                &initialized_ticks,
                block_number,
            )
            .await?;

        Ok(UniswapV4PoolState {
            pool_id,
            tick_spacing,
            block_number,
            slot0,
            liquidity,
            ticks,
        })
    }

    async fn fetch_tick_liquidity(
        &self,
        state_view_address: &Address,
        pool_id: B256,
        ticks: &[i32],
        block_number: u64,
    ) -> Result<Vec<UniswapV4TickLiquidityState>, UniswapV4StateViewError> {
        let calls = ticks
            .iter()
            .map(|&tick| {
                let encoded_tick =
                    I24::try_from(tick).map_err(|error| UniswapV4StateViewError::InvalidValue {
                        field: format!("getTickLiquidity({tick})"),
                        reason: format!("tick cannot be encoded as int24: {error}"),
                    })?;
                Ok(ContractCall {
                    target: *state_view_address,
                    allow_failure: false,
                    call_data: UniswapV4StateView::getTickLiquidityCall {
                        poolId: pool_id,
                        tick: encoded_tick,
                    }
                    .abi_encode(),
                })
            })
            .collect::<Result<Vec<_>, UniswapV4StateViewError>>()?;
        let results = self
            .base
            .execute_multicall(calls, Some(block_number))
            .await?;
        validate_result_count("tick liquidity", ticks.len(), &results)?;

        ticks
            .iter()
            .zip(&results)
            .map(|(&tick, result)| {
                let field = format!("getTickLiquidity({tick})");
                let raw_result = successful_result(&field, result)?;
                let liquidity =
                    UniswapV4StateView::getTickLiquidityCall::abi_decode_returns(raw_result)
                        .map_err(|error| decode_error(&field, error, raw_result))?;
                Ok(UniswapV4TickLiquidityState {
                    tick,
                    liquidity_gross: liquidity.liquidityGross,
                    liquidity_net: liquidity.liquidityNet,
                })
            })
            .collect()
    }
}

fn bitmap_word_positions(tick_spacing: i32) -> Result<Vec<i16>, UniswapV4StateViewError> {
    if tick_spacing <= 0 {
        return Err(UniswapV4StateViewError::InvalidTickSpacing { tick_spacing });
    }

    let min_compressed = PoolTick::MIN_TICK.div_euclid(tick_spacing);
    let max_compressed = PoolTick::MAX_TICK.div_euclid(tick_spacing);
    let min_word = min_compressed.div_euclid(256);
    let max_word = max_compressed.div_euclid(256);
    let min_word =
        i16::try_from(min_word).map_err(|error| UniswapV4StateViewError::InvalidValue {
            field: "minimum bitmap word".to_string(),
            reason: error.to_string(),
        })?;
    let max_word =
        i16::try_from(max_word).map_err(|error| UniswapV4StateViewError::InvalidValue {
            field: "maximum bitmap word".to_string(),
            reason: error.to_string(),
        })?;

    Ok((min_word..=max_word).collect())
}

fn decode_initialized_ticks(
    bitmap_words: &[(i16, U256)],
    tick_spacing: i32,
) -> Result<Vec<i32>, UniswapV4StateViewError> {
    if tick_spacing <= 0 {
        return Err(UniswapV4StateViewError::InvalidTickSpacing { tick_spacing });
    }

    let mut ticks = Vec::new();
    for &(word_position, bitmap) in bitmap_words {
        for bit_position in 0..256 {
            if !bitmap.bit(bit_position) {
                continue;
            }

            let compressed_tick = i64::from(word_position)
                .checked_mul(256)
                .and_then(|value| value.checked_add(i64::try_from(bit_position).ok()?))
                .ok_or_else(|| UniswapV4StateViewError::InvalidValue {
                    field: "compressed tick".to_string(),
                    reason: format!(
                        "overflow for word {word_position}, bit position {bit_position}"
                    ),
                })?;
            let tick = compressed_tick
                .checked_mul(i64::from(tick_spacing))
                .ok_or_else(|| UniswapV4StateViewError::InvalidValue {
                    field: "initialized tick".to_string(),
                    reason: format!(
                        "overflow for compressed tick {compressed_tick} and spacing {tick_spacing}"
                    ),
                })?;
            let tick =
                i32::try_from(tick).map_err(|error| UniswapV4StateViewError::InvalidValue {
                    field: "initialized tick".to_string(),
                    reason: error.to_string(),
                })?;
            if !(PoolTick::MIN_TICK..=PoolTick::MAX_TICK).contains(&tick) {
                return Err(UniswapV4StateViewError::InvalidValue {
                    field: "initialized tick".to_string(),
                    reason: format!("{tick} is outside Uniswap tick bounds"),
                });
            }
            ticks.push(tick);
        }
    }

    ticks.sort_unstable();
    ticks.dedup();
    Ok(ticks)
}

fn validate_result_count(
    operation: &str,
    expected: usize,
    results: &[Multicall3::Result],
) -> Result<(), UniswapV4StateViewError> {
    if results.len() != expected {
        return Err(UniswapV4StateViewError::MalformedResultCount {
            operation: operation.to_string(),
            expected,
            actual: results.len(),
        });
    }
    Ok(())
}

fn successful_result<'a>(
    field: &str,
    result: &'a Multicall3::Result,
) -> Result<&'a [u8], UniswapV4StateViewError> {
    if !result.success {
        return Err(UniswapV4StateViewError::CallFailed {
            field: field.to_string(),
        });
    }
    Ok(&result.returnData)
}

fn decode_error(
    field: &str,
    error: alloy::sol_types::Error,
    raw_data: &[u8],
) -> UniswapV4StateViewError {
    UniswapV4StateViewError::DecodingError {
        field: field.to_string(),
        reason: error.to_string(),
        raw_data: hex::encode(raw_data),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(1, -3466, 3465, 6932)]
    #[case(10, -347, 346, 694)]
    #[case(60, -58, 57, 116)]
    #[case(200, -18, 17, 36)]
    fn bitmap_range_covers_tick_bounds(
        #[case] tick_spacing: i32,
        #[case] expected_first: i16,
        #[case] expected_last: i16,
        #[case] expected_len: usize,
    ) {
        let words = bitmap_word_positions(tick_spacing).unwrap();

        assert_eq!(words.first(), Some(&expected_first));
        assert_eq!(words.last(), Some(&expected_last));
        assert_eq!(words.len(), expected_len);
    }

    #[rstest]
    fn set_bit_decoding_handles_negative_ticks() {
        let words = vec![
            (-2, U256::from(1) << 255),
            (-1, U256::from(1) | (U256::from(1) << 255)),
            (0, U256::from(1)),
        ];

        assert_eq!(
            decode_initialized_ticks(&words, 1).unwrap(),
            vec![-257, -256, -1, 0]
        );
    }

    #[rstest]
    #[case(0)]
    #[case(-1)]
    fn invalid_tick_spacing_is_rejected(#[case] tick_spacing: i32) {
        assert!(matches!(
            bitmap_word_positions(tick_spacing),
            Err(UniswapV4StateViewError::InvalidTickSpacing { .. })
        ));
        assert!(matches!(
            decode_initialized_ticks(&[], tick_spacing),
            Err(UniswapV4StateViewError::InvalidTickSpacing { .. })
        ));
    }

    #[rstest]
    fn initialized_ticks_are_sorted_deterministically() {
        let words = vec![
            (1, U256::from(1) << 1),
            (-1, U256::from(1) << 255),
            (0, (U256::from(1) << 10) | U256::from(1)),
        ];

        assert_eq!(
            decode_initialized_ticks(&words, 10).unwrap(),
            vec![-10, 0, 100, 2570]
        );
    }
}
