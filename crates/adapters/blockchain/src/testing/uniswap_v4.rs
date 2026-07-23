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

//! Historical Uniswap v4 transition differential testing infrastructure.

use std::{str::FromStr, sync::Arc};

use alloy::primitives::{Address, B256, I256, U256, keccak256};
use anyhow::Context;
use nautilus_model::defi::{SharedDex, rpc::RpcLog};
use serde::Deserialize;

use crate::{
    contracts::uniswap_v4_state_view::{UniswapV4PoolState, UniswapV4StateViewContract},
    exchanges::parsing::uniswap_v4::{
        modify_liquidity::{MODIFY_LIQUIDITY_EVENT_SIGNATURE, parse_modify_liquidity_event_rpc},
        protocol_fee_updated::{
            PROTOCOL_FEE_UPDATED_EVENT_SIGNATURE, parse_protocol_fee_updated_event_rpc,
        },
        swap::{SWAP_EVENT_SIGNATURE, parse_swap_event_rpc},
    },
    rpc::{helpers as rpc_helpers, http::BlockchainHttpRpcClient},
    services::{
        UniswapV4EventPosition, UniswapV4Mirror, UniswapV4MirrorConfig, UniswapV4QuoteDirection,
        UniswapV4QuoteEngine, UniswapV4QuoteRequest, UniswapV4QuoteResult,
    },
};

/// Canonical identity and parent linkage for one numbered block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalBlockRef {
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
}

/// Immutable inputs selecting one historical pool transition.
#[derive(Debug, Clone)]
pub struct UniswapV4HistoricalTransitionCase {
    pub dex: SharedDex,
    pub pool_manager: Address,
    pub state_view_address: Address,
    pub mirror_config: UniswapV4MirrorConfig,
    pub block_number: u64,
    pub multicall_calls_per_rpc_request: u32,
}

/// Number-pinned state and logs needed for a pure historical transition replay.
#[derive(Debug, Clone)]
pub struct UniswapV4HistoricalTransitionFixture {
    pub case: UniswapV4HistoricalTransitionCase,
    pub parent_block: CanonicalBlockRef,
    pub target_block: CanonicalBlockRef,
    pub parent_state: UniswapV4PoolState,
    pub target_state: UniswapV4PoolState,
    pub logs: Vec<RpcLog>,
}

/// Which observed amount semantics exactly reproduced a real Swap event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniswapV4ObservedSwapMatch {
    ExactInput,
    ExactOutput,
    Both,
}

/// Differential validation result for one Swap event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniswapV4SwapValidation {
    pub position: UniswapV4EventPosition,
    pub transaction_hash: String,
    pub match_mode: UniswapV4ObservedSwapMatch,
    pub quote_result: UniswapV4QuoteResult,
}

/// Successful replay report for one complete block transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniswapV4HistoricalTransitionReport {
    pub block: CanonicalBlockRef,
    pub swap_count: usize,
    pub modify_liquidity_count: usize,
    pub protocol_fee_updated_count: usize,
    pub swaps: Vec<UniswapV4SwapValidation>,
    pub final_state_validated: bool,
}

#[derive(Debug, Deserialize)]
struct CanonicalBlockResponse {
    number: String,
    hash: String,
    #[serde(rename = "parentHash")]
    parent_hash: String,
}

impl CanonicalBlockResponse {
    fn parse(self, expected_number: u64) -> anyhow::Result<CanonicalBlockRef> {
        let number = rpc_helpers::parse_hex_u64(&self.number)
            .context("canonical block has an invalid number")?;
        anyhow::ensure!(
            number == expected_number,
            "RPC returned canonical block {number}, requested {expected_number}"
        );
        let hash = B256::from_str(&self.hash).context("canonical block has an invalid hash")?;
        let parent_hash = B256::from_str(&self.parent_hash)
            .context("canonical block has an invalid parent hash")?;
        Ok(CanonicalBlockRef {
            number,
            hash,
            parent_hash,
        })
    }
}

/// Fetches a reorg-checked historical transition fixture from an archive RPC endpoint.
///
/// # Errors
///
/// Returns an error if block identities are malformed or change during acquisition, the target is
/// not the canonical child of its requested parent, StateView is bound to another PoolManager, a
/// number-pinned state read fails, or any selected log has incomplete or inconsistent metadata.
pub async fn fetch_uniswap_v4_historical_transition_fixture(
    client: Arc<BlockchainHttpRpcClient>,
    case: UniswapV4HistoricalTransitionCase,
) -> anyhow::Result<UniswapV4HistoricalTransitionFixture> {
    anyhow::ensure!(
        case.block_number > 0,
        "target block number must be greater than zero"
    );
    let parent_number = case.block_number - 1;
    let parent_block = fetch_canonical_block(&client, parent_number)
        .await
        .context("failed to fetch canonical parent block")?;
    let target_block = fetch_canonical_block(&client, case.block_number)
        .await
        .context("failed to fetch canonical target block")?;
    validate_block_pair(&parent_block, &target_block, case.block_number)?;

    let state_view =
        UniswapV4StateViewContract::new(Arc::clone(&client), case.multicall_calls_per_rpc_request);
    let actual_pool_manager = state_view
        .pool_manager(&case.state_view_address, case.block_number)
        .await
        .context("failed to fetch StateView PoolManager binding at target block")?;
    anyhow::ensure!(
        actual_pool_manager == case.pool_manager,
        "StateView is bound to PoolManager {actual_pool_manager}, expected {}",
        case.pool_manager
    );

    let parent_state = state_view
        .fetch_pool_state(
            &case.state_view_address,
            case.mirror_config.pool_id(),
            case.mirror_config.tick_spacing(),
            parent_number,
        )
        .await
        .context("failed to fetch parent StateView pool state")?;
    let target_state = state_view
        .fetch_pool_state(
            &case.state_view_address,
            case.mirror_config.pool_id(),
            case.mirror_config.tick_spacing(),
            case.block_number,
        )
        .await
        .context("failed to fetch target StateView pool state")?;

    let signatures = operational_signatures();
    let logs = client
        .get_logs_with_topic_alternatives(
            Some(&case.pool_manager),
            &[signatures.to_vec(), vec![case.mirror_config.pool_id()]],
            case.block_number,
            case.block_number,
        )
        .await
        .context("failed to fetch selected pool transition logs")?;
    let mut positioned_logs = Vec::with_capacity(logs.len());
    for log in logs {
        validate_log_metadata(&log, &case, &target_block)
            .context("selected pool log failed canonical metadata validation")?;
        positioned_logs.push((log_position(&log)?, log));
    }
    positioned_logs.sort_by_key(|(position, _)| *position);
    let logs = positioned_logs
        .into_iter()
        .map(|(_, log)| log)
        .collect::<Vec<_>>();
    validate_strict_log_order(&logs)?;

    let parent_after = fetch_canonical_block(&client, parent_number)
        .await
        .context("failed to re-fetch canonical parent block")?;
    let target_after = fetch_canonical_block(&client, case.block_number)
        .await
        .context("failed to re-fetch canonical target block")?;
    anyhow::ensure!(
        parent_after == parent_block,
        "canonical parent block identity changed during fixture acquisition"
    );
    anyhow::ensure!(
        target_after == target_block,
        "canonical target block identity changed during fixture acquisition"
    );
    validate_block_pair(&parent_after, &target_after, case.block_number)?;

    Ok(UniswapV4HistoricalTransitionFixture {
        case,
        parent_block,
        target_block,
        parent_state,
        target_state,
        logs,
    })
}

/// Replays a historical fixture and differentially validates each Swap against a quote engine.
///
/// # Errors
///
/// Returns an error for inconsistent fixture metadata, parser or mirror failures, a Swap for which
/// neither observed amount interpretation reproduces the event, disagreement between a prediction
/// and the authoritative post-event mirror, a fixture without a Swap, or final StateView drift.
pub fn validate_uniswap_v4_historical_transition(
    fixture: &UniswapV4HistoricalTransitionFixture,
    quote_engine: &dyn UniswapV4QuoteEngine,
) -> anyhow::Result<UniswapV4HistoricalTransitionReport> {
    validate_fixture_metadata(fixture)?;
    let mut mirror = UniswapV4Mirror::bootstrap(fixture.case.mirror_config, &fixture.parent_state)
        .context("failed to bootstrap parent mirror")?;
    let signatures = operational_signatures();
    let mut swaps = Vec::new();
    let mut modify_liquidity_count = 0;
    let mut protocol_fee_updated_count = 0;

    for log in &fixture.logs {
        let position = log_position(log)?;
        let signature = log_signature(log)?;
        if signature == signatures[0] {
            let event = parse_swap_event_rpc(fixture.case.dex.clone(), log)
                .with_context(|| format!("failed to parse Swap at {position:?}"))?;
            let (direction, input, output) = observed_swap_amounts(event.amount0, event.amount1)
                .with_context(|| format!("invalid Swap signs at {position:?}"))?;
            let exact_input = quote_engine.quote(
                &mirror,
                &UniswapV4QuoteRequest::exact_input(direction, input, None),
            );
            let exact_output = quote_engine.quote(
                &mirror,
                &UniswapV4QuoteRequest::exact_output(direction, output, None),
            );
            let input_matches = exact_input
                .as_ref()
                .is_ok_and(|quote| quote_matches_event(quote, &event, mirror.watermark()));
            let output_matches = exact_output
                .as_ref()
                .is_ok_and(|quote| quote_matches_event(quote, &event, mirror.watermark()));
            anyhow::ensure!(
                input_matches || output_matches,
                "neither quote candidate reproduced Swap at {position:?}; exact-input: {}; exact-output: {}",
                quote_attempt_summary(&exact_input),
                quote_attempt_summary(&exact_output)
            );

            let (match_mode, quote_result) = match (input_matches, output_matches) {
                (true, true) => {
                    let input_quote = exact_input.as_ref().map_err(|error| {
                        anyhow::anyhow!("matched exact-input quote failed: {error}")
                    })?;
                    let output_quote = exact_output.as_ref().map_err(|error| {
                        anyhow::anyhow!("matched exact-output quote failed: {error}")
                    })?;
                    anyhow::ensure!(
                        predicted_scalar_state_matches(input_quote, output_quote),
                        "exact-input and exact-output predictions disagree at {position:?}"
                    );
                    (UniswapV4ObservedSwapMatch::Both, input_quote.clone())
                }
                (true, false) => (
                    UniswapV4ObservedSwapMatch::ExactInput,
                    exact_input.context("matched exact-input quote unexpectedly failed")?,
                ),
                (false, true) => (
                    UniswapV4ObservedSwapMatch::ExactOutput,
                    exact_output.context("matched exact-output quote unexpectedly failed")?,
                ),
                (false, false) => anyhow::bail!(
                    "candidate classification failed after requiring a match at {position:?}"
                ),
            };

            mirror
                .apply_swap(&event)
                .with_context(|| format!("failed to apply authoritative Swap at {position:?}"))?;
            anyhow::ensure!(
                mirror.sqrt_price_x96() == quote_result.sqrt_price_x96_after
                    && mirror.tick() == quote_result.tick_after
                    && mirror.liquidity() == quote_result.liquidity_after,
                "authoritative mirror state disagrees with prediction at {position:?}"
            );
            swaps.push(UniswapV4SwapValidation {
                position,
                transaction_hash: event.transaction_hash,
                match_mode,
                quote_result,
            });
        } else if signature == signatures[1] {
            let event = parse_modify_liquidity_event_rpc(fixture.case.dex.clone(), log)
                .with_context(|| format!("failed to parse ModifyLiquidity at {position:?}"))?;
            mirror
                .apply_modify_liquidity(&event)
                .with_context(|| format!("failed to apply ModifyLiquidity at {position:?}"))?;
            modify_liquidity_count += 1;
        } else if signature == signatures[2] {
            let event = parse_protocol_fee_updated_event_rpc(fixture.case.dex.clone(), log)
                .with_context(|| format!("failed to parse ProtocolFeeUpdated at {position:?}"))?;
            mirror
                .apply_protocol_fee_updated(&event)
                .with_context(|| format!("failed to apply ProtocolFeeUpdated at {position:?}"))?;
            protocol_fee_updated_count += 1;
        } else {
            anyhow::bail!("unexpected operational signature {signature} at {position:?}");
        }
    }

    anyhow::ensure!(
        !swaps.is_empty(),
        "historical transition fixture contains no Swap events"
    );
    mirror
        .validate_against_snapshot(&fixture.target_state)
        .context("final replay mirror differs from target StateView state")?;

    Ok(UniswapV4HistoricalTransitionReport {
        block: fixture.target_block,
        swap_count: swaps.len(),
        modify_liquidity_count,
        protocol_fee_updated_count,
        swaps,
        final_state_validated: true,
    })
}

/// Fetches and validates one historical transition in a single call.
///
/// # Errors
///
/// Returns any fixture acquisition or pure replay validation error.
pub async fn validate_uniswap_v4_historical_transition_live(
    client: Arc<BlockchainHttpRpcClient>,
    case: UniswapV4HistoricalTransitionCase,
    quote_engine: &dyn UniswapV4QuoteEngine,
) -> anyhow::Result<UniswapV4HistoricalTransitionReport> {
    let fixture = fetch_uniswap_v4_historical_transition_fixture(client, case).await?;
    validate_uniswap_v4_historical_transition(&fixture, quote_engine)
}

async fn fetch_canonical_block(
    client: &BlockchainHttpRpcClient,
    block_number: u64,
) -> anyhow::Result<CanonicalBlockRef> {
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBlockByNumber",
        "params": [format!("0x{block_number:x}"), false]
    });
    let response: CanonicalBlockResponse = client
        .execute_rpc_call(request)
        .await
        .with_context(|| format!("eth_getBlockByNumber failed for block {block_number}"))?;
    response.parse(block_number)
}

fn operational_signatures() -> [B256; 3] {
    [
        keccak256(SWAP_EVENT_SIGNATURE),
        keccak256(MODIFY_LIQUIDITY_EVENT_SIGNATURE),
        keccak256(PROTOCOL_FEE_UPDATED_EVENT_SIGNATURE),
    ]
}

fn validate_block_pair(
    parent: &CanonicalBlockRef,
    target: &CanonicalBlockRef,
    expected_target: u64,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        target.number == expected_target,
        "target block number mismatch"
    );
    anyhow::ensure!(
        parent.number.checked_add(1) == Some(target.number),
        "target block is not immediately after parent block"
    );
    anyhow::ensure!(
        target.parent_hash == parent.hash,
        "target parent hash does not match canonical parent hash"
    );
    Ok(())
}

fn validate_fixture_metadata(fixture: &UniswapV4HistoricalTransitionFixture) -> anyhow::Result<()> {
    anyhow::ensure!(
        fixture.case.block_number > 0,
        "target block number must be greater than zero"
    );
    validate_block_pair(
        &fixture.parent_block,
        &fixture.target_block,
        fixture.case.block_number,
    )?;
    anyhow::ensure!(
        fixture.parent_state.block_number == fixture.parent_block.number,
        "parent StateView snapshot block does not match canonical parent"
    );
    anyhow::ensure!(
        fixture.target_state.block_number == fixture.target_block.number,
        "target StateView snapshot block does not match canonical target"
    );
    for log in &fixture.logs {
        validate_log_metadata(log, &fixture.case, &fixture.target_block)?;
    }
    validate_strict_log_order(&fixture.logs)
}

fn validate_log_metadata(
    log: &RpcLog,
    case: &UniswapV4HistoricalTransitionCase,
    target: &CanonicalBlockRef,
) -> anyhow::Result<()> {
    anyhow::ensure!(!log.removed, "selected log has removed=true");
    let emitter = Address::from_str(&log.address).context("selected log has an invalid emitter")?;
    anyhow::ensure!(
        emitter == case.pool_manager,
        "selected log emitter {emitter} does not match PoolManager {}",
        case.pool_manager
    );
    let position = log_position(log)?;
    anyhow::ensure!(
        position.block_number == case.block_number,
        "selected log is from block {}, expected {}",
        position.block_number,
        case.block_number
    );
    let block_hash = B256::from_str(
        log.block_hash
            .as_deref()
            .context("selected log is missing block hash")?,
    )
    .context("selected log has an invalid block hash")?;
    anyhow::ensure!(
        block_hash == target.hash,
        "selected log block hash {block_hash} does not match canonical target {}",
        target.hash
    );
    B256::from_str(
        log.transaction_hash
            .as_deref()
            .context("selected log is missing transaction hash")?,
    )
    .context("selected log has an invalid transaction hash")?;
    let signature = log_signature(log)?;
    anyhow::ensure!(
        operational_signatures().contains(&signature),
        "selected log has unknown operational signature {signature}"
    );
    let pool_id = B256::from_str(
        log.topics
            .get(1)
            .context("selected log is missing pool ID topic")?,
    )
    .context("selected log has an invalid pool ID topic")?;
    anyhow::ensure!(
        pool_id == case.mirror_config.pool_id(),
        "selected log belongs to pool {pool_id}, expected {}",
        case.mirror_config.pool_id()
    );
    Ok(())
}

fn validate_strict_log_order(logs: &[RpcLog]) -> anyhow::Result<()> {
    let mut previous = None;
    for log in logs {
        let position = log_position(log)?;
        if let Some(previous) = previous {
            anyhow::ensure!(
                position > previous,
                "logs are not strictly ordered: {position:?} follows {previous:?}"
            );
        }
        previous = Some(position);
    }
    Ok(())
}

fn log_position(log: &RpcLog) -> anyhow::Result<UniswapV4EventPosition> {
    Ok(UniswapV4EventPosition::new(
        rpc_helpers::extract_block_number(log).context("selected log has invalid block number")?,
        rpc_helpers::extract_transaction_index(log)
            .context("selected log has invalid transaction index")?,
        rpc_helpers::extract_log_index(log).context("selected log has invalid log index")?,
    ))
}

fn log_signature(log: &RpcLog) -> anyhow::Result<B256> {
    B256::from_str(
        log.topics
            .first()
            .context("selected log is missing event signature topic")?,
    )
    .context("selected log has an invalid event signature topic")
}

fn observed_swap_amounts(
    amount0: I256,
    amount1: I256,
) -> anyhow::Result<(UniswapV4QuoteDirection, U256, U256)> {
    if amount0 < I256::ZERO && amount1 >= I256::ZERO {
        Ok((
            UniswapV4QuoteDirection::ZeroForOne,
            amount0.unsigned_abs(),
            amount1.into_raw(),
        ))
    } else if amount1 < I256::ZERO && amount0 >= I256::ZERO {
        Ok((
            UniswapV4QuoteDirection::OneForZero,
            amount1.unsigned_abs(),
            amount0.into_raw(),
        ))
    } else {
        anyhow::bail!(
            "expected one negative input and one non-negative output, got {amount0}/{amount1}"
        )
    }
}

fn quote_matches_event(
    quote: &UniswapV4QuoteResult,
    event: &crate::events::uniswap_v4_swap::UniswapV4SwapEvent,
    expected_watermark: UniswapV4EventPosition,
) -> bool {
    quote.amount0 == event.amount0
        && quote.amount1 == event.amount1
        && quote.sqrt_price_x96_after == event.sqrt_price_x96
        && quote.tick_after == event.tick
        && quote.liquidity_after == event.liquidity
        && quote.effective_fee_pips == event.fee
        && quote.mirror_watermark == expected_watermark
}

fn predicted_scalar_state_matches(
    left: &UniswapV4QuoteResult,
    right: &UniswapV4QuoteResult,
) -> bool {
    left.sqrt_price_x96_after == right.sqrt_price_x96_after
        && left.tick_after == right.tick_after
        && left.liquidity_after == right.liquidity_after
        && left.effective_fee_pips == right.effective_fee_pips
        && left.mirror_watermark == right.mirror_watermark
}

fn quote_attempt_summary(
    result: &Result<UniswapV4QuoteResult, crate::services::UniswapV4QuoteError>,
) -> String {
    match result {
        Ok(_) => "quote succeeded but fields differed".to_string(),
        Err(error) => format!("quote failed: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use alloy::{
        primitives::{U160, aliases::I24, aliases::U24},
        sol,
        sol_types::SolValue,
    };
    use nautilus_core::hex;

    use super::*;
    use crate::{
        contracts::uniswap_v4_state_view::{UniswapV4Slot0State, UniswapV4TickLiquidityState},
        exchanges::base,
        services::{ExactUniswapV4QuoteEngine, UniswapV4QuoteAmount, UniswapV4QuoteError},
    };

    sol! {
        struct TestSwapEventData {
            int128 amount0;
            int128 amount1;
            uint160 sqrtPriceX96;
            uint128 liquidity;
            int24 tick;
            uint24 fee;
        }
    }

    const PARENT_NUMBER: u64 = 10;
    const TARGET_NUMBER: u64 = 11;
    const LIQUIDITY: u128 = 1_000_000;

    fn test_case() -> UniswapV4HistoricalTransitionCase {
        let currency0 = Address::repeat_byte(0x11);
        let currency1 = Address::repeat_byte(0x22);
        let pool_id = keccak256(
            (
                currency0,
                currency1,
                U24::from(3_000),
                I24::try_from(60).unwrap(),
                Address::ZERO,
            )
                .abi_encode(),
        );
        UniswapV4HistoricalTransitionCase {
            dex: base::UNISWAP_V4.dex.clone(),
            pool_manager: Address::repeat_byte(0x44),
            state_view_address: Address::repeat_byte(0x55),
            mirror_config: UniswapV4MirrorConfig::new(
                pool_id,
                currency0,
                currency1,
                60,
                3_000,
                Address::ZERO,
            )
            .unwrap(),
            block_number: TARGET_NUMBER,
            multicall_calls_per_rpc_request: 100,
        }
    }

    fn parent_state(case: &UniswapV4HistoricalTransitionCase) -> UniswapV4PoolState {
        UniswapV4PoolState {
            pool_id: case.mirror_config.pool_id(),
            tick_spacing: 60,
            block_number: PARENT_NUMBER,
            slot0: UniswapV4Slot0State {
                sqrt_price_x96: U160::from(1_u8) << 96,
                tick: 0,
                protocol_fee: 0,
                lp_fee: 3_000,
            },
            liquidity: LIQUIDITY,
            ticks: vec![
                UniswapV4TickLiquidityState {
                    tick: -600,
                    liquidity_gross: LIQUIDITY,
                    liquidity_net: LIQUIDITY as i128,
                },
                UniswapV4TickLiquidityState {
                    tick: 600,
                    liquidity_gross: LIQUIDITY,
                    liquidity_net: -(LIQUIDITY as i128),
                },
            ],
        }
    }

    fn deterministic_fixture() -> UniswapV4HistoricalTransitionFixture {
        let case = test_case();
        let parent_state = parent_state(&case);
        let mirror = UniswapV4Mirror::bootstrap(case.mirror_config, &parent_state).unwrap();
        let quote = ExactUniswapV4QuoteEngine
            .quote(
                &mirror,
                &UniswapV4QuoteRequest::exact_input(
                    UniswapV4QuoteDirection::ZeroForOne,
                    U256::from(1_000),
                    None,
                ),
            )
            .unwrap();
        let parent_hash = B256::repeat_byte(0x10);
        let target_hash = B256::repeat_byte(0x11);
        let transaction_hash = B256::repeat_byte(0x77);
        let mut sender_topic = [0_u8; 32];
        sender_topic[12..].copy_from_slice(Address::repeat_byte(0x66).as_slice());
        let data = TestSwapEventData {
            amount0: i128::try_from(quote.amount0).unwrap(),
            amount1: i128::try_from(quote.amount1).unwrap(),
            sqrtPriceX96: quote.sqrt_price_x96_after,
            liquidity: quote.liquidity_after,
            tick: I24::try_from(quote.tick_after).unwrap(),
            fee: U24::from(quote.effective_fee_pips),
        }
        .abi_encode();
        let log = RpcLog {
            removed: false,
            log_index: Some("0x0".to_string()),
            transaction_index: Some("0x0".to_string()),
            transaction_hash: Some(transaction_hash.to_string()),
            block_hash: Some(target_hash.to_string()),
            block_number: Some("0xb".to_string()),
            address: case.pool_manager.to_string(),
            data: hex::encode_prefixed(data),
            topics: vec![
                keccak256(SWAP_EVENT_SIGNATURE).to_string(),
                case.mirror_config.pool_id().to_string(),
                hex::encode_prefixed(sender_topic),
            ],
        };
        let target_state = UniswapV4PoolState {
            pool_id: parent_state.pool_id,
            tick_spacing: parent_state.tick_spacing,
            block_number: TARGET_NUMBER,
            slot0: UniswapV4Slot0State {
                sqrt_price_x96: quote.sqrt_price_x96_after,
                tick: quote.tick_after,
                protocol_fee: parent_state.slot0.protocol_fee,
                lp_fee: parent_state.slot0.lp_fee,
            },
            liquidity: quote.liquidity_after,
            ticks: parent_state.ticks.clone(),
        };
        UniswapV4HistoricalTransitionFixture {
            case,
            parent_block: CanonicalBlockRef {
                number: PARENT_NUMBER,
                hash: parent_hash,
                parent_hash: B256::repeat_byte(0x09),
            },
            target_block: CanonicalBlockRef {
                number: TARGET_NUMBER,
                hash: target_hash,
                parent_hash,
            },
            parent_state,
            target_state,
            logs: vec![log],
        }
    }

    #[test]
    fn deterministic_fixture_classifies_candidate_and_validates_final_state() {
        let report = validate_uniswap_v4_historical_transition(
            &deterministic_fixture(),
            &ExactUniswapV4QuoteEngine,
        )
        .unwrap();

        assert_eq!(report.swap_count, 1);
        assert_eq!(
            report.swaps[0].match_mode,
            UniswapV4ObservedSwapMatch::ExactInput
        );
        assert!(report.final_state_validated);
    }

    #[test]
    fn quote_engine_substitution_can_classify_exact_input_only() {
        struct InputOnlyEngine;

        impl UniswapV4QuoteEngine for InputOnlyEngine {
            fn quote(
                &self,
                mirror: &UniswapV4Mirror,
                request: &UniswapV4QuoteRequest,
            ) -> Result<UniswapV4QuoteResult, UniswapV4QuoteError> {
                if matches!(request.amount, UniswapV4QuoteAmount::ExactOutput(_)) {
                    return Err(UniswapV4QuoteError::ZeroAmount);
                }
                ExactUniswapV4QuoteEngine.quote(mirror, request)
            }
        }

        let report =
            validate_uniswap_v4_historical_transition(&deterministic_fixture(), &InputOnlyEngine)
                .unwrap();
        assert_eq!(
            report.swaps[0].match_mode,
            UniswapV4ObservedSwapMatch::ExactInput
        );
    }

    #[test]
    fn canonical_block_parser_and_linkage_fail_closed() {
        let parsed = CanonicalBlockResponse {
            number: "0xb".to_string(),
            hash: B256::repeat_byte(0x11).to_string(),
            parent_hash: B256::repeat_byte(0x10).to_string(),
        }
        .parse(TARGET_NUMBER)
        .unwrap();
        assert_eq!(parsed.number, TARGET_NUMBER);

        assert!(
            CanonicalBlockResponse {
                number: "0xb".to_string(),
                hash: "0x01".to_string(),
                parent_hash: B256::repeat_byte(0x10).to_string(),
            }
            .parse(TARGET_NUMBER)
            .is_err()
        );

        let mut fixture = deterministic_fixture();
        fixture.target_block.parent_hash = B256::repeat_byte(0xff);
        assert!(
            validate_uniswap_v4_historical_transition(&fixture, &ExactUniswapV4QuoteEngine)
                .unwrap_err()
                .to_string()
                .contains("parent hash")
        );
    }

    #[test]
    fn duplicate_or_incomplete_log_metadata_is_rejected_before_replay() {
        let mut duplicate = deterministic_fixture();
        duplicate.logs.push(duplicate.logs[0].clone());
        assert!(
            validate_uniswap_v4_historical_transition(&duplicate, &ExactUniswapV4QuoteEngine)
                .unwrap_err()
                .to_string()
                .contains("strictly ordered")
        );

        let mut incomplete = deterministic_fixture();
        incomplete.logs[0].transaction_index = None;
        assert!(
            validate_uniswap_v4_historical_transition(&incomplete, &ExactUniswapV4QuoteEngine)
                .unwrap_err()
                .to_string()
                .contains("transaction index")
        );
    }
}
