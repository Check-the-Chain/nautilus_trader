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

use alloy::{dyn_abi::SolType, primitives::I256, sol, sol_types::SolValue};
use nautilus_model::defi::{
    SharedDex,
    rpc::RpcLog,
    tick_map::{
        tick::PoolTick,
        tick_math::{
            MAX_SQRT_RATIO, MIN_SQRT_RATIO, get_sqrt_ratio_at_tick, get_tick_at_sqrt_ratio,
        },
    },
};

use super::common::{
    hypersync_data, hypersync_metadata, hypersync_topic, parse_indexed_address, parse_pool_id,
    rpc_data, rpc_metadata, validate_topic_count,
};
use crate::{
    events::uniswap_v4_swap::UniswapV4SwapEvent,
    hypersync::{HypersyncLog, helpers::validate_event_signature_hash},
    rpc::helpers as rpc_helpers,
};

/// Canonical `IPoolManager.Swap` event signature.
pub const SWAP_EVENT_SIGNATURE: &str =
    "Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)";
const SWAP_EVENT_SIGNATURE_HASH: &str =
    "40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f";
const MAX_SWAP_FEE: u32 = 1_000_000;

sol! {
    struct SwapEventData {
        int128 amount0;
        int128 amount1;
        uint160 sqrt_price_x96;
        uint128 liquidity;
        int24 tick;
        uint24 fee;
    }
}

fn validate_swap_state(sqrt_price_x96: alloy::primitives::U160, tick: i32) -> anyhow::Result<()> {
    anyhow::ensure!(
        sqrt_price_x96 >= MIN_SQRT_RATIO && sqrt_price_x96 < MAX_SQRT_RATIO,
        "Swap sqrt price is out of bounds: {sqrt_price_x96}"
    );
    anyhow::ensure!(
        (PoolTick::MIN_TICK..=PoolTick::MAX_TICK).contains(&tick),
        "Swap tick is out of bounds: {tick}"
    );

    let price_tick = get_tick_at_sqrt_ratio(sqrt_price_x96);
    let is_boundary_predecessor = price_tick.checked_sub(1) == Some(tick)
        && sqrt_price_x96 == get_sqrt_ratio_at_tick(price_tick);
    anyhow::ensure!(
        tick == price_tick || is_boundary_predecessor,
        "Swap tick {tick} is inconsistent with sqrt price tick {price_tick}"
    );
    Ok(())
}

fn decode_event(
    dex: SharedDex,
    pool_id: alloy::primitives::B256,
    sender: alloy::primitives::Address,
    data: &[u8],
    metadata: super::common::EventMetadata,
) -> anyhow::Result<UniswapV4SwapEvent> {
    let decoded = <SwapEventData as SolType>::abi_decode(data)
        .map_err(|error| anyhow::anyhow!("Failed to decode Swap event data: {error}"))?;
    anyhow::ensure!(
        decoded.abi_encode() == data,
        "Swap event data is not canonical ABI"
    );
    let tick = i32::try_from(decoded.tick)
        .map_err(|error| anyhow::anyhow!("Invalid Swap tick: {error}"))?;
    let fee = decoded.fee.to::<u32>();
    anyhow::ensure!(fee <= MAX_SWAP_FEE, "Swap fee exceeds 100%: {fee}");
    validate_swap_state(decoded.sqrt_price_x96, tick)?;
    let amount0 = I256::try_from(decoded.amount0)
        .map_err(|error| anyhow::anyhow!("Invalid Swap amount0: {error}"))?;
    let amount1 = I256::try_from(decoded.amount1)
        .map_err(|error| anyhow::anyhow!("Invalid Swap amount1: {error}"))?;

    Ok(UniswapV4SwapEvent::new(
        dex,
        pool_id,
        metadata.block_number,
        metadata.transaction_hash,
        metadata.transaction_index,
        metadata.log_index,
        sender,
        amount0,
        amount1,
        decoded.sqrt_price_x96,
        decoded.liquidity,
        tick,
        fee,
    ))
}

/// Parses an `IPoolManager.Swap` event from a HyperSync log.
///
/// # Errors
///
/// Returns an error for a malformed log or an invalid V4 pool state payload.
pub fn parse_swap_event_hypersync(
    dex: SharedDex,
    log: &HypersyncLog,
) -> anyhow::Result<UniswapV4SwapEvent> {
    validate_event_signature_hash("Swap", SWAP_EVENT_SIGNATURE_HASH, log)?;
    validate_topic_count(log.topics.len(), 3, "Swap")?;
    let pool_id = parse_pool_id(hypersync_topic(log, 1, "PoolId")?)?;
    let sender = parse_indexed_address(hypersync_topic(log, 2, "sender")?, "Sender")?;
    let data = hypersync_data(log, 6 * 32, "Swap")?;
    decode_event(dex, pool_id, sender, data, hypersync_metadata(log)?)
}

/// Parses an `IPoolManager.Swap` event from an Ethereum RPC log.
///
/// # Errors
///
/// Returns an error for a malformed log or an invalid V4 pool state payload.
pub fn parse_swap_event_rpc(dex: SharedDex, log: &RpcLog) -> anyhow::Result<UniswapV4SwapEvent> {
    rpc_helpers::validate_event_signature(log, SWAP_EVENT_SIGNATURE_HASH, "Swap")?;
    validate_topic_count(log.topics.len(), 3, "Swap")?;
    let pool_id = parse_pool_id(&rpc_helpers::extract_topic_bytes(log, 1)?)?;
    let sender = parse_indexed_address(&rpc_helpers::extract_topic_bytes(log, 2)?, "Sender")?;
    let data = rpc_data(log, 6 * 32, "Swap")?;
    decode_event(dex, pool_id, sender, &data, rpc_metadata(log)?)
}

#[cfg(test)]
mod tests {
    use alloy::{
        primitives::{Address, B256, U160, aliases::I24, aliases::U24, keccak256},
        sol_types::SolValue,
    };
    use nautilus_core::hex;

    use super::*;
    use crate::{exchanges::base, exchanges::parsing::uniswap_v4::common::abi_generated_logs};

    fn topic_for_address(address: Address) -> String {
        let mut topic = [0_u8; 32];
        topic[12..].copy_from_slice(address.as_slice());
        hex::encode_prefixed(topic)
    }

    fn logs(tick: i32, fee: u32) -> (HypersyncLog, RpcLog) {
        let data = SwapEventData {
            amount0: -125,
            amount1: 250,
            sqrt_price_x96: U160::from(1_u8) << 96,
            liquidity: 50_000,
            tick: I24::try_from(tick).unwrap(),
            fee: U24::from(fee),
        }
        .abi_encode();
        abi_generated_logs(
            vec![
                hex::encode_prefixed(keccak256(SWAP_EVENT_SIGNATURE)),
                B256::repeat_byte(0x11).to_string(),
                topic_for_address(Address::repeat_byte(0x22)),
            ],
            data,
        )
    }

    #[test]
    fn swap_signature_hash_matches_official_signature() {
        assert_eq!(
            hex::encode(keccak256(SWAP_EVENT_SIGNATURE)),
            SWAP_EVENT_SIGNATURE_HASH
        );
    }

    #[test]
    fn parses_signed_deltas_with_hypersync_rpc_parity() {
        let (hypersync, rpc) = logs(0, 3_000);
        let dex = base::UNISWAP_V4.dex.clone();
        let hypersync_event = parse_swap_event_hypersync(dex.clone(), &hypersync).unwrap();
        let rpc_event = parse_swap_event_rpc(dex, &rpc).unwrap();

        assert_eq!(hypersync_event.pool_id, rpc_event.pool_id);
        assert_eq!(hypersync_event.sender, rpc_event.sender);
        assert_eq!(hypersync_event.amount0, I256::try_from(-125_i128).unwrap());
        assert_eq!(hypersync_event.amount1, I256::try_from(250_i128).unwrap());
        assert_eq!(hypersync_event.amount0, rpc_event.amount0);
        assert_eq!(hypersync_event.amount1, rpc_event.amount1);
        assert_eq!(hypersync_event.fee, 3_000);
        assert_eq!(hypersync_event.transaction_hash, rpc_event.transaction_hash);
        assert_eq!(
            hypersync_event.transaction_index,
            rpc_event.transaction_index
        );
        assert_eq!(hypersync_event.log_index, rpc_event.log_index);
    }

    #[test]
    fn accepts_boundary_predecessor_tick() {
        let (_, rpc) = logs(-1, 500);
        assert!(parse_swap_event_rpc(base::UNISWAP_V4.dex.clone(), &rpc).is_ok());
    }

    #[test]
    fn rejects_noncanonical_sender_and_inconsistent_tick() {
        let (_, mut rpc) = logs(0, 500);
        rpc.topics[2].replace_range(2..4, "01");
        assert!(
            parse_swap_event_rpc(base::UNISWAP_V4.dex.clone(), &rpc)
                .unwrap_err()
                .to_string()
                .contains("non-zero address padding")
        );

        let (_, rpc) = logs(10, 500);
        assert!(
            parse_swap_event_rpc(base::UNISWAP_V4.dex.clone(), &rpc)
                .unwrap_err()
                .to_string()
                .contains("inconsistent")
        );
    }

    #[test]
    fn rejects_wrong_topic_count_and_trailing_data() {
        let (_, mut rpc) = logs(0, 500);
        rpc.topics.push(B256::ZERO.to_string());
        assert!(parse_swap_event_rpc(base::UNISWAP_V4.dex.clone(), &rpc).is_err());

        let (_, mut rpc) = logs(0, 500);
        rpc.data.push_str("00");
        assert!(parse_swap_event_rpc(base::UNISWAP_V4.dex.clone(), &rpc).is_err());
    }
}
