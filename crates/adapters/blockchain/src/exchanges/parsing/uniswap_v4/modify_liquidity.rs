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

use alloy::{dyn_abi::SolType, sol, sol_types::SolValue};
use nautilus_model::defi::{SharedDex, rpc::RpcLog, tick_map::tick::PoolTick};

use super::common::{
    hypersync_data, hypersync_metadata, hypersync_topic, parse_indexed_address, parse_pool_id,
    rpc_data, rpc_metadata, validate_topic_count,
};
use crate::{
    events::modify_liquidity::ModifyLiquidityEvent,
    hypersync::{HypersyncLog, helpers::validate_event_signature_hash},
    rpc::helpers as rpc_helpers,
};

/// Canonical `IPoolManager.ModifyLiquidity` event signature.
pub const MODIFY_LIQUIDITY_EVENT_SIGNATURE: &str =
    "ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)";
const MODIFY_LIQUIDITY_EVENT_SIGNATURE_HASH: &str =
    "f208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec";

sol! {
    struct ModifyLiquidityEventData {
        int24 tick_lower;
        int24 tick_upper;
        int256 liquidity_delta;
        bytes32 salt;
    }
}

fn decode_event(
    dex: SharedDex,
    pool_id: alloy::primitives::B256,
    sender: alloy::primitives::Address,
    data: &[u8],
    metadata: super::common::EventMetadata,
) -> anyhow::Result<ModifyLiquidityEvent> {
    let decoded = <ModifyLiquidityEventData as SolType>::abi_decode(data)
        .map_err(|error| anyhow::anyhow!("Failed to decode ModifyLiquidity event data: {error}"))?;
    anyhow::ensure!(
        decoded.abi_encode() == data,
        "ModifyLiquidity event data is not canonical ABI"
    );
    let tick_lower = i32::try_from(decoded.tick_lower)
        .map_err(|error| anyhow::anyhow!("Invalid lower tick: {error}"))?;
    let tick_upper = i32::try_from(decoded.tick_upper)
        .map_err(|error| anyhow::anyhow!("Invalid upper tick: {error}"))?;
    anyhow::ensure!(
        (PoolTick::MIN_TICK..=PoolTick::MAX_TICK).contains(&tick_lower),
        "ModifyLiquidity lower tick is out of bounds: {tick_lower}"
    );
    anyhow::ensure!(
        (PoolTick::MIN_TICK..=PoolTick::MAX_TICK).contains(&tick_upper),
        "ModifyLiquidity upper tick is out of bounds: {tick_upper}"
    );
    anyhow::ensure!(
        tick_lower < tick_upper,
        "ModifyLiquidity tick range must have positive width: {tick_lower}..{tick_upper}"
    );

    Ok(ModifyLiquidityEvent::new(
        dex,
        pool_id,
        metadata.block_number,
        metadata.transaction_hash,
        metadata.transaction_index,
        metadata.log_index,
        sender,
        tick_lower,
        tick_upper,
        decoded.liquidity_delta,
        decoded.salt,
    ))
}

/// Parses an `IPoolManager.ModifyLiquidity` event from a HyperSync log.
///
/// # Errors
///
/// Returns an error for a malformed log or invalid tick range.
pub fn parse_modify_liquidity_event_hypersync(
    dex: SharedDex,
    log: &HypersyncLog,
) -> anyhow::Result<ModifyLiquidityEvent> {
    validate_event_signature_hash(
        "ModifyLiquidity",
        MODIFY_LIQUIDITY_EVENT_SIGNATURE_HASH,
        log,
    )?;
    validate_topic_count(log.topics.len(), 3, "ModifyLiquidity")?;
    let pool_id = parse_pool_id(hypersync_topic(log, 1, "PoolId")?)?;
    let sender = parse_indexed_address(hypersync_topic(log, 2, "sender")?, "Sender")?;
    let data = hypersync_data(log, 4 * 32, "ModifyLiquidity")?;
    decode_event(dex, pool_id, sender, data, hypersync_metadata(log)?)
}

/// Parses an `IPoolManager.ModifyLiquidity` event from an Ethereum RPC log.
///
/// # Errors
///
/// Returns an error for a malformed log or invalid tick range.
pub fn parse_modify_liquidity_event_rpc(
    dex: SharedDex,
    log: &RpcLog,
) -> anyhow::Result<ModifyLiquidityEvent> {
    rpc_helpers::validate_event_signature(
        log,
        MODIFY_LIQUIDITY_EVENT_SIGNATURE_HASH,
        "ModifyLiquidity",
    )?;
    validate_topic_count(log.topics.len(), 3, "ModifyLiquidity")?;
    let pool_id = parse_pool_id(&rpc_helpers::extract_topic_bytes(log, 1)?)?;
    let sender = parse_indexed_address(&rpc_helpers::extract_topic_bytes(log, 2)?, "Sender")?;
    let data = rpc_data(log, 4 * 32, "ModifyLiquidity")?;
    decode_event(dex, pool_id, sender, &data, rpc_metadata(log)?)
}

#[cfg(test)]
mod tests {
    use alloy::{
        primitives::{Address, B256, I256, aliases::I24, keccak256},
        sol_types::SolValue,
    };
    use nautilus_core::hex;

    use super::*;
    use crate::{exchanges::base, exchanges::parsing::uniswap_v4::common::abi_generated_logs};

    fn logs(tick_lower: i32, tick_upper: i32, delta: I256) -> (HypersyncLog, RpcLog) {
        let data = ModifyLiquidityEventData {
            tick_lower: I24::try_from(tick_lower).unwrap(),
            tick_upper: I24::try_from(tick_upper).unwrap(),
            liquidity_delta: delta,
            salt: B256::repeat_byte(0x33),
        }
        .abi_encode();
        let mut sender_topic = [0_u8; 32];
        sender_topic[12..].copy_from_slice(Address::repeat_byte(0x22).as_slice());
        abi_generated_logs(
            vec![
                hex::encode_prefixed(keccak256(MODIFY_LIQUIDITY_EVENT_SIGNATURE)),
                B256::repeat_byte(0x11).to_string(),
                hex::encode_prefixed(sender_topic),
            ],
            data,
        )
    }

    #[test]
    fn modify_liquidity_signature_hash_matches_official_signature() {
        assert_eq!(
            hex::encode(keccak256(MODIFY_LIQUIDITY_EVENT_SIGNATURE)),
            MODIFY_LIQUIDITY_EVENT_SIGNATURE_HASH
        );
    }

    #[test]
    fn parses_signed_delta_with_hypersync_rpc_parity() {
        let delta = I256::try_from(-1_000_000_i128).unwrap();
        let (hypersync, rpc) = logs(-120, 120, delta);
        let dex = base::UNISWAP_V4.dex.clone();
        let hypersync_event =
            parse_modify_liquidity_event_hypersync(dex.clone(), &hypersync).unwrap();
        let rpc_event = parse_modify_liquidity_event_rpc(dex, &rpc).unwrap();

        assert_eq!(hypersync_event.pool_id, rpc_event.pool_id);
        assert_eq!(hypersync_event.sender, rpc_event.sender);
        assert_eq!(hypersync_event.tick_lower, -120);
        assert_eq!(hypersync_event.tick_upper, 120);
        assert_eq!(hypersync_event.liquidity_delta, delta);
        assert_eq!(hypersync_event.liquidity_delta, rpc_event.liquidity_delta);
        assert_eq!(hypersync_event.salt, rpc_event.salt);
        assert_eq!(hypersync_event.block_number, rpc_event.block_number);
        assert_eq!(hypersync_event.transaction_hash, rpc_event.transaction_hash);
    }

    #[test]
    fn permits_zero_delta_pokes_but_rejects_nonpositive_ranges() {
        let (_, rpc) = logs(-120, 120, I256::ZERO);
        assert!(parse_modify_liquidity_event_rpc(base::UNISWAP_V4.dex.clone(), &rpc).is_ok());

        let (_, rpc) = logs(120, 120, I256::try_from(1_i128).unwrap());
        assert!(
            parse_modify_liquidity_event_rpc(base::UNISWAP_V4.dex.clone(), &rpc)
                .unwrap_err()
                .to_string()
                .contains("positive width")
        );
        let (_, rpc) = logs(121, 120, I256::try_from(1_i128).unwrap());
        assert!(parse_modify_liquidity_event_rpc(base::UNISWAP_V4.dex.clone(), &rpc).is_err());
    }

    #[test]
    fn rejects_noncanonical_pool_id_topic() {
        let (_, mut rpc) = logs(-120, 120, I256::try_from(1_i128).unwrap());
        rpc.topics[1] = "0x11".to_string();
        assert!(
            parse_modify_liquidity_event_rpc(base::UNISWAP_V4.dex.clone(), &rpc)
                .unwrap_err()
                .to_string()
                .contains("PoolId topic must be 32 bytes")
        );
    }
}
