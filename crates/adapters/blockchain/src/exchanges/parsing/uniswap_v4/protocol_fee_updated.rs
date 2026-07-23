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
use nautilus_model::defi::{SharedDex, rpc::RpcLog};

use super::common::{
    hypersync_data, hypersync_metadata, hypersync_topic, parse_pool_id, rpc_data, rpc_metadata,
    validate_topic_count,
};
use crate::{
    events::protocol_fee_updated::ProtocolFeeUpdatedEvent,
    hypersync::{HypersyncLog, helpers::validate_event_signature_hash},
    rpc::helpers as rpc_helpers,
};

/// Canonical `IProtocolFees.ProtocolFeeUpdated` event signature.
pub const PROTOCOL_FEE_UPDATED_EVENT_SIGNATURE: &str = "ProtocolFeeUpdated(bytes32,uint24)";
const PROTOCOL_FEE_UPDATED_EVENT_SIGNATURE_HASH: &str =
    "e9c42593e71f84403b84352cd168d693e2c9fcd1fdbcc3feb21d92b43e6696f9";
const MAX_PROTOCOL_FEE: u32 = 1_000;

sol! {
    struct ProtocolFeeUpdatedEventData {
        uint24 protocol_fee;
    }
}

fn decode_event(
    dex: SharedDex,
    pool_id: alloy::primitives::B256,
    data: &[u8],
    metadata: super::common::EventMetadata,
) -> anyhow::Result<ProtocolFeeUpdatedEvent> {
    let decoded = <ProtocolFeeUpdatedEventData as SolType>::abi_decode(data).map_err(|error| {
        anyhow::anyhow!("Failed to decode ProtocolFeeUpdated event data: {error}")
    })?;
    anyhow::ensure!(
        decoded.abi_encode() == data,
        "ProtocolFeeUpdated event data is not canonical ABI"
    );
    let protocol_fee = decoded.protocol_fee.to::<u32>();
    let zero_for_one_fee = protocol_fee & 0x0fff;
    let one_for_zero_fee = protocol_fee >> 12;
    anyhow::ensure!(
        zero_for_one_fee <= MAX_PROTOCOL_FEE && one_for_zero_fee <= MAX_PROTOCOL_FEE,
        "Protocol fee contains an out-of-range directional fee: {protocol_fee}"
    );

    Ok(ProtocolFeeUpdatedEvent::new(
        dex,
        pool_id,
        metadata.block_number,
        metadata.transaction_hash,
        metadata.transaction_index,
        metadata.log_index,
        protocol_fee,
    ))
}

/// Parses an `IProtocolFees.ProtocolFeeUpdated` event from a HyperSync log.
///
/// # Errors
///
/// Returns an error for a malformed log or invalid packed protocol fee.
pub fn parse_protocol_fee_updated_event_hypersync(
    dex: SharedDex,
    log: &HypersyncLog,
) -> anyhow::Result<ProtocolFeeUpdatedEvent> {
    validate_event_signature_hash(
        "ProtocolFeeUpdated",
        PROTOCOL_FEE_UPDATED_EVENT_SIGNATURE_HASH,
        log,
    )?;
    validate_topic_count(log.topics.len(), 2, "ProtocolFeeUpdated")?;
    let pool_id = parse_pool_id(hypersync_topic(log, 1, "PoolId")?)?;
    let data = hypersync_data(log, 32, "ProtocolFeeUpdated")?;
    decode_event(dex, pool_id, data, hypersync_metadata(log)?)
}

/// Parses an `IProtocolFees.ProtocolFeeUpdated` event from an Ethereum RPC log.
///
/// # Errors
///
/// Returns an error for a malformed log or invalid packed protocol fee.
pub fn parse_protocol_fee_updated_event_rpc(
    dex: SharedDex,
    log: &RpcLog,
) -> anyhow::Result<ProtocolFeeUpdatedEvent> {
    rpc_helpers::validate_event_signature(
        log,
        PROTOCOL_FEE_UPDATED_EVENT_SIGNATURE_HASH,
        "ProtocolFeeUpdated",
    )?;
    validate_topic_count(log.topics.len(), 2, "ProtocolFeeUpdated")?;
    let pool_id = parse_pool_id(&rpc_helpers::extract_topic_bytes(log, 1)?)?;
    let data = rpc_data(log, 32, "ProtocolFeeUpdated")?;
    decode_event(dex, pool_id, &data, rpc_metadata(log)?)
}

#[cfg(test)]
mod tests {
    use alloy::{
        primitives::{B256, aliases::U24, keccak256},
        sol_types::SolValue,
    };
    use nautilus_core::hex;

    use super::*;
    use crate::{exchanges::base, exchanges::parsing::uniswap_v4::common::abi_generated_logs};

    fn logs(protocol_fee: u32) -> (HypersyncLog, RpcLog) {
        let data = ProtocolFeeUpdatedEventData {
            protocol_fee: U24::from(protocol_fee),
        }
        .abi_encode();
        abi_generated_logs(
            vec![
                hex::encode_prefixed(keccak256(PROTOCOL_FEE_UPDATED_EVENT_SIGNATURE)),
                B256::repeat_byte(0x11).to_string(),
            ],
            data,
        )
    }

    #[test]
    fn protocol_fee_signature_hash_matches_official_signature() {
        assert_eq!(
            hex::encode(keccak256(PROTOCOL_FEE_UPDATED_EVENT_SIGNATURE)),
            PROTOCOL_FEE_UPDATED_EVENT_SIGNATURE_HASH
        );
    }

    #[test]
    fn parses_hypersync_rpc_with_parity() {
        let packed_fee = 500 | (750 << 12);
        let (hypersync, rpc) = logs(packed_fee);
        let dex = base::UNISWAP_V4.dex.clone();
        let hypersync_event =
            parse_protocol_fee_updated_event_hypersync(dex.clone(), &hypersync).unwrap();
        let rpc_event = parse_protocol_fee_updated_event_rpc(dex, &rpc).unwrap();

        assert_eq!(hypersync_event.pool_id, rpc_event.pool_id);
        assert_eq!(hypersync_event.protocol_fee, packed_fee);
        assert_eq!(hypersync_event.protocol_fee, rpc_event.protocol_fee);
        assert_eq!(hypersync_event.block_number, rpc_event.block_number);
        assert_eq!(hypersync_event.transaction_hash, rpc_event.transaction_hash);
        assert_eq!(
            hypersync_event.transaction_index,
            rpc_event.transaction_index
        );
        assert_eq!(hypersync_event.log_index, rpc_event.log_index);
    }

    #[test]
    fn rejects_invalid_directional_fee_and_noncanonical_abi() {
        let (_, rpc) = logs(1_001);
        assert!(
            parse_protocol_fee_updated_event_rpc(base::UNISWAP_V4.dex.clone(), &rpc)
                .unwrap_err()
                .to_string()
                .contains("out-of-range")
        );

        let (_, mut rpc) = logs(500);
        rpc.data.replace_range(2..4, "01");
        assert!(parse_protocol_fee_updated_event_rpc(base::UNISWAP_V4.dex.clone(), &rpc).is_err());
    }
}
