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

use alloy::primitives::Address;
use nautilus_model::defi::{AmmType, PoolIdentifier, rpc::RpcLog};

use crate::{
    events::pool_created::PoolCreatedEvent,
    exchanges::parsing::core,
    hypersync::{HypersyncLog, helpers::extract_block_number},
    rpc::helpers as rpc_helpers,
};

const UP_POOL_CREATED_SIGNATURE: &str =
    "2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e";
const GIGA_PAIR_CREATED_SIGNATURE: &str =
    "c4805696c66d7cf352fc1d6bb633ad5ee82f6cb577c453024b6e0eb8306c6fc9";

/// Parses an UP `PoolCreated` event from a HyperSync log.
///
/// # Errors
///
/// Returns an error if the log does not contain a canonical UP `PoolCreated` event.
pub fn parse_up_pool_created_event_hypersync(
    log: HypersyncLog,
) -> anyhow::Result<PoolCreatedEvent> {
    parse_hypersync(log, UP_POOL_CREATED_SIGNATURE, "UP PoolCreated")
}

/// Parses an UP `PoolCreated` event from an RPC log.
///
/// # Errors
///
/// Returns an error if the log does not contain a canonical UP `PoolCreated` event.
pub fn parse_up_pool_created_event_rpc(log: &RpcLog) -> anyhow::Result<PoolCreatedEvent> {
    parse_rpc(log, UP_POOL_CREATED_SIGNATURE, "UP PoolCreated")
}

/// Parses a GIGA `PairCreated` event from a HyperSync log.
///
/// # Errors
///
/// Returns an error if the log does not contain a canonical GIGA `PairCreated` event.
pub fn parse_giga_pool_created_event_hypersync(
    log: HypersyncLog,
) -> anyhow::Result<PoolCreatedEvent> {
    parse_hypersync(log, GIGA_PAIR_CREATED_SIGNATURE, "GIGA PairCreated")
}

/// Parses a GIGA `PairCreated` event from an RPC log.
///
/// # Errors
///
/// Returns an error if the log does not contain a canonical GIGA `PairCreated` event.
pub fn parse_giga_pool_created_event_rpc(log: &RpcLog) -> anyhow::Result<PoolCreatedEvent> {
    parse_rpc(log, GIGA_PAIR_CREATED_SIGNATURE, "GIGA PairCreated")
}

fn parse_hypersync(
    log: HypersyncLog,
    signature: &str,
    event_name: &str,
) -> anyhow::Result<PoolCreatedEvent> {
    let block_number = extract_block_number(&log)?;
    let topics = log
        .topics
        .iter()
        .map(|topic| topic.as_ref().map(AsRef::as_ref))
        .collect::<Vec<_>>();
    decode_pool_created(
        block_number,
        &topics,
        log.data.as_ref().map(AsRef::as_ref),
        signature,
        event_name,
    )
}

fn parse_rpc(log: &RpcLog, signature: &str, event_name: &str) -> anyhow::Result<PoolCreatedEvent> {
    let block_number = rpc_helpers::extract_block_number(log)?;
    let topic_bytes = log
        .topics
        .iter()
        .map(|topic| rpc_helpers::decode_hex(topic))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let topics = topic_bytes
        .iter()
        .map(|topic| Some(topic.as_slice()))
        .collect::<Vec<_>>();
    let data = rpc_helpers::extract_data_bytes(log)?;
    decode_pool_created(block_number, &topics, Some(&data), signature, event_name)
}

fn decode_pool_created(
    block_number: u64,
    topics: &[Option<&[u8]>],
    data: Option<&[u8]>,
    signature: &str,
    event_name: &str,
) -> anyhow::Result<PoolCreatedEvent> {
    anyhow::ensure!(
        topics.len() == 4,
        "{event_name} event must have exactly 4 topics, was {}",
        topics.len()
    );

    let topic = |index: usize, name: &str| {
        topics[index].ok_or_else(|| anyhow::anyhow!("Missing {name} in topic{index}"))
    };
    core::validate_signature_bytes(topic(0, "event signature")?, signature, event_name)?;
    let token0 = parse_address_word(topic(1, "token0")?, "token0 topic")?;
    let token1 = parse_address_word(topic(2, "token1")?, "token1 topic")?;
    let stable = parse_indexed_bool(topic(3, "stable")?)?;

    anyhow::ensure!(token0 != token1, "{event_name} tokens must be distinct");
    anyhow::ensure!(
        token0 < token1,
        "{event_name} tokens must be strictly ordered"
    );

    let data = data.ok_or_else(|| anyhow::anyhow!("Missing data in {event_name} event log"))?;
    anyhow::ensure!(
        data.len() == 64,
        "{event_name} event data must be exactly 64 bytes, was {}",
        data.len()
    );
    let pool_address = parse_address_word(&data[..32], "pool address")?;
    anyhow::ensure!(
        pool_address != Address::ZERO,
        "{event_name} pool address must be nonzero"
    );

    let mut event = PoolCreatedEvent::new(
        block_number,
        token0,
        token1,
        pool_address,
        PoolIdentifier::from_address(pool_address),
        None,
        None,
    );
    event.set_amm_type(if stable {
        AmmType::StableSwap
    } else {
        AmmType::CPAMM
    });
    Ok(event)
}

fn parse_address_word(word: &[u8], name: &str) -> anyhow::Result<Address> {
    anyhow::ensure!(
        word.len() == 32,
        "{name} must be exactly 32 bytes, was {}",
        word.len()
    );
    anyhow::ensure!(
        word[..12].iter().all(|byte| *byte == 0),
        "{name} has non-zero address padding"
    );
    Ok(Address::from_slice(&word[12..]))
}

fn parse_indexed_bool(word: &[u8]) -> anyhow::Result<bool> {
    anyhow::ensure!(
        word.len() == 32,
        "stable topic must be exactly 32 bytes, was {}",
        word.len()
    );
    anyhow::ensure!(
        word[..31].iter().all(|byte| *byte == 0) && word[31] <= 1,
        "stable topic must use canonical bool encoding"
    );
    Ok(word[31] == 1)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const TOKEN0: &str = "0000000000000000000000001111111111111111111111111111111111111111";
    const TOKEN1: &str = "0000000000000000000000002222222222222222222222222222222222222222";
    const POOL_DATA: &str = concat!(
        "0000000000000000000000003333333333333333333333333333333333333333",
        "0000000000000000000000000000000000000000000000000000000000000001"
    );
    const ZERO_POOL_DATA: &str = concat!(
        "0000000000000000000000000000000000000000000000000000000000000000",
        "0000000000000000000000000000000000000000000000000000000000000001"
    );
    const BOOL_FALSE: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    const BOOL_TRUE: &str = "0000000000000000000000000000000000000000000000000000000000000001";
    const BOOL_TWO: &str = "0000000000000000000000000000000000000000000000000000000000000002";

    fn representative_logs(
        signature: &str,
        token0: &str,
        token1: &str,
        stable: &str,
        data: &str,
    ) -> (HypersyncLog, RpcLog) {
        let topics = [signature, token0, token1, stable]
            .map(|topic| format!("0x{topic}"))
            .to_vec();
        let data = format!("0x{data}");
        let hypersync = serde_json::from_value(json!({
            "removed": null,
            "log_index": "0x7",
            "transaction_index": "0x3",
            "transaction_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "block_hash": null,
            "block_number": "0x1234",
            "address": "0x4444444444444444444444444444444444444444",
            "data": data,
            "topics": topics,
        }))
        .expect("valid representative HyperSync log");
        let rpc = RpcLog {
            removed: false,
            log_index: Some("0x7".to_string()),
            transaction_index: Some("0x3".to_string()),
            transaction_hash: Some(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            ),
            block_hash: None,
            block_number: Some("0x1234".to_string()),
            address: "0x4444444444444444444444444444444444444444".to_string(),
            data,
            topics,
        };
        (hypersync, rpc)
    }

    fn assert_parity(hypersync: PoolCreatedEvent, rpc: PoolCreatedEvent) {
        assert_eq!(hypersync.block_number, rpc.block_number);
        assert_eq!(hypersync.token0, rpc.token0);
        assert_eq!(hypersync.token1, rpc.token1);
        assert_eq!(hypersync.pool_address, rpc.pool_address);
        assert_eq!(hypersync.pool_identifier, rpc.pool_identifier);
        assert_eq!(hypersync.fee, None);
        assert_eq!(hypersync.tick_spacing, None);
        assert_eq!(hypersync.amm_type, rpc.amm_type);
    }

    #[test]
    fn parses_up_pool_created_with_rpc_hypersync_parity() {
        let (hypersync, rpc) = representative_logs(
            UP_POOL_CREATED_SIGNATURE,
            TOKEN0,
            TOKEN1,
            BOOL_TRUE,
            POOL_DATA,
        );
        let hypersync = parse_up_pool_created_event_hypersync(hypersync).unwrap();
        let rpc = parse_up_pool_created_event_rpc(&rpc).unwrap();

        assert_parity(hypersync.clone(), rpc);
        assert_eq!(hypersync.block_number, 0x1234);
        assert_eq!(hypersync.amm_type, Some(AmmType::StableSwap));
    }

    #[test]
    fn parses_giga_pair_created_with_rpc_hypersync_parity() {
        let (hypersync, rpc) = representative_logs(
            GIGA_PAIR_CREATED_SIGNATURE,
            TOKEN0,
            TOKEN1,
            BOOL_FALSE,
            POOL_DATA,
        );
        let hypersync = parse_giga_pool_created_event_hypersync(hypersync).unwrap();
        let rpc = parse_giga_pool_created_event_rpc(&rpc).unwrap();

        assert_parity(hypersync.clone(), rpc);
        assert_eq!(hypersync.amm_type, Some(AmmType::CPAMM));
    }

    #[test]
    fn wrappers_reject_the_other_event_signature() {
        let (up_hypersync, up_rpc) = representative_logs(
            UP_POOL_CREATED_SIGNATURE,
            TOKEN0,
            TOKEN1,
            BOOL_FALSE,
            POOL_DATA,
        );
        let (giga_hypersync, giga_rpc) = representative_logs(
            GIGA_PAIR_CREATED_SIGNATURE,
            TOKEN0,
            TOKEN1,
            BOOL_FALSE,
            POOL_DATA,
        );

        assert!(parse_giga_pool_created_event_hypersync(up_hypersync).is_err());
        assert!(parse_giga_pool_created_event_rpc(&up_rpc).is_err());
        assert!(parse_up_pool_created_event_hypersync(giga_hypersync).is_err());
        assert!(parse_up_pool_created_event_rpc(&giga_rpc).is_err());
    }

    #[test]
    fn rejects_wrong_topic_count_for_both_transports() {
        let (mut hypersync, mut rpc) = representative_logs(
            UP_POOL_CREATED_SIGNATURE,
            TOKEN0,
            TOKEN1,
            BOOL_FALSE,
            POOL_DATA,
        );
        hypersync.topics.pop();
        rpc.topics.pop();

        assert!(parse_up_pool_created_event_hypersync(hypersync).is_err());
        assert!(parse_up_pool_created_event_rpc(&rpc).is_err());
    }

    #[test]
    fn rejects_malformed_layouts_for_both_transports() {
        let malformed = [
            (TOKEN0, TOKEN1, BOOL_TWO, POOL_DATA),
            (TOKEN0, TOKEN1, BOOL_FALSE, ZERO_POOL_DATA),
            (TOKEN0, TOKEN0, BOOL_FALSE, POOL_DATA),
            (TOKEN1, TOKEN0, BOOL_FALSE, POOL_DATA),
            (TOKEN0, TOKEN1, BOOL_FALSE, &POOL_DATA[..64]),
        ];

        for (token0, token1, stable, data) in malformed {
            let (hypersync, rpc) =
                representative_logs(UP_POOL_CREATED_SIGNATURE, token0, token1, stable, data);
            assert!(parse_up_pool_created_event_hypersync(hypersync).is_err());
            assert!(parse_up_pool_created_event_rpc(&rpc).is_err());
        }
    }
}
