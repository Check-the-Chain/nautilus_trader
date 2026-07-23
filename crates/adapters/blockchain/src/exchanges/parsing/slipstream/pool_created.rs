// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use alloy::primitives::Address;
use nautilus_model::defi::{AmmType, PoolIdentifier, rpc::RpcLog};
use ustr::Ustr;

use crate::{
    events::pool_created::PoolCreatedEvent,
    hypersync::{
        HypersyncLog,
        helpers::{extract_block_number, validate_event_signature_hash},
    },
    rpc::helpers as rpc_helpers,
};

const POOL_CREATED_EVENT_SIGNATURE_HASH: &str =
    "ab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2";

fn decode_address_word(word: &[u8], name: &str) -> anyhow::Result<Address> {
    anyhow::ensure!(
        word.len() == 32,
        "{name} ABI word must be 32 bytes, was {}",
        word.len()
    );
    anyhow::ensure!(
        word[..12].iter().all(|byte| *byte == 0),
        "{name} ABI word has non-zero address padding"
    );
    Ok(Address::from_slice(&word[12..]))
}

fn decode_tick_spacing(word: &[u8]) -> anyhow::Result<u32> {
    anyhow::ensure!(
        word.len() == 32,
        "tickSpacing ABI word must be 32 bytes, was {}",
        word.len()
    );

    let negative = word[29] & 0x80 != 0;
    let padding = if negative { 0xff } else { 0x00 };
    anyhow::ensure!(
        word[..29].iter().all(|byte| *byte == padding),
        "tickSpacing is not canonically sign-extended int24"
    );

    let unsigned = (i32::from(word[29]) << 16) | (i32::from(word[30]) << 8) | i32::from(word[31]);
    let tick_spacing = if negative {
        unsigned | !0x00ff_ffff
    } else {
        unsigned
    };
    anyhow::ensure!(
        (-8_388_608..=8_388_607).contains(&tick_spacing),
        "tickSpacing is outside the int24 range"
    );
    anyhow::ensure!(tick_spacing > 0, "tickSpacing must be strictly positive");

    u32::try_from(tick_spacing).map_err(Into::into)
}

fn decode_pool_created(
    block_number: u64,
    token0_word: &[u8],
    token1_word: &[u8],
    tick_spacing_word: &[u8],
    data: &[u8],
) -> anyhow::Result<PoolCreatedEvent> {
    anyhow::ensure!(
        data.len() == 32,
        "Slipstream PoolCreated data must be exactly 32 bytes, was {}",
        data.len()
    );

    let token0 = decode_address_word(token0_word, "token0")?;
    let token1 = decode_address_word(token1_word, "token1")?;
    let tick_spacing = decode_tick_spacing(tick_spacing_word)?;
    let pool = decode_address_word(data, "pool")?;

    anyhow::ensure!(token0 != Address::ZERO, "token0 must be nonzero");
    anyhow::ensure!(token1 != Address::ZERO, "token1 must be nonzero");
    anyhow::ensure!(token0 != token1, "token0 and token1 must be distinct");
    anyhow::ensure!(pool != Address::ZERO, "pool must be nonzero");
    anyhow::ensure!(
        pool != token0 && pool != token1,
        "pool must be distinct from token0 and token1"
    );

    let mut event = PoolCreatedEvent::new(
        block_number,
        token0,
        token1,
        pool,
        PoolIdentifier::Address(Ustr::from(&pool.to_string())),
        None,
        Some(tick_spacing),
    );
    event.set_amm_type(AmmType::CLAMM);
    Ok(event)
}

/// Parses a Slipstream factory `PoolCreated` event from a HyperSync log.
///
/// # Errors
///
/// Returns an error if the signature, indexed topics, data word, or identities are invalid.
pub fn parse_pool_created_event_hypersync(log: HypersyncLog) -> anyhow::Result<PoolCreatedEvent> {
    validate_event_signature_hash(
        "Slipstream PoolCreated",
        POOL_CREATED_EVENT_SIGNATURE_HASH,
        &log,
    )?;
    anyhow::ensure!(
        log.topics.len() == 4,
        "Slipstream PoolCreated must have exactly 4 topics, was {}",
        log.topics.len()
    );

    let block_number = extract_block_number(&log)?;
    let token0 = log.topics[1]
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing token0 in topic1"))?;
    let token1 = log.topics[2]
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing token1 in topic2"))?;
    let tick_spacing = log.topics[3]
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing tickSpacing in topic3"))?;
    let data = log
        .data
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing pool data word"))?;

    decode_pool_created(
        block_number,
        token0.as_ref(),
        token1.as_ref(),
        tick_spacing.as_ref(),
        data.as_ref(),
    )
}

/// Parses a Slipstream factory `PoolCreated` event from an RPC log.
///
/// # Errors
///
/// Returns an error if the signature, indexed topics, data word, or identities are invalid.
pub fn parse_pool_created_event_rpc(log: &RpcLog) -> anyhow::Result<PoolCreatedEvent> {
    rpc_helpers::validate_event_signature(
        log,
        POOL_CREATED_EVENT_SIGNATURE_HASH,
        "Slipstream PoolCreated",
    )?;
    anyhow::ensure!(
        log.topics.len() == 4,
        "Slipstream PoolCreated must have exactly 4 topics, was {}",
        log.topics.len()
    );

    let block_number = rpc_helpers::extract_block_number(log)?;
    let token0 = rpc_helpers::extract_topic_bytes(log, 1)?;
    let token1 = rpc_helpers::extract_topic_bytes(log, 2)?;
    let tick_spacing = rpc_helpers::extract_topic_bytes(log, 3)?;
    let data = rpc_helpers::extract_data_bytes(log)?;

    decode_pool_created(block_number, &token0, &token1, &tick_spacing, &data)
}

#[cfg(test)]
mod tests {
    use nautilus_core::hex;
    use serde_json::json;

    use super::*;

    const TOKEN0_TOPIC: &str = "0x0000000000000000000000004200000000000000000000000000000000000006";
    const TOKEN1_TOPIC: &str = "0x000000000000000000000000833589fcd6edb6e08f4c7c32d4f71b54bda02913";
    const TICK_SPACING_TOPIC: &str =
        "0x0000000000000000000000000000000000000000000000000000000000000064";
    const POOL_DATA: &str = "0x000000000000000000000000cdac0d6c6c59727a65f871236188350531885c43";

    fn rpc_log() -> RpcLog {
        RpcLog {
            removed: false,
            log_index: Some("0x2a".to_string()),
            transaction_index: Some("0x4".to_string()),
            transaction_hash: Some(
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            ),
            block_hash: Some(
                "0x2222222222222222222222222222222222222222222222222222222222222222".to_string(),
            ),
            block_number: Some("0xd33cf8".to_string()),
            address: "0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a".to_string(),
            data: POOL_DATA.to_string(),
            topics: vec![
                format!("0x{POOL_CREATED_EVENT_SIGNATURE_HASH}"),
                TOKEN0_TOPIC.to_string(),
                TOKEN1_TOPIC.to_string(),
                TICK_SPACING_TOPIC.to_string(),
            ],
        }
    }

    fn hypersync_log() -> HypersyncLog {
        serde_json::from_value(json!({
            "removed": null,
            "log_index": "0x2a",
            "transaction_index": "0x4",
            "transaction_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
            "block_hash": null,
            "block_number": "0xd33cf8",
            "address": "0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a",
            "data": POOL_DATA,
            "topics": [
                format!("0x{POOL_CREATED_EVENT_SIGNATURE_HASH}"),
                TOKEN0_TOPIC,
                TOKEN1_TOPIC,
                TICK_SPACING_TOPIC,
            ],
        }))
        .expect("valid HyperSync fixture")
    }

    fn word(hex_value: &str) -> Vec<u8> {
        hex::decode(hex_value).expect("valid ABI word")
    }

    #[test]
    fn parses_rpc_and_hypersync_with_identical_results() {
        let rpc = parse_pool_created_event_rpc(&rpc_log()).expect("RPC parse");
        let hypersync =
            parse_pool_created_event_hypersync(hypersync_log()).expect("HyperSync parse");

        assert_eq!(rpc.block_number, 13_843_704);
        assert_eq!(rpc.block_number, hypersync.block_number);
        assert_eq!(rpc.token0, hypersync.token0);
        assert_eq!(rpc.token1, hypersync.token1);
        assert_eq!(rpc.pool_address, hypersync.pool_address);
        assert_eq!(rpc.pool_identifier, hypersync.pool_identifier);
        assert_eq!(rpc.fee, None);
        assert_eq!(rpc.fee, hypersync.fee);
        assert_eq!(rpc.tick_spacing, Some(100));
        assert_eq!(rpc.tick_spacing, hypersync.tick_spacing);
        assert_eq!(rpc.amm_type, Some(AmmType::CLAMM));
        assert_eq!(rpc.amm_type, hypersync.amm_type);
    }

    #[test]
    fn rejects_rpc_signature_mismatch() {
        let mut log = rpc_log();
        log.topics[0] =
            "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118".to_string();

        assert!(parse_pool_created_event_rpc(&log).is_err());
    }

    #[test]
    fn rejects_hypersync_tick_spacing_in_data_layout() {
        let log: HypersyncLog = serde_json::from_value(json!({
            "removed": null,
            "log_index": "0x2a",
            "transaction_index": "0x4",
            "transaction_hash": null,
            "block_hash": null,
            "block_number": "0xd33cf8",
            "address": "0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a",
            "data": format!("{TICK_SPACING_TOPIC}{}", &POOL_DATA[2..]),
            "topics": [
                format!("0x{POOL_CREATED_EVENT_SIGNATURE_HASH}"),
                TOKEN0_TOPIC,
                TOKEN1_TOPIC,
            ],
        }))
        .expect("valid malformed HyperSync fixture");

        assert!(parse_pool_created_event_hypersync(log).is_err());
    }

    #[test]
    fn rejects_noncanonical_and_nonpositive_tick_spacing() {
        let token0 = word(&TOKEN0_TOPIC[2..]);
        let token1 = word(&TOKEN1_TOPIC[2..]);
        let pool = word(&POOL_DATA[2..]);

        let mut noncanonical = [0_u8; 32];
        noncanonical[0] = 1;
        noncanonical[31] = 1;
        assert!(decode_pool_created(1, &token0, &token1, &noncanonical, &pool).is_err());

        let zero = [0_u8; 32];
        assert!(decode_pool_created(1, &token0, &token1, &zero, &pool).is_err());

        let negative = [0xff_u8; 32];
        assert!(decode_pool_created(1, &token0, &token1, &negative, &pool).is_err());

        let mut positive_outside_int24 = [0_u8; 32];
        positive_outside_int24[29] = 0x80;
        assert!(decode_pool_created(1, &token0, &token1, &positive_outside_int24, &pool).is_err());
    }

    #[test]
    fn rejects_invalid_data_lengths_and_pool_address() {
        let mut short = rpc_log();
        short.data = format!("0x{}", "00".repeat(31));
        assert!(parse_pool_created_event_rpc(&short).is_err());

        let mut long = rpc_log();
        long.data = format!("{POOL_DATA}{}", "00".repeat(32));
        assert!(parse_pool_created_event_rpc(&long).is_err());

        let mut zero_pool = rpc_log();
        zero_pool.data = format!("0x{}", "00".repeat(32));
        assert!(parse_pool_created_event_rpc(&zero_pool).is_err());
    }

    #[test]
    fn rejects_invalid_token_and_pool_identities() {
        let mut duplicate_tokens = rpc_log();
        duplicate_tokens.topics[2] = TOKEN0_TOPIC.to_string();
        assert!(parse_pool_created_event_rpc(&duplicate_tokens).is_err());

        let mut zero_token = rpc_log();
        zero_token.topics[1] = format!("0x{}", "00".repeat(32));
        assert!(parse_pool_created_event_rpc(&zero_token).is_err());

        let mut pool_is_token = rpc_log();
        pool_is_token.data = TOKEN0_TOPIC.to_string();
        assert!(parse_pool_created_event_rpc(&pool_is_token).is_err());
    }

    #[test]
    fn rejects_noncanonical_address_padding() {
        let mut log = rpc_log();
        log.topics[1].replace_range(2..4, "01");
        assert!(parse_pool_created_event_rpc(&log).is_err());

        let mut log = rpc_log();
        log.data.replace_range(2..4, "01");
        assert!(parse_pool_created_event_rpc(&log).is_err());
    }
}
