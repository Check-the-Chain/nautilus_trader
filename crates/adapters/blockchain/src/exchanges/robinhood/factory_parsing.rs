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
use nautilus_model::defi::{PoolIdentifier, rpc::RpcLog};

use crate::{events::pool_created::PoolCreatedEvent, rpc::helpers as rpc_helpers};

const V2_PAIR_CREATED_SIGNATURE: &str =
    "0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9";
const V3_POOL_CREATED_SIGNATURE: &str =
    "783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118";

pub(super) fn parse_v2_pool_created_event_rpc(log: &RpcLog) -> anyhow::Result<PoolCreatedEvent> {
    rpc_helpers::validate_event_signature(log, V2_PAIR_CREATED_SIGNATURE, "PairCreated")?;
    anyhow::ensure!(
        log.topics.len() == 3,
        "PairCreated must have exactly 3 topics, was {}",
        log.topics.len()
    );

    let block_number = rpc_helpers::extract_block_number(log)?;
    let token0 = decode_address_word(&rpc_helpers::extract_topic_bytes(log, 1)?, "token0")?;
    let token1 = decode_address_word(&rpc_helpers::extract_topic_bytes(log, 2)?, "token1")?;
    let data = rpc_helpers::extract_data_bytes(log)?;
    anyhow::ensure!(
        data.len() == 64,
        "PairCreated data must be exactly 64 bytes, was {}",
        data.len()
    );
    let pool = decode_address_word(&data[..32], "pair")?;

    validate_identities(token0, token1, pool, "PairCreated")?;
    Ok(PoolCreatedEvent::new(
        block_number,
        token0,
        token1,
        pool,
        PoolIdentifier::from_address(pool),
        None,
        None,
    ))
}

pub(super) fn parse_v3_pool_created_event_rpc(log: &RpcLog) -> anyhow::Result<PoolCreatedEvent> {
    rpc_helpers::validate_event_signature(log, V3_POOL_CREATED_SIGNATURE, "PoolCreated")?;
    anyhow::ensure!(
        log.topics.len() == 4,
        "PoolCreated must have exactly 4 topics, was {}",
        log.topics.len()
    );

    let block_number = rpc_helpers::extract_block_number(log)?;
    let token0 = decode_address_word(&rpc_helpers::extract_topic_bytes(log, 1)?, "token0")?;
    let token1 = decode_address_word(&rpc_helpers::extract_topic_bytes(log, 2)?, "token1")?;
    let fee = decode_u24(&rpc_helpers::extract_topic_bytes(log, 3)?, "fee")?;
    let data = rpc_helpers::extract_data_bytes(log)?;
    anyhow::ensure!(
        data.len() == 64,
        "PoolCreated data must be exactly 64 bytes, was {}",
        data.len()
    );
    let tick_spacing = decode_positive_i24(&data[..32], "tickSpacing")?;
    let pool = decode_address_word(&data[32..], "pool")?;

    validate_identities(token0, token1, pool, "PoolCreated")?;
    Ok(PoolCreatedEvent::new(
        block_number,
        token0,
        token1,
        pool,
        PoolIdentifier::from_address(pool),
        Some(fee),
        Some(tick_spacing),
    ))
}

fn decode_address_word(word: &[u8], name: &str) -> anyhow::Result<Address> {
    anyhow::ensure!(
        word.len() == 32,
        "{name} ABI word must be exactly 32 bytes, was {}",
        word.len()
    );
    anyhow::ensure!(
        word[..12].iter().all(|byte| *byte == 0),
        "{name} ABI word has non-zero address padding"
    );
    Ok(Address::from_slice(&word[12..]))
}

fn decode_u24(word: &[u8], name: &str) -> anyhow::Result<u32> {
    anyhow::ensure!(
        word.len() == 32,
        "{name} ABI word must be exactly 32 bytes, was {}",
        word.len()
    );
    anyhow::ensure!(
        word[..29].iter().all(|byte| *byte == 0),
        "{name} is not canonically encoded as uint24"
    );
    Ok((u32::from(word[29]) << 16) | (u32::from(word[30]) << 8) | u32::from(word[31]))
}

fn decode_positive_i24(word: &[u8], name: &str) -> anyhow::Result<u32> {
    anyhow::ensure!(
        word.len() == 32,
        "{name} ABI word must be exactly 32 bytes, was {}",
        word.len()
    );
    let negative = word[29] & 0x80 != 0;
    let padding = if negative { 0xff } else { 0x00 };
    anyhow::ensure!(
        word[..29].iter().all(|byte| *byte == padding),
        "{name} is not canonically sign-extended int24"
    );
    anyhow::ensure!(!negative, "{name} must be strictly positive");

    let value = (u32::from(word[29]) << 16) | (u32::from(word[30]) << 8) | u32::from(word[31]);
    anyhow::ensure!(value > 0, "{name} must be strictly positive");
    Ok(value)
}

fn validate_identities(
    token0: Address,
    token1: Address,
    pool: Address,
    event_name: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        token0 != Address::ZERO,
        "{event_name} token0 must be nonzero"
    );
    anyhow::ensure!(
        token1 != Address::ZERO,
        "{event_name} token1 must be nonzero"
    );
    anyhow::ensure!(token0 != token1, "{event_name} tokens must be distinct");
    anyhow::ensure!(pool != Address::ZERO, "{event_name} pool must be nonzero");
    anyhow::ensure!(
        pool != token0 && pool != token1,
        "{event_name} pool must be distinct from its tokens"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN0: &str = "0x0000000000000000000000001111111111111111111111111111111111111111";
    const TOKEN1: &str = "0x0000000000000000000000002222222222222222222222222222222222222222";
    const POOL: &str = "0000000000000000000000003333333333333333333333333333333333333333";

    fn rpc_log(signature: &str, topics: &[&str], data: String) -> RpcLog {
        RpcLog {
            removed: false,
            log_index: Some("0x0".to_string()),
            transaction_index: Some("0x0".to_string()),
            transaction_hash: None,
            block_hash: None,
            block_number: Some("0x1234".to_string()),
            address: "0x4444444444444444444444444444444444444444".to_string(),
            data,
            topics: std::iter::once(format!("0x{signature}"))
                .chain(topics.iter().map(ToString::to_string))
                .collect(),
        }
    }

    fn v2_log() -> RpcLog {
        rpc_log(
            V2_PAIR_CREATED_SIGNATURE,
            &[TOKEN0, TOKEN1],
            format!("0x{POOL}{}", "00".repeat(31) + "01"),
        )
    }

    fn v3_log() -> RpcLog {
        rpc_log(
            V3_POOL_CREATED_SIGNATURE,
            &[
                TOKEN0,
                TOKEN1,
                "0x0000000000000000000000000000000000000000000000000000000000000bb8",
            ],
            format!("0x{}{POOL}", "00".repeat(31) + "3c"),
        )
    }

    #[test]
    fn parses_verified_v2_and_v3_layouts() {
        let v2 = parse_v2_pool_created_event_rpc(&v2_log()).unwrap();
        let v3 = parse_v3_pool_created_event_rpc(&v3_log()).unwrap();

        assert_eq!(v2.block_number, 0x1234);
        assert_eq!(v2.fee, None);
        assert_eq!(v2.tick_spacing, None);
        assert_eq!(v3.fee, Some(3000));
        assert_eq!(v3.tick_spacing, Some(60));
    }

    #[test]
    fn rejects_non_exact_topic_and_data_lengths() {
        let mut v2 = v2_log();
        v2.data.push_str(&"00".repeat(32));
        assert!(parse_v2_pool_created_event_rpc(&v2).is_err());

        let mut v2 = v2_log();
        v2.topics.push(TOKEN0.to_string());
        assert!(parse_v2_pool_created_event_rpc(&v2).is_err());

        let mut v3 = v3_log();
        v3.data.truncate(v3.data.len() - 64);
        assert!(parse_v3_pool_created_event_rpc(&v3).is_err());

        let mut v3 = v3_log();
        v3.topics.pop();
        assert!(parse_v3_pool_created_event_rpc(&v3).is_err());
    }

    #[test]
    fn rejects_noncanonical_abi_words() {
        let mut v2 = v2_log();
        v2.topics[1].replace_range(2..4, "01");
        assert!(parse_v2_pool_created_event_rpc(&v2).is_err());

        let mut v3 = v3_log();
        v3.topics[3].replace_range(2..4, "01");
        assert!(parse_v3_pool_created_event_rpc(&v3).is_err());

        let mut v3 = v3_log();
        v3.data.replace_range(2..4, "01");
        assert!(parse_v3_pool_created_event_rpc(&v3).is_err());

        let mut v3 = v3_log();
        v3.data.replace_range(66..68, "01");
        assert!(parse_v3_pool_created_event_rpc(&v3).is_err());
    }
}
