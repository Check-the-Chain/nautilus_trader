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

use alloy::primitives::{Address, B256};
use nautilus_model::defi::rpc::RpcLog;

use crate::{hypersync::HypersyncLog, rpc::helpers as rpc_helpers};

pub(super) struct EventMetadata {
    pub block_number: u64,
    pub transaction_hash: String,
    pub transaction_index: u32,
    pub log_index: u32,
}

pub(super) fn hypersync_metadata(log: &HypersyncLog) -> anyhow::Result<EventMetadata> {
    let transaction_index = log
        .transaction_index
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing transaction index in the log"))?;
    let log_index = log
        .log_index
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing log index in the log"))?;

    Ok(EventMetadata {
        block_number: log
            .block_number
            .as_ref()
            .map(|number| **number)
            .ok_or_else(|| anyhow::anyhow!("Missing block number in log"))?,
        transaction_hash: log
            .transaction_hash
            .as_ref()
            .map(ToString::to_string)
            .ok_or_else(|| anyhow::anyhow!("Missing transaction hash in log"))?,
        transaction_index: u32::try_from(**transaction_index)
            .map_err(|_| anyhow::anyhow!("Transaction index exceeds u32"))?,
        log_index: u32::try_from(**log_index)
            .map_err(|_| anyhow::anyhow!("Log index exceeds u32"))?,
    })
}

pub(super) fn rpc_metadata(log: &RpcLog) -> anyhow::Result<EventMetadata> {
    Ok(EventMetadata {
        block_number: rpc_helpers::extract_block_number(log)?,
        transaction_hash: rpc_helpers::extract_transaction_hash(log)?,
        transaction_index: rpc_helpers::extract_transaction_index(log)?,
        log_index: rpc_helpers::extract_log_index(log)?,
    })
}

pub(super) fn validate_topic_count(
    actual: usize,
    expected: usize,
    name: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        actual == expected,
        "{name} event must have exactly {expected} topics, was {actual}"
    );
    Ok(())
}

pub(super) fn hypersync_topic<'a>(
    log: &'a HypersyncLog,
    index: usize,
    name: &str,
) -> anyhow::Result<&'a [u8]> {
    log.topics
        .get(index)
        .and_then(Option::as_ref)
        .map(AsRef::as_ref)
        .ok_or_else(|| anyhow::anyhow!("Missing {name} in topic{index}"))
}

pub(super) fn parse_pool_id(topic: &[u8]) -> anyhow::Result<B256> {
    anyhow::ensure!(
        topic.len() == B256::len_bytes(),
        "PoolId topic must be 32 bytes, was {}",
        topic.len()
    );
    Ok(B256::from_slice(topic))
}

pub(super) fn parse_indexed_address(topic: &[u8], name: &str) -> anyhow::Result<Address> {
    anyhow::ensure!(
        topic.len() == 32,
        "{name} topic must be 32 bytes, was {}",
        topic.len()
    );
    anyhow::ensure!(
        topic[..12].iter().all(|byte| *byte == 0),
        "{name} topic has non-zero address padding"
    );
    Ok(Address::from_slice(&topic[12..]))
}

pub(super) fn hypersync_data<'a>(
    log: &'a HypersyncLog,
    expected: usize,
    name: &str,
) -> anyhow::Result<&'a [u8]> {
    let data = log
        .data
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing data in {name} event log"))?
        .as_ref();
    validate_data_length(data, expected, name)?;
    Ok(data)
}

pub(super) fn rpc_data(log: &RpcLog, expected: usize, name: &str) -> anyhow::Result<Vec<u8>> {
    let data = rpc_helpers::extract_data_bytes(log)?;
    validate_data_length(&data, expected, name)?;
    Ok(data)
}

fn validate_data_length(data: &[u8], expected: usize, name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        data.len() == expected,
        "{name} event data must be exactly {expected} bytes, was {}",
        data.len()
    );
    Ok(())
}

#[cfg(test)]
pub(super) fn abi_generated_logs(topics: Vec<String>, data: Vec<u8>) -> (HypersyncLog, RpcLog) {
    use nautilus_core::hex;
    use serde_json::json;

    let data = hex::encode_prefixed(data);
    let hypersync = serde_json::from_value(json!({
        "removed": null,
        "log_index": "0x7",
        "transaction_index": "0x3",
        "transaction_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "block_hash": null,
        "block_number": "0x1234",
        "address": "0x0000000000000000000000000000000000000001",
        "data": data,
        "topics": topics,
    }))
    .expect("valid ABI-generated HyperSync fixture");
    let rpc = RpcLog {
        removed: false,
        log_index: Some("0x7".to_string()),
        transaction_index: Some("0x3".to_string()),
        transaction_hash: Some(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        ),
        block_hash: None,
        block_number: Some("0x1234".to_string()),
        address: "0x0000000000000000000000000000000000000001".to_string(),
        data,
        topics,
    };
    (hypersync, rpc)
}
