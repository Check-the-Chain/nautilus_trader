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

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
    str::FromStr,
};

use alloy::primitives::{Address, B256, U256};
use bytes::Bytes;
use nautilus_core::hex;
use nautilus_model::defi::{
    Block,
    rpc::{RpcLog, RpcNodeHttpResponse},
};
use nautilus_network::{
    http::{HttpClient, Method},
    ratelimiter::quota::Quota,
};
use serde::{Deserialize, de::DeserializeOwned};

use crate::rpc::error::BlockchainRpcClientError;

#[derive(Debug, Deserialize)]
struct RpcBlockNumber {
    number: String,
    hash: String,
}

/// Client for making HTTP-based RPC requests to blockchain nodes.
///
/// This client is designed to interact with Ethereum-compatible blockchain networks, providing
/// methods to execute RPC calls and handle responses in a type-safe manner.
#[derive(Debug)]
pub struct BlockchainHttpRpcClient {
    /// The HTTP URL for the blockchain node's RPC endpoint.
    http_rpc_url: String,
    /// The HTTP client for making RPC http-based requests.
    http_client: HttpClient,
}

impl BlockchainHttpRpcClient {
    /// Creates a new HTTP RPC client with the given endpoint URL and optional rate limit.
    ///
    /// If `rpc_request_per_second` is `Some(0)` or an invalid value, rate limiting is disabled.
    ///
    /// # Panics
    ///
    /// Panics if the internal HTTP client cannot be created.
    #[must_use]
    pub fn new(
        http_rpc_url: String,
        rpc_request_per_second: Option<u32>,
        proxy_url: Option<String>,
    ) -> Self {
        let default_quota =
            rpc_request_per_second.and_then(|rps| Quota::per_second(NonZeroU32::new(rps)?));
        let http_client = HttpClient::new(
            HashMap::new(),
            vec![],
            Vec::new(),
            default_quota,
            None, // timeout_secs
            proxy_url,
        )
        .expect("Failed to create HTTP client");
        Self {
            http_rpc_url,
            http_client,
        }
    }

    /// Generic method that sends a JSON-RPC request and returns the raw response in bytes.
    async fn send_rpc_request(
        &self,
        rpc_request: serde_json::Value,
    ) -> Result<Bytes, BlockchainRpcClientError> {
        let body_bytes = serde_json::to_vec(&rpc_request).map_err(|e| {
            BlockchainRpcClientError::ClientError(format!("Failed to serialize request: {e}"))
        })?;

        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());

        match self
            .http_client
            .request(
                Method::POST,
                self.http_rpc_url.clone(),
                None,
                Some(headers),
                Some(body_bytes),
                None,
                None,
            )
            .await
        {
            Ok(response) => Ok(response.body),
            Err(e) => Err(BlockchainRpcClientError::ClientError(e.to_string())),
        }
    }

    /// Executes an Ethereum JSON-RPC call and deserializes the response into the specified type T.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP RPC request fails or the response cannot be parsed.
    pub async fn execute_rpc_call<T: DeserializeOwned>(
        &self,
        rpc_request: serde_json::Value,
    ) -> anyhow::Result<T> {
        match self.send_rpc_request(rpc_request).await {
            Ok(bytes) => match serde_json::from_slice::<RpcNodeHttpResponse<T>>(bytes.as_ref()) {
                Ok(parsed) => {
                    // Check for non-standard rate limit error (e.g., Infura)
                    // These responses have code/message at top level without jsonrpc field
                    if parsed.jsonrpc.is_none()
                        && let (Some(code), Some(message)) = (parsed.code, parsed.message)
                    {
                        anyhow::bail!("RPC provider error {code}: {message}");
                    }

                    if let Some(error) = parsed.error {
                        Err(anyhow::anyhow!(
                            "RPC error {}: {}",
                            error.code,
                            error.message
                        ))
                    } else if let Some(result) = parsed.result {
                        Ok(result)
                    } else {
                        Err(anyhow::anyhow!(
                            "Response missing both result and error fields"
                        ))
                    }
                }
                Err(e) => {
                    // Try to convert bytes to string for better error reporting
                    let raw_response = String::from_utf8_lossy(bytes.as_ref());
                    let preview = if raw_response.len() > 500 {
                        format!(
                            "{}... (truncated, {} bytes total)",
                            &raw_response[..500],
                            raw_response.len()
                        )
                    } else {
                        raw_response.to_string()
                    };

                    Err(anyhow::anyhow!(
                        "Failed to parse eth call response: {e}\nRaw response: {preview}"
                    ))
                }
            },
            Err(e) => Err(anyhow::anyhow!(
                "Failed to execute eth call RPC request: {e}"
            )),
        }
    }

    /// Creates a properly formatted `eth_call` JSON-RPC request object targeting a specific contract address with encoded function data.
    #[must_use]
    pub fn construct_eth_call(
        &self,
        to: &str,
        call_data: &[u8],
        block: Option<u64>,
    ) -> serde_json::Value {
        let encoded_data = hex::encode_prefixed(call_data);
        let call = serde_json::json!({
            "to": to,
            "data": encoded_data
        });

        let block_param = if let Some(block_number) = block {
            serde_json::json!(format!("0x{:x}", block_number))
        } else {
            serde_json::json!("latest")
        };

        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [call, block_param]
        })
    }

    /// Retrieves the balance of the specified Ethereum address at the given block.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC call fails or if the returned balance string cannot be parsed as a valid U256.
    pub async fn get_balance(&self, address: &Address, block: Option<u64>) -> anyhow::Result<U256> {
        let block_param = if let Some(block_number) = block {
            serde_json::json!(format!("0x{:x}", block_number))
        } else {
            serde_json::json!("latest")
        };

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBalance",
            "params": [address, block_param]
        });
        let hex_string: String = self.execute_rpc_call(request).await?;

        U256::from_str(&hex_string)
            .map_err(|e| anyhow::anyhow!("Failed to parse balance hex string '{hex_string}': {e}"))
    }

    /// Returns the latest block number reported by the RPC node.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC call fails or the result is not a valid hexadecimal block
    /// number.
    pub async fn current_block(&self) -> anyhow::Result<u64> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_blockNumber",
            "params": []
        });
        let block_number: String = self.execute_rpc_call(request).await?;
        crate::rpc::helpers::parse_hex_u64(&block_number)
    }

    /// Returns the latest finalized block number reported by the RPC node.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC call fails, the node does not support the `finalized` block tag,
    /// or the result does not contain a valid hexadecimal block number.
    pub async fn finalized_block(&self) -> anyhow::Result<u64> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByNumber",
            "params": ["finalized", false]
        });
        let block: RpcBlockNumber = self.execute_rpc_call(request).await?;
        crate::rpc::helpers::parse_hex_u64(&block.number)
    }

    /// Returns the canonical hash currently reported for an explicit block number.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC call fails or the block response is malformed.
    pub async fn block_hash(&self, block_number: u64) -> anyhow::Result<String> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByNumber",
            "params": [format!("0x{block_number:x}"), false]
        });
        let block: RpcBlockNumber = self.execute_rpc_call(request).await?;
        anyhow::ensure!(
            crate::rpc::helpers::parse_hex_u64(&block.number)? == block_number,
            "RPC returned a different block number than requested"
        );
        Ok(block.hash)
    }

    /// Returns the block header for an explicit block number.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC call fails, the block response is malformed, or the node
    /// returns a different block number than requested.
    pub async fn block(&self, block_number: u64) -> anyhow::Result<Block> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByNumber",
            "params": [format!("0x{block_number:x}"), false]
        });
        let block: Block = self.execute_rpc_call(request).await?;
        anyhow::ensure!(
            block.number == block_number,
            "RPC returned block {} when block {block_number} was requested",
            block.number,
        );
        Ok(block)
    }

    /// Retrieves logs matching the given filter criteria.
    ///
    /// This method calls the `eth_getLogs` RPC method to fetch event logs from the blockchain.
    /// It's commonly used for querying historical events like token transfers, swaps, etc.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC call fails or the response cannot be parsed.
    pub async fn get_logs(
        &self,
        address: Option<&Address>,
        topics: Option<Vec<Option<String>>>,
        from_block: u64,
        to_block: u64,
    ) -> anyhow::Result<Vec<RpcLog>> {
        let mut filter = serde_json::Map::new();

        filter.insert(
            "fromBlock".to_string(),
            serde_json::json!(format!("0x{:x}", from_block)),
        );
        filter.insert(
            "toBlock".to_string(),
            serde_json::json!(format!("0x{:x}", to_block)),
        );

        if let Some(addr) = address {
            filter.insert(
                "address".to_string(),
                serde_json::json!(format!("{:?}", addr)),
            );
        }

        if let Some(topics) = topics {
            filter.insert("topics".to_string(), serde_json::json!(topics));
        }

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getLogs",
            "params": [filter]
        });

        self.execute_rpc_call(request).await
    }

    /// Retrieves logs using typed OR alternatives at each indexed topic position.
    ///
    /// Each outer vector element is one topic position and each inner vector contains the hashes
    /// accepted at that position. For example, two inner vectors serialize as
    /// `topics: [[topic0_a, topic0_b], [topic1_a, topic1_b]]`.
    ///
    /// # Errors
    ///
    /// Returns an error when the range is inverted, there are more than four topic positions, a
    /// position has no alternatives or contains duplicates, or the RPC call fails.
    pub async fn get_logs_with_topic_alternatives(
        &self,
        address: Option<&Address>,
        topic_alternatives: &[Vec<B256>],
        from_block: u64,
        to_block: u64,
    ) -> anyhow::Result<Vec<RpcLog>> {
        let filter =
            construct_topic_alternatives_filter(address, topic_alternatives, from_block, to_block)?;
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getLogs",
            "params": [filter]
        });

        self.execute_rpc_call(request).await
    }
}

fn construct_topic_alternatives_filter(
    address: Option<&Address>,
    topic_alternatives: &[Vec<B256>],
    from_block: u64,
    to_block: u64,
) -> anyhow::Result<serde_json::Value> {
    anyhow::ensure!(
        from_block <= to_block,
        "eth_getLogs block range is inverted: {from_block}..={to_block}"
    );
    anyhow::ensure!(
        topic_alternatives.len() <= 4,
        "eth_getLogs supports at most four topic positions"
    );

    for (position, alternatives) in topic_alternatives.iter().enumerate() {
        anyhow::ensure!(
            !alternatives.is_empty(),
            "eth_getLogs topic position {position} has no alternatives"
        );
        let unique = alternatives.iter().copied().collect::<HashSet<_>>();
        anyhow::ensure!(
            unique.len() == alternatives.len(),
            "eth_getLogs topic position {position} contains duplicate alternatives"
        );
    }

    let topics = topic_alternatives
        .iter()
        .map(|alternatives| {
            alternatives
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut filter = serde_json::Map::new();
    filter.insert(
        "fromBlock".to_string(),
        serde_json::json!(format!("0x{from_block:x}")),
    );
    filter.insert(
        "toBlock".to_string(),
        serde_json::json!(format!("0x{to_block:x}")),
    );
    if let Some(address) = address {
        filter.insert(
            "address".to_string(),
            serde_json::json!(address.to_string()),
        );
    }
    if !topics.is_empty() {
        filter.insert("topics".to_string(), serde_json::json!(topics));
    }
    Ok(serde_json::Value::Object(filter))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, address};

    use super::{BlockchainHttpRpcClient, construct_topic_alternatives_filter};

    #[test]
    fn topic_alternatives_filter_is_typed_and_deterministic() {
        let address = address!("8366a39CC670B4001A1121B8F6A443A643e40951");
        let topic0 = vec![B256::repeat_byte(2), B256::repeat_byte(1)];
        let topic1 = vec![B256::repeat_byte(4), B256::repeat_byte(3)];

        let filter = construct_topic_alternatives_filter(
            Some(&address),
            &[topic0.clone(), topic1.clone()],
            10,
            20,
        )
        .unwrap();

        assert_eq!(filter["fromBlock"], "0xa");
        assert_eq!(filter["toBlock"], "0x14");
        assert_eq!(filter["address"], address.to_string());
        assert_eq!(
            filter["topics"],
            serde_json::json!([
                topic0.iter().map(ToString::to_string).collect::<Vec<_>>(),
                topic1.iter().map(ToString::to_string).collect::<Vec<_>>(),
            ])
        );
    }

    #[test]
    fn topic_alternatives_filter_rejects_invalid_shapes() {
        let duplicate = B256::repeat_byte(1);
        assert!(construct_topic_alternatives_filter(None, &[Vec::new()], 0, 1).is_err());
        assert!(
            construct_topic_alternatives_filter(None, &[vec![duplicate, duplicate]], 0, 1).is_err()
        );
        assert!(construct_topic_alternatives_filter(None, &[vec![duplicate]], 2, 1).is_err());
        assert!(
            construct_topic_alternatives_filter(
                None,
                &[
                    vec![duplicate],
                    vec![duplicate],
                    vec![duplicate],
                    vec![duplicate],
                    vec![duplicate],
                ],
                0,
                1,
            )
            .is_err()
        );
    }

    #[tokio::test]
    #[ignore = "requires live Robinhood Chain RPC access"]
    async fn live_robinhood_rpc_returns_initialize_logs() {
        let client = BlockchainHttpRpcClient::new(
            "https://rpc.mainnet.chain.robinhood.com".to_string(),
            None,
            None,
        );
        let head = client.current_block().await.expect("current block");
        assert!(head >= 19_069);
        let finalized = client.finalized_block().await.expect("finalized block");
        assert!((19_069..=head).contains(&finalized));
        let block = client.block(finalized).await.expect("finalized block header");
        assert_eq!(block.number, finalized);
        assert!(block.timestamp.as_u64() > 0);

        let pool_manager = address!("8366a39CC670B4001A1121B8F6A443A643e40951");
        let initialize = "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438";
        let logs = client
            .get_logs(
                Some(&pool_manager),
                Some(vec![Some(initialize.to_string())]),
                9_070,
                19_069,
            )
            .await
            .expect("Initialize logs");

        assert_eq!(logs.len(), 2);
        assert!(logs.iter().all(|log| !log.removed));
        assert!(logs.iter().all(|log| {
            log.address.eq_ignore_ascii_case(&pool_manager.to_string())
                && log.topics.first().is_some_and(|topic| topic == initialize)
        }));
    }
}
