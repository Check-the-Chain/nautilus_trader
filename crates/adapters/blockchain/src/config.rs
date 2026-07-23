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

use std::any::Any;

use nautilus_common::factories::ClientConfig;
use nautilus_infrastructure::sql::pg::PostgresConnectOptions;
use nautilus_model::{
    defi::{Chain, DexType, SharedChain},
    identifiers::{AccountId, TraderId},
};
use nautilus_network::websocket::TransportBackend;
use serde::{Deserialize, Serialize};

/// Defines filtering criteria for the DEX pool universe that the data client will operate on.
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.core.nautilus_pyo3.blockchain",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.blockchain")
)]
pub struct DexPoolFilters {
    /// Whether to exclude pools containing tokens with empty name or symbol fields.
    #[builder(default = true)]
    pub remove_pools_with_empty_erc20fields: bool,
}

impl Default for DexPoolFilters {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Explicit controls for one selected Uniswap v4 mirror universe.
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.core.nautilus_pyo3.blockchain",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.blockchain")
)]
pub struct UniswapV4MirrorDataConfig {
    /// Address of the StateView contract bound to the configured Uniswap v4 PoolManager.
    pub state_view_address: String,
    /// Selected bytes32 Pool IDs. Complete PoolKeys are resolved from authenticated discovery.
    pub pool_ids: Vec<String>,
    /// Maximum local monotonic interval without an advancing WSS head.
    pub head_timeout_ms: u64,
}

/// Configuration for blockchain data clients.
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.core.nautilus_pyo3.blockchain",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.blockchain")
)]
pub struct BlockchainDataClientConfig {
    /// The blockchain chain configuration.
    pub chain: SharedChain,
    /// List of decentralized exchange IDs to register and sync during connection.
    #[builder(default)]
    #[serde(default)]
    pub dex_ids: Vec<DexType>,
    /// Determines if the client should use Hypersync for live data streaming.
    #[builder(default)]
    #[serde(default)]
    pub use_hypersync_for_live_data: bool,
    /// The HTTP URL for the blockchain RPC endpoint.
    pub http_rpc_url: String,
    /// The maximum number of RPC requests allowed per second.
    pub rpc_requests_per_second: Option<u32>,
    /// The maximum number of Multicall calls per one RPC request.
    #[builder(default = 200)]
    #[serde(default = "default_multicall_calls_per_rpc_request")]
    pub multicall_calls_per_rpc_request: u32,
    /// The WebSocket secure URL for the blockchain RPC endpoint.
    pub wss_rpc_url: Option<String>,
    /// Optional proxy URL for HTTP and WebSocket transports.
    pub proxy_url: Option<String>,
    /// The block from which to sync historical data.
    pub from_block: Option<u64>,
    /// Filtering criteria that define which DEX pools to include in the data universe.
    #[builder(default)]
    #[serde(default)]
    pub pool_filters: DexPoolFilters,
    /// Optional selected-pool Uniswap v4 mirror controls.
    #[serde(default)]
    pub uniswap_v4_mirror: Option<UniswapV4MirrorDataConfig>,
    /// Optional configuration for data client's Postgres cache database
    pub postgres_cache_database_config: Option<PostgresConnectOptions>,
    /// WebSocket transport backend (defaults to `Tungstenite`).
    #[builder(default)]
    #[serde(default)]
    pub transport_backend: TransportBackend,
}

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(BlockchainDataClientConfig {
    dex_ids: Vec<DexType>,
    multicall_calls_per_rpc_request: u32,
    pool_filters: DexPoolFilters,
    transport_backend: TransportBackend,
    uniswap_v4_mirror: Option<UniswapV4MirrorDataConfig>,
});

const fn default_multicall_calls_per_rpc_request() -> u32 {
    200
}

#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct BlockchainExecutionClientConfig {
    /// The trader ID for the client.
    pub trader_id: TraderId,
    /// The account ID for the client.
    pub client_id: AccountId,
    /// The blockchain chain configuration.
    pub chain: Chain,
    /// The wallet address of the execution client.
    pub wallet_address: String,
    /// Token universe: set of ERC-20 token addresses to monitor for balance tracking.
    pub tokens: Option<Vec<String>>,
    /// The HTTP URL for the blockchain RPC endpoint.
    pub http_rpc_url: String,
    /// The maximum number of RPC requests allowed per second.
    pub rpc_requests_per_second: Option<u32>,
    /// WebSocket transport backend (defaults to `Tungstenite`).
    #[builder(default)]
    #[serde(default)]
    pub transport_backend: TransportBackend,
}

impl ClientConfig for BlockchainExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::defi::Blockchain;
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_data_config_toml_minimal() {
        let config: BlockchainDataClientConfig = toml::from_str(
            r#"
http_rpc_url = "https://eth-mainnet.example.com"

[chain]
name = "Ethereum"
chain_id = 1
hypersync_url = "https://1.hypersync.xyz"
native_currency_decimals = 18
"#,
        )
        .unwrap();

        assert_eq!(config.http_rpc_url, "https://eth-mainnet.example.com");
        assert_eq!(config.chain.chain_id, 1);
        assert!(config.dex_ids.is_empty());
        assert!(!config.use_hypersync_for_live_data);
        assert_eq!(config.multicall_calls_per_rpc_request, 200);
        assert!(config.pool_filters.remove_pools_with_empty_erc20fields);
        assert_eq!(config.transport_backend, TransportBackend::default());
        assert!(config.uniswap_v4_mirror.is_none());
    }

    #[rstest]
    fn test_data_config_toml_robinhood_rpc() {
        let config: BlockchainDataClientConfig = toml::from_str(
            r#"
http_rpc_url = "https://rpc.mainnet.chain.robinhood.com"
wss_rpc_url = "ws://127.0.0.1:8548"
use_hypersync_for_live_data = false

[chain]
name = "Robinhood"
chain_id = 4663
hypersync_url = "https://4663.hypersync.xyz"
native_currency_decimals = 18
"#,
        )
        .unwrap();

        assert_eq!(config.chain.name, Blockchain::Robinhood);
        assert_eq!(config.chain.chain_id, 4663);
        assert_eq!(config.wss_rpc_url.as_deref(), Some("ws://127.0.0.1:8548"));
        assert!(!config.use_hypersync_for_live_data);
    }

    #[test]
    fn test_data_config_toml_uniswap_v4_mirror() {
        let config: BlockchainDataClientConfig = toml::from_str(
            r#"
http_rpc_url = "https://robinhood-mainnet.example.com"
wss_rpc_url = "wss://robinhood-mainnet.example.com"
dex_ids = ["UniswapV4"]

[uniswap_v4_mirror]
state_view_address = "0xF3334192D15450CdD385c8B70e03f9A6bD9E673b"
pool_ids = ["0x3bb34a44f1b2b5f32c034c38a53065a521a47b199700fa9bd19d60985ff24bf1"]
head_timeout_ms = 5000

[chain]
name = "Robinhood"
chain_id = 4663
hypersync_url = "https://4663.hypersync.xyz"
native_currency_decimals = 18
"#,
        )
        .unwrap();

        let mirror = config.uniswap_v4_mirror.unwrap();
        assert_eq!(mirror.pool_ids.len(), 1);
        assert_eq!(mirror.head_timeout_ms, 5_000);
    }

    #[rstest]
    fn test_execution_config_toml_minimal() {
        let config: BlockchainExecutionClientConfig = toml::from_str(
            r#"
trader_id = "TRADER-001"
client_id = "BLOCKCHAIN-001"
wallet_address = "0x0000000000000000000000000000000000000000"
http_rpc_url = "https://eth-mainnet.example.com"

[chain]
name = "Ethereum"
chain_id = 1
hypersync_url = "https://1.hypersync.xyz"
native_currency_decimals = 18
"#,
        )
        .unwrap();

        assert_eq!(config.http_rpc_url, "https://eth-mainnet.example.com");
        assert_eq!(config.chain.chain_id, 1);
        assert_eq!(
            config.wallet_address,
            "0x0000000000000000000000000000000000000000",
        );
        assert!(config.tokens.is_none());
        assert!(config.rpc_requests_per_second.is_none());
        assert_eq!(config.transport_backend, TransportBackend::default());
    }
}
