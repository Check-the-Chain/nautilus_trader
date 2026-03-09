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

//! Configuration structures for the Lighter adapter.

use std::collections::HashMap;

use crate::constants::{MAINNET_CHAIN_ID, TESTNET_CHAIN_ID};

pub const LIGHTER_MAINNET_HOST: &str = "mainnet.zklighter.elliot.ai";
pub const LIGHTER_TESTNET_HOST: &str = "testnet.zklighter.elliot.ai";

#[must_use]
pub fn lighter_http_base_url(is_testnet: bool) -> String {
    let host = if is_testnet {
        LIGHTER_TESTNET_HOST
    } else {
        LIGHTER_MAINNET_HOST
    };
    format!("https://{host}")
}

#[must_use]
pub fn lighter_ws_base_url(is_testnet: bool) -> String {
    let host = if is_testnet {
        LIGHTER_TESTNET_HOST
    } else {
        LIGHTER_MAINNET_HOST
    };
    format!("wss://{host}/stream")
}

/// Internal low-level configuration used by the imported Lighter protocol modules.
#[derive(Clone, Debug)]
pub struct Config {
    pub host: String,
    pub chain_id: u32,
    pub pool_size: usize,
    pub signer_lib_path: Option<String>,
    pub proxy: Option<String>,
    pub timeout_secs: Option<u64>,
    pub base_url_http: Option<String>,
    pub base_url_ws: Option<String>,
}

impl Config {
    #[must_use]
    pub fn new(host: impl Into<String>) -> Self {
        let host = host.into();
        let chain_id = if host.contains("testnet") {
            TESTNET_CHAIN_ID
        } else {
            MAINNET_CHAIN_ID
        };
        Self {
            host,
            chain_id,
            pool_size: 10,
            signer_lib_path: None,
            proxy: None,
            timeout_secs: None,
            base_url_http: None,
            base_url_ws: None,
        }
    }

    #[must_use]
    pub fn for_network(is_testnet: bool) -> Self {
        let host = if is_testnet {
            LIGHTER_TESTNET_HOST
        } else {
            LIGHTER_MAINNET_HOST
        };
        Self::new(host)
    }

    #[must_use]
    pub fn with_chain_id(mut self, chain_id: u32) -> Self {
        self.chain_id = chain_id;
        self
    }

    #[must_use]
    pub fn with_pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = pool_size;
        self
    }

    #[must_use]
    pub fn with_signer_lib_path(mut self, path: impl Into<String>) -> Self {
        self.signer_lib_path = Some(path.into());
        self
    }

    #[must_use]
    pub fn with_proxy(mut self, proxy: impl Into<String>) -> Self {
        self.proxy = Some(proxy.into());
        self
    }

    #[must_use]
    pub fn with_timeout_secs(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = Some(timeout_secs);
        self
    }

    #[must_use]
    pub fn with_http_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url_http = Some(url.into());
        self
    }

    #[must_use]
    pub fn with_ws_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url_ws = Some(url.into());
        self
    }

    #[must_use]
    pub fn api_base_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| format!("https://{}", self.host))
    }

    #[must_use]
    pub fn ws_base_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| format!("wss://{}/stream", self.host))
    }
}

/// Configuration for the Lighter data client.
#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.lighter", from_py_object)
)]
pub struct LighterDataClientConfig {
    pub base_url_http: Option<String>,
    pub base_url_ws: Option<String>,
    pub http_proxy_url: Option<String>,
    pub ws_proxy_url: Option<String>,
    pub is_testnet: bool,
    pub http_timeout_secs: Option<u64>,
    pub update_instruments_interval_mins: Option<u64>,
}

impl Default for LighterDataClientConfig {
    fn default() -> Self {
        Self {
            base_url_http: None,
            base_url_ws: None,
            http_proxy_url: None,
            ws_proxy_url: None,
            is_testnet: false,
            http_timeout_secs: Some(30),
            update_instruments_interval_mins: Some(60),
        }
    }
}

impl LighterDataClientConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn http_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| lighter_http_base_url(self.is_testnet))
    }

    #[must_use]
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| lighter_ws_base_url(self.is_testnet))
    }
}

/// Configuration for the Lighter execution client.
#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.lighter", from_py_object)
)]
pub struct LighterExecClientConfig {
    pub account_index: Option<i64>,
    pub private_key: Option<String>,
    pub api_key_index: Option<u8>,
    pub api_private_keys: Option<HashMap<u8, String>>,
    pub signer_lib_path: Option<String>,
    pub base_url_http: Option<String>,
    pub base_url_ws: Option<String>,
    pub http_proxy_url: Option<String>,
    pub ws_proxy_url: Option<String>,
    pub is_testnet: bool,
    pub http_timeout_secs: Option<u64>,
    pub nonce_mode: String,
    pub default_auth_token_ttl_secs: u64,
    pub cancel_all_gtt_secs: u64,
}

impl Default for LighterExecClientConfig {
    fn default() -> Self {
        Self {
            account_index: None,
            private_key: None,
            api_key_index: None,
            api_private_keys: None,
            signer_lib_path: None,
            base_url_http: None,
            base_url_ws: None,
            http_proxy_url: None,
            ws_proxy_url: None,
            is_testnet: false,
            http_timeout_secs: Some(30),
            nonce_mode: "optimistic".to_string(),
            default_auth_token_ttl_secs: 300,
            cancel_all_gtt_secs: 300,
        }
    }
}

impl LighterExecClientConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn http_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| lighter_http_base_url(self.is_testnet))
    }

    #[must_use]
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| lighter_ws_base_url(self.is_testnet))
    }

    #[must_use]
    pub fn credentials_map(&self) -> HashMap<u8, String> {
        if let Some(map) = &self.api_private_keys {
            return map.clone();
        }

        match (self.api_key_index, self.private_key.clone()) {
            (Some(index), Some(key)) => HashMap::from([(index, key)]),
            _ => HashMap::new(),
        }
    }
}
