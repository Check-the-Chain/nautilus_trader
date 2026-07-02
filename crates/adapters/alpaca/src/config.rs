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

//! Configuration structures for the Alpaca adapter.

use std::fmt::Debug;

use nautilus_core::string::secret::REDACTED;
use nautilus_model::identifiers::{AccountId, TraderId};
use nautilus_network::websocket::TransportBackend;
use serde::{Deserialize, Serialize};

use crate::common::{
    credential::credential_env_vars,
    enums::{AlpacaDataFeed, AlpacaEnvironment},
    urls::{
        alpaca_data_http_url, alpaca_stocks_stream_ws_url, alpaca_trade_updates_ws_url,
        alpaca_trading_http_url,
    },
};

/// Configuration for the Alpaca data client.
#[derive(Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
pub struct AlpacaDataClientConfig {
    /// Optional Market Data REST URL override.
    pub base_url_http: Option<String>,
    /// Optional market data stream URL override.
    pub base_url_ws: Option<String>,
    /// Optional proxy URL for HTTP and WebSocket transports.
    pub proxy_url: Option<String>,
    /// Target environment (selects the credential env-var pair).
    #[builder(default)]
    pub environment: AlpacaEnvironment,
    /// Market data feed subscription level.
    #[builder(default)]
    pub data_feed: AlpacaDataFeed,
    /// Alpaca API key ID. Falls back to `ALPACA_API_KEY` / `ALPACA_PAPER_API_KEY`.
    pub api_key: Option<String>,
    /// Alpaca API secret key. Falls back to `ALPACA_API_SECRET` / `ALPACA_PAPER_API_SECRET`.
    pub api_secret: Option<String>,
    /// HTTP request timeout in seconds.
    #[builder(default = 60)]
    pub http_timeout_secs: u64,
    /// WebSocket connect timeout in seconds.
    #[builder(default = 30)]
    pub ws_timeout_secs: u64,
    /// Refresh interval for instrument metadata in minutes.
    #[builder(default = 60)]
    pub update_instruments_interval_mins: u64,
    /// Use MessagePack framing on the market data stream (default `true`).
    ///
    /// MessagePack frames are smaller and decode faster than JSON; disable
    /// only for debugging against JSON captures.
    #[builder(default = true)]
    pub use_msgpack: bool,
    /// WebSocket transport backend.
    #[builder(default)]
    pub transport_backend: TransportBackend,
}

impl Default for AlpacaDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl AlpacaDataClientConfig {
    /// Creates a new configuration with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the resolved Market Data REST base URL.
    #[must_use]
    pub fn http_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| alpaca_data_http_url().to_string())
    }

    /// Returns the resolved market data stream URL.
    #[must_use]
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| alpaca_stocks_stream_ws_url(self.data_feed).to_string())
    }

    /// Returns `true` when both credential fields are available.
    #[must_use]
    pub fn has_credentials(&self) -> bool {
        let (key_var, secret_var) = credential_env_vars(self.environment);
        let has_key = self
            .api_key
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
            || env_var_is_set(key_var);
        let has_secret = self
            .api_secret
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
            || env_var_is_set(secret_var);

        has_key && has_secret
    }
}

impl Debug for AlpacaDataClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AlpacaDataClientConfig))
            .field("base_url_http", &self.base_url_http)
            .field("base_url_ws", &self.base_url_ws)
            .field("proxy_url", &self.proxy_url)
            .field("environment", &self.environment)
            .field("data_feed", &self.data_feed)
            .field("api_key", &self.api_key.as_ref().map(|_| REDACTED))
            .field("api_secret", &self.api_secret.as_ref().map(|_| REDACTED))
            .field("http_timeout_secs", &self.http_timeout_secs)
            .field("ws_timeout_secs", &self.ws_timeout_secs)
            .field(
                "update_instruments_interval_mins",
                &self.update_instruments_interval_mins,
            )
            .field("use_msgpack", &self.use_msgpack)
            .field("transport_backend", &self.transport_backend)
            .finish()
    }
}

/// Configuration for the Alpaca execution client.
#[derive(Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
pub struct AlpacaExecClientConfig {
    /// The trader ID for this client.
    #[builder(default)]
    pub trader_id: TraderId,
    /// The account ID for this client.
    #[builder(default = AccountId::from("ALPACA-001"))]
    pub account_id: AccountId,
    /// Optional Trading API REST URL override.
    pub base_url_http: Option<String>,
    /// Optional trade-updates stream URL override.
    pub base_url_ws: Option<String>,
    /// Optional proxy URL for HTTP and WebSocket transports.
    pub proxy_url: Option<String>,
    /// Target environment.
    #[builder(default)]
    pub environment: AlpacaEnvironment,
    /// Alpaca API key ID. Falls back to `ALPACA_API_KEY` / `ALPACA_PAPER_API_KEY`.
    pub api_key: Option<String>,
    /// Alpaca API secret key. Falls back to `ALPACA_API_SECRET` / `ALPACA_PAPER_API_SECRET`.
    pub api_secret: Option<String>,
    /// HTTP request timeout in seconds.
    #[builder(default = 60)]
    pub http_timeout_secs: u64,
    /// WebSocket connect timeout in seconds.
    #[builder(default = 30)]
    pub ws_timeout_secs: u64,
    /// WebSocket transport backend.
    #[builder(default)]
    pub transport_backend: TransportBackend,
}

impl Default for AlpacaExecClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl AlpacaExecClientConfig {
    /// Creates a new configuration with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the resolved Trading API base URL.
    #[must_use]
    pub fn http_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| alpaca_trading_http_url(self.environment).to_string())
    }

    /// Returns the resolved trade-updates stream URL.
    #[must_use]
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| alpaca_trade_updates_ws_url(self.environment).to_string())
    }

    /// Returns `true` when both credential fields are available.
    #[must_use]
    pub fn has_credentials(&self) -> bool {
        let (key_var, secret_var) = credential_env_vars(self.environment);
        let has_key = self
            .api_key
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
            || env_var_is_set(key_var);
        let has_secret = self
            .api_secret
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
            || env_var_is_set(secret_var);

        has_key && has_secret
    }
}

impl Debug for AlpacaExecClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AlpacaExecClientConfig))
            .field("trader_id", &self.trader_id)
            .field("account_id", &self.account_id)
            .field("base_url_http", &self.base_url_http)
            .field("base_url_ws", &self.base_url_ws)
            .field("proxy_url", &self.proxy_url)
            .field("environment", &self.environment)
            .field("api_key", &self.api_key.as_ref().map(|_| REDACTED))
            .field("api_secret", &self.api_secret.as_ref().map(|_| REDACTED))
            .field("http_timeout_secs", &self.http_timeout_secs)
            .field("ws_timeout_secs", &self.ws_timeout_secs)
            .field("transport_backend", &self.transport_backend)
            .finish()
    }
}

fn env_var_is_set(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| !v.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_data_config_default_urls() {
        let config = AlpacaDataClientConfig::default();

        assert_eq!(config.http_url(), "https://data.alpaca.markets");
        assert_eq!(config.ws_url(), "wss://stream.data.alpaca.markets/v2/iex");
    }

    #[rstest]
    fn test_data_config_url_overrides() {
        let config = AlpacaDataClientConfig::builder()
            .base_url_http("http://localhost:8080".to_string())
            .base_url_ws("ws://localhost:8081".to_string())
            .build();

        assert_eq!(config.http_url(), "http://localhost:8080");
        assert_eq!(config.ws_url(), "ws://localhost:8081");
    }

    #[rstest]
    fn test_data_config_sip_feed_url() {
        let config = AlpacaDataClientConfig::builder()
            .data_feed(AlpacaDataFeed::Sip)
            .build();

        assert_eq!(config.ws_url(), "wss://stream.data.alpaca.markets/v2/sip");
    }

    #[rstest]
    fn test_exec_config_default_urls() {
        let config = AlpacaExecClientConfig::default();

        assert_eq!(config.http_url(), "https://paper-api.alpaca.markets");
        assert_eq!(config.ws_url(), "wss://paper-api.alpaca.markets/stream");
    }

    #[rstest]
    fn test_exec_config_live_urls() {
        let config = AlpacaExecClientConfig::builder()
            .environment(AlpacaEnvironment::Live)
            .build();

        assert_eq!(config.http_url(), "https://api.alpaca.markets");
        assert_eq!(config.ws_url(), "wss://api.alpaca.markets/stream");
    }

    #[rstest]
    fn test_debug_redacts_credentials() {
        let config = AlpacaExecClientConfig::builder()
            .api_key("key-id".to_string())
            .api_secret("secret-key".to_string())
            .build();

        let dbg_out = format!("{config:?}");

        assert!(!dbg_out.contains("secret-key"));
        assert!(!dbg_out.contains("key-id"));
    }
}
