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

//! Configuration structures for the Variational adapter.

use serde::{Deserialize, Serialize};

pub const VARIATIONAL_OMNI_HTTP_BASE_URL: &str =
    "https://omni-client-api.prod.ap-northeast-1.variational.io";
pub const VARIATIONAL_OMNI_WS_BASE_URL: &str =
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io";
pub const VARIATIONAL_WS_PRICE_FUNDING_INTERVAL_SECS: u64 = 3_600;

#[must_use]
pub fn variational_http_base_url() -> String {
    VARIATIONAL_OMNI_HTTP_BASE_URL.to_string()
}

#[must_use]
pub fn variational_ws_base_url() -> String {
    VARIATIONAL_OMNI_WS_BASE_URL.to_string()
}

/// Quote tier to map into Nautilus `QuoteTick` data.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        eq,
        eq_int,
        module = "nautilus_trader.core.nautilus_pyo3.variational",
        from_py_object,
        rename_all = "SCREAMING_SNAKE_CASE",
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.variational")
)]
pub enum VariationalQuoteTier {
    /// Venue-provided base quote, when present.
    #[default]
    Base,
    /// Quote for USD 1,000 notional.
    Size1k,
    /// Quote for USD 100,000 notional.
    Size100k,
    /// Quote for USD 1,000,000 notional.
    Size1m,
}

impl VariationalQuoteTier {
    #[must_use]
    pub const fn key(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::Size1k => "size_1k",
            Self::Size100k => "size_100k",
            Self::Size1m => "size_1m",
        }
    }

    #[must_use]
    pub const fn notional_usdc(self) -> Option<u64> {
        match self {
            Self::Base => None,
            Self::Size1k => Some(1_000),
            Self::Size100k => Some(100_000),
            Self::Size1m => Some(1_000_000),
        }
    }
}

/// Configuration for the Variational read-only data client.
#[derive(Clone, Debug, bon::Builder)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.core.nautilus_pyo3.variational",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.variational")
)]
pub struct VariationalDataClientConfig {
    /// Optional HTTP API base URL override.
    pub base_url_http: Option<String>,
    /// Optional WebSocket API base URL override.
    pub base_url_ws: Option<String>,
    /// Optional HTTP proxy URL.
    pub proxy_url: Option<String>,
    /// HTTP request timeout in seconds.
    #[builder(default = 30)]
    pub http_timeout_secs: u64,
    /// Polling interval in seconds for subscription updates.
    #[builder(default = 30)]
    pub poll_interval_secs: u64,
    /// Quote tier used for Nautilus quote ticks.
    #[builder(default)]
    pub quote_tier: VariationalQuoteTier,
    /// Size precision to use because the public API does not expose quantity increments.
    #[builder(default = 8)]
    pub default_size_precision: u8,
    /// Funding interval required by Variational's public price WebSocket instrument payloads.
    #[builder(default = VARIATIONAL_WS_PRICE_FUNDING_INTERVAL_SECS)]
    pub ws_price_funding_interval_secs: u64,
}

impl Default for VariationalDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl VariationalDataClientConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn http_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(variational_http_base_url)
    }

    #[must_use]
    pub fn ws_prices_url(&self) -> String {
        let base_url = self
            .base_url_ws
            .clone()
            .unwrap_or_else(variational_ws_base_url);
        format!("{}/prices", base_url.trim_end_matches('/'))
    }
}
