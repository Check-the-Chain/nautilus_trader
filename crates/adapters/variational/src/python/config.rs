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

//! Python bindings for Variational configuration.

use pyo3::prelude::*;

use crate::config::{VariationalDataClientConfig, VariationalQuoteTier};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl VariationalDataClientConfig {
    /// Configuration for the Variational data client.
    #[new]
    #[pyo3(signature = (
        base_url_http = None,
        base_url_ws = None,
        proxy_url = None,
        http_timeout_secs = None,
        poll_interval_secs = None,
        quote_tier = None,
        default_size_precision = None,
        ws_price_funding_interval_secs = None,
    ))]
    fn py_new(
        base_url_http: Option<String>,
        base_url_ws: Option<String>,
        proxy_url: Option<String>,
        http_timeout_secs: Option<u64>,
        poll_interval_secs: Option<u64>,
        quote_tier: Option<VariationalQuoteTier>,
        default_size_precision: Option<u8>,
        ws_price_funding_interval_secs: Option<u64>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            base_url_http,
            base_url_ws,
            proxy_url,
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            poll_interval_secs: poll_interval_secs.unwrap_or(defaults.poll_interval_secs),
            quote_tier: quote_tier.unwrap_or(defaults.quote_tier),
            default_size_precision: default_size_precision
                .unwrap_or(defaults.default_size_precision),
            ws_price_funding_interval_secs: ws_price_funding_interval_secs
                .unwrap_or(defaults.ws_price_funding_interval_secs),
        }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
