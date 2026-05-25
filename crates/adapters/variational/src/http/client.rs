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

//! HTTP client for Variational's public read-only API.

use std::time::Duration;

use reqwest::Client;

use crate::{
    config::variational_http_base_url,
    error::{Error, Result},
    models::VariationalStats,
};

/// Low-level HTTP client for Variational Omni.
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.core.nautilus_pyo3.variational",
        from_py_object
    )
)]
#[derive(Clone, Debug)]
pub struct VariationalHttpClient {
    client: Client,
    base_url: String,
}

impl VariationalHttpClient {
    /// Creates a new [`VariationalHttpClient`].
    pub fn new(
        base_url: Option<String>,
        proxy_url: Option<String>,
        timeout_secs: u64,
    ) -> Result<Self> {
        let mut builder = Client::builder().timeout(Duration::from_secs(timeout_secs));

        if let Some(proxy_url) = proxy_url {
            builder = builder.proxy(reqwest::Proxy::all(proxy_url)?);
        }

        Ok(Self {
            client: builder.build()?,
            base_url: base_url.unwrap_or_else(variational_http_base_url),
        })
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Requests current platform and per-listing statistics.
    pub async fn stats(&self) -> Result<VariationalStats> {
        let url = format!("{}/metadata/stats", self.base_url.trim_end_matches('/'));
        let response = self.client.get(url).send().await?;
        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            return Err(Error::Http {
                status: status.as_u16(),
                message: body,
            });
        }

        serde_json::from_str(&body).map_err(Into::into)
    }

    /// Requests current platform and per-listing statistics as JSON.
    pub async fn stats_json(&self) -> Result<String> {
        let stats = self.stats().await?;
        serde_json::to_string(&stats).map_err(Into::into)
    }
}

impl Default for VariationalHttpClient {
    fn default() -> Self {
        Self::new(None, None, 30).expect("default Variational HTTP client should build")
    }
}
