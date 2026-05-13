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

//! Python bindings for Lighter configuration.

use std::collections::HashMap;

use pyo3::prelude::*;

use crate::config::{LighterDataClientConfig, LighterEnvironment, LighterExecClientConfig};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl LighterDataClientConfig {
    /// Configuration for the Lighter data client.
    #[new]
    #[pyo3(signature = (
        environment = None,
        base_url_http = None,
        base_url_ws = None,
        proxy_url = None,
        http_timeout_secs = None,
        ws_timeout_secs = None,
        update_instruments_interval_mins = None,
    ))]
    fn py_new(
        environment: Option<LighterEnvironment>,
        base_url_http: Option<String>,
        base_url_ws: Option<String>,
        proxy_url: Option<String>,
        http_timeout_secs: Option<u64>,
        ws_timeout_secs: Option<u64>,
        update_instruments_interval_mins: Option<u64>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            base_url_http,
            base_url_ws,
            proxy_url,
            environment: environment.unwrap_or(defaults.environment),
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            ws_timeout_secs: ws_timeout_secs.unwrap_or(defaults.ws_timeout_secs),
            update_instruments_interval_mins: update_instruments_interval_mins
                .unwrap_or(defaults.update_instruments_interval_mins),
            transport_backend: defaults.transport_backend,
        }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl LighterExecClientConfig {
    /// Configuration for the Lighter execution client.
    #[new]
    #[pyo3(signature = (
        account_index = None,
        private_key = None,
        api_key_index = None,
        api_private_keys = None,
        signer_lib_path = None,
        environment = None,
        base_url_http = None,
        base_url_ws = None,
        proxy_url = None,
        http_timeout_secs = None,
        ws_timeout_secs = None,
        nonce_mode = None,
        default_auth_token_ttl_secs = None,
        cancel_all_gtt_secs = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        account_index: Option<i64>,
        private_key: Option<String>,
        api_key_index: Option<u8>,
        api_private_keys: Option<HashMap<u8, String>>,
        signer_lib_path: Option<String>,
        environment: Option<LighterEnvironment>,
        base_url_http: Option<String>,
        base_url_ws: Option<String>,
        proxy_url: Option<String>,
        http_timeout_secs: Option<u64>,
        ws_timeout_secs: Option<u64>,
        nonce_mode: Option<String>,
        default_auth_token_ttl_secs: Option<u64>,
        cancel_all_gtt_secs: Option<u64>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            account_index,
            private_key,
            api_key_index,
            api_private_keys,
            signer_lib_path,
            base_url_http,
            base_url_ws,
            proxy_url,
            environment: environment.unwrap_or(defaults.environment),
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            ws_timeout_secs: ws_timeout_secs.unwrap_or(defaults.ws_timeout_secs),
            nonce_mode: nonce_mode.unwrap_or(defaults.nonce_mode),
            default_auth_token_ttl_secs: default_auth_token_ttl_secs
                .unwrap_or(defaults.default_auth_token_ttl_secs),
            cancel_all_gtt_secs: cancel_all_gtt_secs.unwrap_or(defaults.cancel_all_gtt_secs),
            transport_backend: defaults.transport_backend,
        }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
