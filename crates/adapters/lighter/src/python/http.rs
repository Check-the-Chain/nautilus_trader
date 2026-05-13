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

use std::collections::HashMap;

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{
    client::{LighterCancelOrderRequest, LighterSubmitOrderRequest},
    config::{Config, lighter_http_base_url, lighter_ws_base_url},
    http::client::LighterHttpClient,
    nonce::NonceManagerType,
};

fn build_config(
    host: Option<String>,
    base_url_http: Option<String>,
    base_url_ws: Option<String>,
    is_testnet: bool,
    signer_lib_path: Option<String>,
    proxy_url: Option<String>,
    pool_size: usize,
    timeout_secs: Option<u64>,
) -> Config {
    let host = host.unwrap_or_else(|| {
        if is_testnet {
            crate::config::LIGHTER_TESTNET_HOST.to_string()
        } else {
            crate::config::LIGHTER_MAINNET_HOST.to_string()
        }
    });

    let mut config = Config::new(host).with_pool_size(pool_size);

    if let Some(path) = signer_lib_path {
        config = config.with_signer_lib_path(path);
    }
    if let Some(proxy_url) = proxy_url {
        config = config.with_proxy(proxy_url);
    }
    if let Some(timeout_secs) = timeout_secs {
        config = config.with_timeout_secs(timeout_secs);
    }

    config
        .with_http_base_url(base_url_http.unwrap_or_else(|| lighter_http_base_url(is_testnet)))
        .with_ws_base_url(base_url_ws.unwrap_or_else(|| lighter_ws_base_url(is_testnet)))
}

fn resolve_nonce_mode(value: &str) -> PyResult<NonceManagerType> {
    match value.to_ascii_lowercase().as_str() {
        "api" => Ok(NonceManagerType::Api),
        "optimistic" => Ok(NonceManagerType::Optimistic),
        other => Err(to_pyvalue_err(format!(
            "Invalid nonce_mode {other:?}, expected 'optimistic' or 'api'"
        ))),
    }
}

#[pymethods]
impl LighterHttpClient {
    #[new]
    #[pyo3(signature = (
        host=None,
        base_url_http=None,
        base_url_ws=None,
        is_testnet=false,
        signer_lib_path=None,
        proxy_url=None,
        account_index=None,
        private_key=None,
        api_key_index=None,
        api_private_keys=None,
        nonce_mode="optimistic",
        pool_size=10,
        timeout_secs=None,
    ))]
    fn py_new(
        host: Option<String>,
        base_url_http: Option<String>,
        base_url_ws: Option<String>,
        is_testnet: bool,
        signer_lib_path: Option<String>,
        proxy_url: Option<String>,
        account_index: Option<i64>,
        private_key: Option<String>,
        api_key_index: Option<u8>,
        api_private_keys: Option<HashMap<u8, String>>,
        nonce_mode: &str,
        pool_size: usize,
        timeout_secs: Option<u64>,
    ) -> PyResult<Self> {
        let config = build_config(
            host,
            base_url_http,
            base_url_ws,
            is_testnet,
            signer_lib_path,
            proxy_url,
            pool_size,
            timeout_secs,
        );

        let resolved_keys = api_private_keys.or_else(|| match (api_key_index, private_key) {
            (Some(index), Some(key)) => Some(HashMap::from([(index, key)])),
            _ => None,
        });

        match (account_index, resolved_keys) {
            (Some(account_index), Some(api_private_keys)) => {
                let nonce_mode = resolve_nonce_mode(nonce_mode)?;
                Self::with_signer(config, account_index, api_private_keys, nonce_mode)
                    .map_err(to_pyvalue_err)
            }
            _ => Self::new_public(config).map_err(to_pyvalue_err),
        }
    }

    #[getter]
    #[pyo3(name = "base_url_http")]
    fn py_base_url_http(&self) -> String {
        self.api_base_url()
    }

    #[getter]
    #[pyo3(name = "base_url_ws")]
    fn py_base_url_ws(&self) -> String {
        self.ws_base_url()
    }

    #[pyo3(name = "load_market_metadata")]
    fn py_load_market_metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .load_market_metadata_json()
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_order_books")]
    fn py_request_order_books<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest.get_order_books().await.map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_order_book_details")]
    fn py_request_order_book_details<'py>(
        &self,
        py: Python<'py>,
        market_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_order_book_details(market_id)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_order_book_snapshot", signature = (market_id, limit=100))]
    fn py_request_order_book_snapshot<'py>(
        &self,
        py: Python<'py>,
        market_id: i64,
        limit: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_order_book_orders(market_id, limit)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_recent_trades", signature = (market_id, limit=200))]
    fn py_request_recent_trades<'py>(
        &self,
        py: Python<'py>,
        market_id: i64,
        limit: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_recent_trades(market_id, limit)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_trades", signature = (market_id, cursor=None))]
    fn py_request_trades<'py>(
        &self,
        py: Python<'py>,
        market_id: i64,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_trades(market_id, cursor.as_deref())
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_candles", signature = (market_id, granularity, cursor=None))]
    fn py_request_candles<'py>(
        &self,
        py: Python<'py>,
        market_id: i64,
        granularity: String,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_candles(market_id, &granularity, cursor.as_deref())
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_funding_rates", signature = (market_id, cursor=None))]
    fn py_request_funding_rates<'py>(
        &self,
        py: Python<'py>,
        market_id: i64,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_funding_rates(market_id, cursor.as_deref())
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_exchange_stats")]
    fn py_request_exchange_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest.get_exchange_stats().await.map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_status", signature = ())]
    fn py_request_status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest.get_status().await.map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_system_config", signature = ())]
    fn py_request_system_config<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest.get_system_config().await.map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_announcements", signature = ())]
    fn py_request_announcements<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest.get_announcements().await.map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_exchange_metrics", signature = (
        period,
        kind,
        filter=None,
        value=None,
    ))]
    fn py_request_exchange_metrics<'py>(
        &self,
        py: Python<'py>,
        period: String,
        kind: String,
        filter: Option<String>,
        value: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_exchange_metrics(&period, &kind, filter.as_deref(), value.as_deref())
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_execute_stats", signature = (period))]
    fn py_request_execute_stats<'py>(
        &self,
        py: Python<'py>,
        period: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_execute_stats(&period)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_asset_details")]
    fn py_request_asset_details<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest.get_asset_details().await.map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_layer1_basic_info", signature = ())]
    fn py_request_layer1_basic_info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest.get_layer1_basic_info().await.map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_zk_lighter_info", signature = ())]
    fn py_request_zk_lighter_info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest.get_zk_lighter_info().await.map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_account", signature = (account_index, auth_token))]
    fn py_request_account<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        auth_token: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_detailed_account_by_index(account_index, &auth_token)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_account_api_keys", signature = (account_index, auth_token))]
    fn py_request_account_api_keys<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        auth_token: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_account_api_keys(account_index, &auth_token)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_account_limits", signature = (account_index, auth_token))]
    fn py_request_account_limits<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        auth_token: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_account_limits(account_index, &auth_token)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_account_metadata", signature = (account_index, auth_token))]
    fn py_request_account_metadata<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        auth_token: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_account_metadata_by_index(account_index, &auth_token)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_l1_metadata", signature = (l1_address, auth_token=None))]
    fn py_request_l1_metadata<'py>(
        &self,
        py: Python<'py>,
        l1_address: String,
        auth_token: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_l1_metadata(&l1_address, auth_token.as_deref())
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_sub_accounts", signature = (l1_address))]
    fn py_request_sub_accounts<'py>(
        &self,
        py: Python<'py>,
        l1_address: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_sub_accounts(&l1_address)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_public_pools_metadata", signature = (
        filter="all".to_string(),
        index=0,
        limit=100,
        account_index=None,
        auth_token=None,
    ))]
    fn py_request_public_pools_metadata<'py>(
        &self,
        py: Python<'py>,
        filter: String,
        index: i64,
        limit: i64,
        account_index: Option<i64>,
        auth_token: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_public_pools_metadata(
                    &filter,
                    index,
                    limit,
                    account_index,
                    auth_token.as_deref(),
                )
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_account_pnl", signature = (account_index, auth_token))]
    fn py_request_account_pnl<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        auth_token: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_account_pnl(account_index, &auth_token)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_liquidations", signature = (
        account_index,
        limit=100,
        market_id=None,
        cursor=None,
        auth_token=None,
    ))]
    fn py_request_liquidations<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        limit: i64,
        market_id: Option<i64>,
        cursor: Option<String>,
        auth_token: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_liquidations(
                    account_index,
                    limit,
                    market_id,
                    cursor.as_deref(),
                    auth_token.as_deref(),
                )
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_account_active_orders", signature = (account_index, market_id, auth_token))]
    fn py_request_account_active_orders<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        market_id: i64,
        auth_token: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_account_active_orders(account_index, market_id, &auth_token)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_account_inactive_orders", signature = (account_index, market_id, auth_token, cursor=None))]
    fn py_request_account_inactive_orders<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        market_id: i64,
        auth_token: String,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_account_inactive_orders(
                    account_index,
                    market_id,
                    &auth_token,
                    cursor.as_deref(),
                )
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_account_trades", signature = (account_index, auth_token, limit=500, cursor=None))]
    fn py_request_account_trades<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        auth_token: String,
        limit: u32,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_account_trades(account_index, &auth_token, limit, cursor.as_deref())
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_position_fundings", signature = (account_index, auth_token))]
    fn py_request_position_fundings<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        auth_token: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_position_fundings(account_index, &auth_token)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_deposit_history", signature = (account_index, auth_token, cursor=None))]
    fn py_request_deposit_history<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        auth_token: String,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_deposit_history(account_index, &auth_token, cursor.as_deref())
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_withdraw_history", signature = (account_index, auth_token, cursor=None))]
    fn py_request_withdraw_history<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        auth_token: String,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_withdraw_history(account_index, &auth_token, cursor.as_deref())
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_transfer_history", signature = (account_index, auth_token, cursor=None))]
    fn py_request_transfer_history<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        auth_token: String,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_transfer_history(account_index, &auth_token, cursor.as_deref())
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "change_account_tier", signature = (account_index, new_tier, auth_token))]
    fn py_change_account_tier<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        new_tier: String,
        auth_token: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .change_account_tier(account_index, &new_tier, &auth_token)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_next_nonce", signature = (account_index, api_key_index))]
    fn py_request_next_nonce<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        api_key_index: u8,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_next_nonce(account_index, api_key_index)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_enriched_tx", signature = (tx_hash))]
    fn py_request_enriched_tx<'py>(
        &self,
        py: Python<'py>,
        tx_hash: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_enriched_tx(&tx_hash)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_tx_from_l1_tx_hash", signature = (l1_tx_hash))]
    fn py_request_tx_from_l1_tx_hash<'py>(
        &self,
        py: Python<'py>,
        l1_tx_hash: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_tx_from_l1_tx_hash(&l1_tx_hash)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_txs", signature = (limit, index=None))]
    fn py_request_txs<'py>(
        &self,
        py: Python<'py>,
        limit: i64,
        index: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest.get_txs(limit, index).await.map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_export", signature = (
        export_type,
        auth_token=None,
        account_index=None,
        market_id=None,
        start_timestamp=None,
        end_timestamp=None,
        side=None,
        role=None,
        trade_type=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn py_request_export<'py>(
        &self,
        py: Python<'py>,
        export_type: String,
        auth_token: Option<String>,
        account_index: Option<i64>,
        market_id: Option<i64>,
        start_timestamp: Option<i64>,
        end_timestamp: Option<i64>,
        side: Option<String>,
        role: Option<String>,
        trade_type: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_export(
                    &export_type,
                    auth_token.as_deref(),
                    account_index,
                    market_id,
                    start_timestamp,
                    end_timestamp,
                    side.as_deref(),
                    role.as_deref(),
                    trade_type.as_deref(),
                )
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_transfer_fee_info", signature = (
        account_index,
        to_account_index=None,
        auth_token=None,
    ))]
    fn py_request_transfer_fee_info<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        to_account_index: Option<i64>,
        auth_token: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_transfer_fee_info(account_index, to_account_index, auth_token.as_deref())
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_withdrawal_delay", signature = ())]
    fn py_request_withdrawal_delay<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest.get_withdrawal_delay().await.map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "create_intent_address", signature = (
        chain_id,
        from_addr,
        amount,
        is_external_deposit=false,
    ))]
    fn py_create_intent_address<'py>(
        &self,
        py: Python<'py>,
        chain_id: String,
        from_addr: String,
        amount: String,
        is_external_deposit: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .create_intent_address(&chain_id, &from_addr, &amount, is_external_deposit)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_fast_bridge_info", signature = ())]
    fn py_request_fast_bridge_info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest.get_fast_bridge_info().await.map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_deposit_latest", signature = (l1_address))]
    fn py_request_deposit_latest<'py>(
        &self,
        py: Python<'py>,
        l1_address: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_deposit_latest(&l1_address)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_deposit_networks", signature = ())]
    fn py_request_deposit_networks<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest.get_deposit_networks().await.map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_fast_withdraw_info", signature = (account_index, auth_token))]
    fn py_request_fast_withdraw_info<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        auth_token: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_fast_withdraw_info(account_index, &auth_token)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_lease_options", signature = ())]
    fn py_request_lease_options<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest.get_lease_options().await.map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_leases", signature = (
        account_index,
        auth_token,
        cursor=None,
        limit=None,
    ))]
    fn py_request_leases<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        auth_token: String,
        cursor: Option<String>,
        limit: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_leases(account_index, &auth_token, cursor.as_deref(), limit)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_api_tokens", signature = (account_index, auth_token))]
    fn py_request_api_tokens<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        auth_token: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_tokens(account_index, &auth_token)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_user_referrals", signature = (
        l1_address,
        cursor=None,
        auth_token=None,
    ))]
    fn py_request_user_referrals<'py>(
        &self,
        py: Python<'py>,
        l1_address: String,
        cursor: Option<i64>,
        auth_token: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_user_referrals(&l1_address, cursor, auth_token.as_deref())
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "request_referral_code", signature = (account_index, auth_token=None))]
    fn py_request_referral_code<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        auth_token: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .get_referral_code(account_index, auth_token.as_deref())
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "create_referral_code", signature = (account_index, auth_token=None))]
    fn py_create_referral_code<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        auth_token: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .create_referral_code(account_index, auth_token.as_deref())
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "update_referral_code", signature = (
        account_index,
        new_referral_code,
        auth_token=None,
    ))]
    fn py_update_referral_code<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        new_referral_code: String,
        auth_token: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .update_referral_code(account_index, &new_referral_code, auth_token.as_deref())
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "update_referral_kickback", signature = (
        account_index,
        kickback_percentage,
        auth_token=None,
    ))]
    fn py_update_referral_kickback<'py>(
        &self,
        py: Python<'py>,
        account_index: i64,
        kickback_percentage: f64,
        auth_token: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .update_referral_kickback(account_index, kickback_percentage, auth_token.as_deref())
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "use_referral_code", signature = (
        l1_address,
        referral_code,
        x,
        discord=None,
        telegram=None,
        signature=None,
        auth_token=None,
    ))]
    fn py_use_referral_code<'py>(
        &self,
        py: Python<'py>,
        l1_address: String,
        referral_code: String,
        x: String,
        discord: Option<String>,
        telegram: Option<String>,
        signature: Option<String>,
        auth_token: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .use_referral_code(
                    &l1_address,
                    &referral_code,
                    &x,
                    discord.as_deref(),
                    telegram.as_deref(),
                    signature.as_deref(),
                    auth_token.as_deref(),
                )
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "create_api_token", signature = (
        name,
        account_index,
        expiry,
        sub_account_access,
        auth_token,
        scopes="read.*".to_string(),
    ))]
    fn py_create_api_token<'py>(
        &self,
        py: Python<'py>,
        name: String,
        account_index: i64,
        expiry: i64,
        sub_account_access: bool,
        auth_token: String,
        scopes: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .create_token(
                    &name,
                    account_index,
                    expiry,
                    sub_account_access,
                    &scopes,
                    &auth_token,
                )
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "revoke_api_token", signature = (token_id, account_index, auth_token))]
    fn py_revoke_api_token<'py>(
        &self,
        py: Python<'py>,
        token_id: i64,
        account_index: i64,
        auth_token: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .revoke_token(token_id, account_index, &auth_token)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "acknowledge_notification", signature = (notif_id, account_index, auth_token))]
    fn py_acknowledge_notification<'py>(
        &self,
        py: Python<'py>,
        notif_id: String,
        account_index: i64,
        auth_token: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .ack_notification(&notif_id, account_index, &auth_token)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "create_auth_token", signature = (deadline_secs, api_key_index=None))]
    fn py_create_auth_token<'py>(
        &self,
        py: Python<'py>,
        deadline_secs: i64,
        api_key_index: Option<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .create_auth_token(deadline_secs, api_key_index)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "submit_order", signature = (
        market_index,
        client_order_index,
        base_amount,
        price,
        is_ask,
        order_type,
        time_in_force,
        reduce_only=false,
        trigger_price=0,
        order_expiry=0,
        api_key_index=None,
        nonce=None,
    ))]
    fn py_submit_order<'py>(
        &self,
        py: Python<'py>,
        market_index: i32,
        client_order_index: i64,
        base_amount: i64,
        price: i32,
        is_ask: bool,
        order_type: i32,
        time_in_force: i32,
        reduce_only: bool,
        trigger_price: i32,
        order_expiry: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .submit_order(
                    market_index,
                    client_order_index,
                    base_amount,
                    price,
                    is_ask,
                    order_type,
                    time_in_force,
                    reduce_only,
                    trigger_price,
                    order_expiry,
                    api_key_index,
                    nonce,
                )
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "submit_order_batch", signature = (requests_json))]
    fn py_submit_order_batch<'py>(
        &self,
        py: Python<'py>,
        requests_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let requests: Vec<LighterSubmitOrderRequest> =
            serde_json::from_str(requests_json).map_err(to_pyvalue_err)?;
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .submit_order_batch(requests)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "modify_order", signature = (
        market_index,
        order_index,
        base_amount,
        price,
        trigger_price=0,
        api_key_index=None,
        nonce=None,
    ))]
    fn py_modify_order<'py>(
        &self,
        py: Python<'py>,
        market_index: i32,
        order_index: i64,
        base_amount: i64,
        price: i64,
        trigger_price: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .modify_order(
                    market_index,
                    order_index,
                    base_amount,
                    price,
                    trigger_price,
                    api_key_index,
                    nonce,
                )
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "cancel_order", signature = (market_index, order_index, api_key_index=None, nonce=None))]
    fn py_cancel_order<'py>(
        &self,
        py: Python<'py>,
        market_index: i32,
        order_index: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .cancel_order(market_index, order_index, api_key_index, nonce)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "cancel_order_batch", signature = (requests_json))]
    fn py_cancel_order_batch<'py>(
        &self,
        py: Python<'py>,
        requests_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let requests: Vec<LighterCancelOrderRequest> =
            serde_json::from_str(requests_json).map_err(to_pyvalue_err)?;
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .cancel_order_batch(requests)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "cancel_all_orders", signature = (time_in_force, timestamp_ms, api_key_index=None, nonce=None))]
    fn py_cancel_all_orders<'py>(
        &self,
        py: Python<'py>,
        time_in_force: i32,
        timestamp_ms: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .cancel_all_orders(time_in_force, timestamp_ms, api_key_index, nonce)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "update_leverage", signature = (
        market_index,
        initial_margin_fraction,
        margin_mode,
        api_key_index=None,
        nonce=None,
    ))]
    fn py_update_leverage<'py>(
        &self,
        py: Python<'py>,
        market_index: i32,
        initial_margin_fraction: i32,
        margin_mode: i32,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .update_leverage(
                    market_index,
                    initial_margin_fraction,
                    margin_mode,
                    api_key_index,
                    nonce,
                )
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "update_margin", signature = (
        market_index,
        usdc_amount,
        direction,
        api_key_index=None,
        nonce=None,
    ))]
    fn py_update_margin<'py>(
        &self,
        py: Python<'py>,
        market_index: i32,
        usdc_amount: i64,
        direction: i32,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .update_margin(market_index, usdc_amount, direction, api_key_index, nonce)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "withdraw", signature = (
        asset_index,
        route_type,
        amount,
        api_key_index=None,
        nonce=None,
    ))]
    fn py_withdraw<'py>(
        &self,
        py: Python<'py>,
        asset_index: i32,
        route_type: i32,
        amount: u64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .withdraw(asset_index, route_type, amount, api_key_index, nonce)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "transfer", signature = (
        to_account_index,
        asset_index,
        from_route_type,
        to_route_type,
        amount,
        usdc_fee=0,
        memo=String::new(),
        api_key_index=None,
        nonce=None,
    ))]
    fn py_transfer<'py>(
        &self,
        py: Python<'py>,
        to_account_index: i64,
        asset_index: i16,
        from_route_type: u8,
        to_route_type: u8,
        amount: i64,
        usdc_fee: i64,
        memo: String,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .transfer(
                    to_account_index,
                    asset_index,
                    from_route_type,
                    to_route_type,
                    amount,
                    usdc_fee,
                    memo,
                    api_key_index,
                    nonce,
                )
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "change_pub_key", signature = (
        new_pub_key,
        api_key_index=None,
        nonce=None,
    ))]
    fn py_change_pub_key<'py>(
        &self,
        py: Python<'py>,
        new_pub_key: String,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .change_pub_key(&new_pub_key, api_key_index, nonce)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "create_sub_account", signature = (api_key_index=None, nonce=None))]
    fn py_create_sub_account<'py>(
        &self,
        py: Python<'py>,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .create_sub_account(api_key_index, nonce)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "create_public_pool", signature = (
        operator_fee,
        initial_total_shares,
        min_operator_share_rate,
        api_key_index=None,
        nonce=None,
    ))]
    fn py_create_public_pool<'py>(
        &self,
        py: Python<'py>,
        operator_fee: i64,
        initial_total_shares: i32,
        min_operator_share_rate: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .create_public_pool(
                    operator_fee,
                    initial_total_shares,
                    min_operator_share_rate,
                    api_key_index,
                    nonce,
                )
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "update_public_pool", signature = (
        public_pool_index,
        status,
        operator_fee,
        min_operator_share_rate,
        api_key_index=None,
        nonce=None,
    ))]
    fn py_update_public_pool<'py>(
        &self,
        py: Python<'py>,
        public_pool_index: i64,
        status: i32,
        operator_fee: i64,
        min_operator_share_rate: i32,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .update_public_pool(
                    public_pool_index,
                    status,
                    operator_fee,
                    min_operator_share_rate,
                    api_key_index,
                    nonce,
                )
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "mint_pool_shares", signature = (
        public_pool_index,
        share_amount,
        api_key_index=None,
        nonce=None,
    ))]
    fn py_mint_pool_shares<'py>(
        &self,
        py: Python<'py>,
        public_pool_index: i64,
        share_amount: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .mint_shares(public_pool_index, share_amount, api_key_index, nonce)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "burn_pool_shares", signature = (
        public_pool_index,
        share_amount,
        api_key_index=None,
        nonce=None,
    ))]
    fn py_burn_pool_shares<'py>(
        &self,
        py: Python<'py>,
        public_pool_index: i64,
        share_amount: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .burn_shares(public_pool_index, share_amount, api_key_index, nonce)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "fast_withdraw", signature = (tx_info, to_address, auth_token))]
    fn py_fast_withdraw<'py>(
        &self,
        py: Python<'py>,
        tx_info: String,
        to_address: String,
        auth_token: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .fast_withdraw(&tx_info, &to_address, &auth_token)
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }

    #[pyo3(name = "lit_lease", signature = (
        tx_info,
        auth_token,
        lease_amount=None,
        duration_days=None,
    ))]
    fn py_lit_lease<'py>(
        &self,
        py: Python<'py>,
        tx_info: String,
        auth_token: String,
        lease_amount: Option<String>,
        duration_days: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rest = self.rest().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = rest
                .lit_lease(
                    &tx_info,
                    lease_amount.as_deref(),
                    duration_days,
                    &auth_token,
                )
                .await
                .map_err(to_pyvalue_err)?;
            serde_json::to_string(&response).map_err(to_pyvalue_err)
        })
    }
}
