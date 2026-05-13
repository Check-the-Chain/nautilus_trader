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

use nautilus_common::live::get_runtime;
use nautilus_core::python::{call_python_threadsafe, to_pyruntime_err};
use pyo3::{conversion::IntoPyObjectExt, prelude::*};

use crate::{config::lighter_ws_base_url, websocket::client::LighterWebSocketClient};

#[pymethods]
impl LighterWebSocketClient {
    #[new]
    #[pyo3(signature = (url=None, testnet=false, auth_token=None))]
    fn py_new(url: Option<String>, testnet: bool, auth_token: Option<String>) -> Self {
        let url = url.unwrap_or_else(|| lighter_ws_base_url(testnet));
        Self::new(url, auth_token)
    }

    #[getter]
    #[pyo3(name = "url")]
    fn py_url(&self) -> String {
        self.url().to_string()
    }

    #[pyo3(name = "is_active")]
    fn py_is_active(&self) -> bool {
        self.is_active()
    }

    #[pyo3(name = "is_closed")]
    fn py_is_closed(&self) -> bool {
        !self.is_active()
    }

    #[pyo3(name = "set_auth_token", signature = (auth_token=None))]
    fn py_set_auth_token<'py>(
        &self,
        py: Python<'py>,
        auth_token: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.set_auth_token(auth_token).await;
            Ok(())
        })
    }

    #[pyo3(name = "connect")]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "PyO3 boundary extracts owned Python handles"
    )]
    fn py_connect<'py>(
        &self,
        py: Python<'py>,
        loop_: Py<PyAny>,
        callback: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let call_soon: Py<PyAny> = loop_.getattr(py, "call_soon_threadsafe")?;
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.connect().await.map_err(to_pyruntime_err)?;

            get_runtime().spawn(async move {
                loop {
                    let message = client.next_message().await;
                    let Some(message) = message else {
                        break;
                    };

                    Python::attach(|py| {
                        if let Ok(py_obj) = message.into_py_any(py) {
                            call_python_threadsafe(py, &call_soon, &callback, py_obj);
                        }
                    });
                }
            });

            Ok(())
        })
    }

    #[pyo3(name = "close")]
    fn py_close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.close().await.map_err(to_pyruntime_err)
        })
    }

    #[pyo3(name = "subscribe", signature = (channel, auth_token=None))]
    fn py_subscribe<'py>(
        &self,
        py: Python<'py>,
        channel: String,
        auth_token: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .subscribe(channel, auth_token)
                .await
                .map_err(to_pyruntime_err)
        })
    }

    #[pyo3(name = "unsubscribe")]
    fn py_unsubscribe<'py>(&self, py: Python<'py>, channel: String) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.unsubscribe(channel).await.map_err(to_pyruntime_err)
        })
    }

    #[pyo3(name = "subscribe_book")]
    fn py_subscribe_book<'py>(
        &self,
        py: Python<'py>,
        market_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_subscribe(py, format!("order_book/{market_id}"), None)
    }

    #[pyo3(name = "unsubscribe_book")]
    fn py_unsubscribe_book<'py>(
        &self,
        py: Python<'py>,
        market_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_unsubscribe(py, format!("order_book/{market_id}"))
    }

    #[pyo3(name = "subscribe_quotes")]
    fn py_subscribe_quotes<'py>(
        &self,
        py: Python<'py>,
        market_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_subscribe(py, format!("ticker/{market_id}"), None)
    }

    #[pyo3(name = "unsubscribe_quotes")]
    fn py_unsubscribe_quotes<'py>(
        &self,
        py: Python<'py>,
        market_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_unsubscribe(py, format!("ticker/{market_id}"))
    }

    #[pyo3(name = "subscribe_trades")]
    fn py_subscribe_trades<'py>(
        &self,
        py: Python<'py>,
        market_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_subscribe(py, format!("trade/{market_id}"), None)
    }

    #[pyo3(name = "unsubscribe_trades")]
    fn py_unsubscribe_trades<'py>(
        &self,
        py: Python<'py>,
        market_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_unsubscribe(py, format!("trade/{market_id}"))
    }

    #[pyo3(name = "subscribe_market_stats")]
    fn py_subscribe_market_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.py_subscribe(py, "market_stats/all".to_string(), None)
    }

    #[pyo3(name = "unsubscribe_market_stats")]
    fn py_unsubscribe_market_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.py_unsubscribe(py, "market_stats/all".to_string())
    }

    #[pyo3(name = "subscribe_spot_market_stats")]
    fn py_subscribe_spot_market_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.py_subscribe(py, "spot_market_stats/all".to_string(), None)
    }

    #[pyo3(name = "unsubscribe_spot_market_stats")]
    fn py_unsubscribe_spot_market_stats<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_unsubscribe(py, "spot_market_stats/all".to_string())
    }

    #[pyo3(name = "subscribe_account_all")]
    fn py_subscribe_account_all<'py>(
        &self,
        py: Python<'py>,
        account_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_subscribe(py, format!("account_all/{account_id}"), None)
    }

    #[pyo3(name = "subscribe_account_all_positions")]
    fn py_subscribe_account_all_positions<'py>(
        &self,
        py: Python<'py>,
        account_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_subscribe(py, format!("account_all_positions/{account_id}"), None)
    }

    #[pyo3(name = "subscribe_account_all_orders")]
    fn py_subscribe_account_all_orders<'py>(
        &self,
        py: Python<'py>,
        account_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_subscribe(py, format!("account_all_orders/{account_id}"), None)
    }

    #[pyo3(name = "subscribe_account_all_trades")]
    fn py_subscribe_account_all_trades<'py>(
        &self,
        py: Python<'py>,
        account_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_subscribe(py, format!("account_all_trades/{account_id}"), None)
    }

    #[pyo3(name = "subscribe_account_all_assets")]
    fn py_subscribe_account_all_assets<'py>(
        &self,
        py: Python<'py>,
        account_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_subscribe(py, format!("account_all_assets/{account_id}"), None)
    }

    #[pyo3(name = "subscribe_user_stats")]
    fn py_subscribe_user_stats<'py>(
        &self,
        py: Python<'py>,
        account_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_subscribe(py, format!("user_stats/{account_id}"), None)
    }

    #[pyo3(name = "subscribe_account_market")]
    fn py_subscribe_account_market<'py>(
        &self,
        py: Python<'py>,
        account_id: i64,
        market_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_subscribe(py, format!("account_market/{account_id}/{market_id}"), None)
    }

    #[pyo3(name = "subscribe_avg_entry_prices")]
    fn py_subscribe_avg_entry_prices<'py>(
        &self,
        py: Python<'py>,
        account_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_subscribe(py, format!("avg_entry_prices/{account_id}"), None)
    }

    #[pyo3(name = "subscribe_notifications")]
    fn py_subscribe_notifications<'py>(
        &self,
        py: Python<'py>,
        account_id: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.py_subscribe(py, format!("notification/{account_id}"), None)
    }
}
