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

use nautilus_core::python::to_pyvalue_err;
use pyo3::prelude::*;

use crate::{config::variational_http_base_url, http::client::VariationalHttpClient};

#[pymethods]
impl VariationalHttpClient {
    #[new]
    #[pyo3(signature = (base_url_http = None, proxy_url = None, timeout_secs = None))]
    fn py_new(
        base_url_http: Option<String>,
        proxy_url: Option<String>,
        timeout_secs: Option<u64>,
    ) -> PyResult<Self> {
        Self::new(
            base_url_http.or_else(|| Some(variational_http_base_url())),
            proxy_url,
            timeout_secs.unwrap_or(30),
        )
        .map_err(to_pyvalue_err)
    }

    #[getter]
    #[pyo3(name = "base_url_http")]
    fn py_base_url_http(&self) -> String {
        self.base_url().to_string()
    }

    #[pyo3(name = "request_stats")]
    fn py_request_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.stats_json().await.map_err(to_pyvalue_err)
        })
    }
}
