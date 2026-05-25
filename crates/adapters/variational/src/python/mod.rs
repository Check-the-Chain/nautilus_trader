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

//! Python bindings from `pyo3`.

#![expect(
    clippy::missing_errors_doc,
    reason = "errors documented on underlying Rust methods"
)]

pub mod config;
pub mod http;
pub mod urls;

use nautilus_common::factories::{ClientConfig, DataClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    config::{VariationalDataClientConfig, VariationalQuoteTier},
    factories::VariationalDataClientFactory,
    http::client::VariationalHttpClient,
};

#[expect(
    clippy::needless_pass_by_value,
    reason = "registry extractor receives owned Python objects"
)]
fn extract_variational_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<VariationalDataClientFactory>(py) {
        Ok(factory) => Ok(Box::new(factory)),
        Err(error) => Err(to_pyvalue_err(format!(
            "Failed to extract VariationalDataClientFactory: {error}"
        ))),
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "registry extractor receives owned Python objects"
)]
fn extract_variational_data_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<VariationalDataClientConfig>(py) {
        Ok(config) => Ok(Box::new(config)),
        Err(error) => Err(to_pyvalue_err(format!(
            "Failed to extract VariationalDataClientConfig: {error}"
        ))),
    }
}

#[pymodule]
pub fn variational(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<VariationalHttpClient>()?;
    m.add_class::<VariationalQuoteTier>()?;
    m.add_class::<VariationalDataClientConfig>()?;
    m.add_class::<VariationalDataClientFactory>()?;
    m.add_function(wrap_pyfunction!(urls::py_get_variational_http_base_url, m)?)?;
    m.add_function(wrap_pyfunction!(urls::py_get_variational_ws_base_url, m)?)?;

    let registry = get_global_pyo3_registry();
    if let Err(error) = registry
        .register_factory_extractor("VARIATIONAL".to_string(), extract_variational_data_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Variational data factory extractor: {error}"
        )));
    }
    if let Err(error) = registry.register_config_extractor(
        "VariationalDataClientConfig".to_string(),
        extract_variational_data_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Variational data config extractor: {error}"
        )));
    }

    Ok(())
}
