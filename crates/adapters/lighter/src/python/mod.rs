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

pub mod factories;
pub mod http;
pub mod urls;
pub mod websocket;

use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::{
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
    get_global_pyo3_registry,
};
use pyo3::prelude::*;

use crate::{
    config::{LighterDataClientConfig, LighterExecClientConfig},
    factories::{
        LighterDataClientFactory, LighterExecFactoryConfig, LighterExecutionClientFactory,
    },
};

fn extract_lighter_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<LighterDataClientFactory>(py) {
        Ok(factory) => Ok(Box::new(factory)),
        Err(error) => Err(to_pyvalue_err(format!(
            "Failed to extract LighterDataClientFactory: {error}"
        ))),
    }
}

fn extract_lighter_exec_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    match factory.extract::<LighterExecutionClientFactory>(py) {
        Ok(factory) => Ok(Box::new(factory)),
        Err(error) => Err(to_pyvalue_err(format!(
            "Failed to extract LighterExecutionClientFactory: {error}"
        ))),
    }
}

fn extract_lighter_data_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<LighterDataClientConfig>(py) {
        Ok(config) => Ok(Box::new(config)),
        Err(error) => Err(to_pyvalue_err(format!(
            "Failed to extract LighterDataClientConfig: {error}"
        ))),
    }
}

fn extract_lighter_exec_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<LighterExecFactoryConfig>(py) {
        Ok(config) => Ok(Box::new(config)),
        Err(error) => Err(to_pyvalue_err(format!(
            "Failed to extract LighterExecFactoryConfig: {error}"
        ))),
    }
}

#[pymodule]
pub fn lighter(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<crate::http::client::LighterHttpClient>()?;
    m.add_class::<crate::websocket::client::LighterWebSocketClient>()?;
    m.add_class::<LighterDataClientConfig>()?;
    m.add_class::<LighterExecClientConfig>()?;
    m.add_class::<LighterExecFactoryConfig>()?;
    m.add_class::<LighterDataClientFactory>()?;
    m.add_class::<LighterExecutionClientFactory>()?;
    m.add_function(wrap_pyfunction!(urls::py_get_lighter_http_base_url, m)?)?;
    m.add_function(wrap_pyfunction!(urls::py_get_lighter_ws_base_url, m)?)?;

    let registry = get_global_pyo3_registry();
    if let Err(error) =
        registry.register_factory_extractor("LIGHTER".to_string(), extract_lighter_data_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Lighter data factory extractor: {error}"
        )));
    }
    if let Err(error) = registry
        .register_exec_factory_extractor("LIGHTER".to_string(), extract_lighter_exec_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Lighter exec factory extractor: {error}"
        )));
    }
    if let Err(error) = registry.register_config_extractor(
        "LighterDataClientConfig".to_string(),
        extract_lighter_data_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Lighter data config extractor: {error}"
        )));
    }
    if let Err(error) = registry.register_config_extractor(
        "LighterExecFactoryConfig".to_string(),
        extract_lighter_exec_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Lighter exec config extractor: {error}"
        )));
    }

    Ok(())
}
