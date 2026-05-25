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

//! Factory functions for creating Variational clients and components.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::Cache,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{config::VariationalDataClientConfig, data::VariationalDataClient};

impl ClientConfig for VariationalDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating Variational data clients.
#[derive(Debug, Clone)]
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
pub struct VariationalDataClientFactory;

impl VariationalDataClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for VariationalDataClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl DataClientFactory for VariationalDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: Rc<RefCell<Cache>>,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<VariationalDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for VariationalDataClientFactory. Expected VariationalDataClientConfig, was {config:?}",
                )
            })?
            .clone();

        Ok(Box::new(VariationalDataClient::new(
            ClientId::from(name),
            config,
        )?))
    }

    fn name(&self) -> &'static str {
        "VARIATIONAL"
    }

    fn config_type(&self) -> &'static str {
        "VariationalDataClientConfig"
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::factories::{ClientConfig, DataClientFactory};
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_variational_data_client_factory_creation() {
        let factory = VariationalDataClientFactory::new();
        assert_eq!(factory.name(), "VARIATIONAL");
        assert_eq!(factory.config_type(), "VariationalDataClientConfig");
    }

    #[rstest]
    fn test_variational_data_client_config_implements_client_config() {
        let config = VariationalDataClientConfig::default();
        let boxed_config: Box<dyn ClientConfig> = Box::new(config);
        assert!(
            boxed_config
                .as_any()
                .downcast_ref::<VariationalDataClientConfig>()
                .is_some()
        );
    }
}
