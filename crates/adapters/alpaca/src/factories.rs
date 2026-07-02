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

//! Factory functions for creating Alpaca clients and components.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::ClientId,
};

use crate::{
    common::consts::{ALPACA, ALPACA_VENUE},
    config::{AlpacaDataClientConfig, AlpacaExecClientConfig},
    data::AlpacaDataClient,
    execution::AlpacaExecutionClient,
};

impl ClientConfig for AlpacaDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ClientConfig for AlpacaExecClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating Alpaca data clients.
#[derive(Debug, Clone, Default)]
pub struct AlpacaDataClientFactory;

impl AlpacaDataClientFactory {
    /// Creates a new [`AlpacaDataClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl DataClientFactory for AlpacaDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let alpaca_config = config
            .as_any()
            .downcast_ref::<AlpacaDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for AlpacaDataClientFactory. Expected AlpacaDataClientConfig, was {config:?}",
                )
            })?
            .clone();

        let client_id = ClientId::from(name);
        let client = AlpacaDataClient::new(client_id, alpaca_config)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        ALPACA
    }

    fn config_type(&self) -> &'static str {
        "AlpacaDataClientConfig"
    }
}

/// Factory for creating Alpaca execution clients.
#[derive(Debug, Clone, Default)]
pub struct AlpacaExecutionClientFactory;

impl AlpacaExecutionClientFactory {
    /// Creates a new [`AlpacaExecutionClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl ExecutionClientFactory for AlpacaExecutionClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let alpaca_config = config
            .as_any()
            .downcast_ref::<AlpacaExecClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for AlpacaExecutionClientFactory. Expected AlpacaExecClientConfig, was {config:?}",
                )
            })?
            .clone();

        // Alpaca accounts are USD margin accounts with net positions per
        // symbol.
        let core = ExecutionClientCore::new(
            alpaca_config.trader_id,
            ClientId::from(name),
            *ALPACA_VENUE,
            OmsType::Netting,
            alpaca_config.account_id,
            AccountType::Margin,
            None,
            cache,
        );

        let client = AlpacaExecutionClient::new(core, alpaca_config)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        ALPACA
    }

    fn config_type(&self) -> &'static str {
        "AlpacaExecClientConfig"
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        clock::TestClock,
        factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
    };
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_alpaca_data_client_factory_creation() {
        let factory = AlpacaDataClientFactory::new();
        assert_eq!(factory.name(), ALPACA);
        assert_eq!(factory.config_type(), "AlpacaDataClientConfig");
    }

    #[rstest]
    fn test_alpaca_execution_client_factory_creation() {
        let factory = AlpacaExecutionClientFactory::new();
        assert_eq!(factory.name(), ALPACA);
        assert_eq!(factory.config_type(), "AlpacaExecClientConfig");
    }

    #[rstest]
    fn test_alpaca_exec_client_config_implements_client_config() {
        let config = AlpacaExecClientConfig::default();
        let boxed_config: Box<dyn ClientConfig> = Box::new(config);
        let downcasted = boxed_config
            .as_any()
            .downcast_ref::<AlpacaExecClientConfig>();

        assert!(downcasted.is_some());
    }

    #[rstest]
    fn test_alpaca_data_client_factory_rejects_wrong_config_type() {
        let factory = AlpacaDataClientFactory::new();
        let wrong_config = AlpacaExecClientConfig::default();
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(TestClock::new()));

        let result = factory.create("ALPACA-TEST", &wrong_config, cache.into(), clock);
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("Invalid config type")
        );
    }

    #[rstest]
    fn test_alpaca_execution_client_factory_rejects_wrong_config_type() {
        let factory = AlpacaExecutionClientFactory::new();
        let wrong_config = AlpacaDataClientConfig::default();
        let cache = Rc::new(RefCell::new(Cache::default()));

        let result = factory.create("ALPACA-TEST", &wrong_config, cache.into());
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("Invalid config type")
        );
    }
}
