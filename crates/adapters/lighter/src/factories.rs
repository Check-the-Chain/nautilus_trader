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

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
use nautilus_common::{
    cache::Cache,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{AccountId, ClientId, TraderId},
};

use crate::{
    config::{LighterDataClientConfig, LighterExecClientConfig},
    data::LighterDataClient,
    execution::LighterExecutionClient,
};

impl ClientConfig for LighterDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ClientConfig for LighterExecClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.lighter", from_py_object)
)]
pub struct LighterDataClientFactory;

impl LighterDataClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for LighterDataClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl DataClientFactory for LighterDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: Rc<RefCell<Cache>>,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<LighterDataClientConfig>()
            .ok_or_else(|| anyhow::anyhow!("Invalid config type for LighterDataClientFactory"))?;
        Ok(Box::new(LighterDataClient::new(
            ClientId::from(name),
            config,
        )?))
    }

    fn name(&self) -> &'static str {
        "LIGHTER"
    }

    fn config_type(&self) -> &'static str {
        "LighterDataClientConfig"
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.lighter", from_py_object)
)]
pub struct LighterExecFactoryConfig {
    pub trader_id: TraderId,
    pub account_id: AccountId,
    pub config: LighterExecClientConfig,
}

impl ClientConfig for LighterExecFactoryConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.lighter", from_py_object)
)]
pub struct LighterExecutionClientFactory;

impl LighterExecutionClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for LighterExecutionClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionClientFactory for LighterExecutionClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: Rc<RefCell<Cache>>,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let config = config
            .as_any()
            .downcast_ref::<LighterExecFactoryConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!("Invalid config type for LighterExecutionClientFactory")
            })?
            .clone();

        let core = ExecutionClientCore::new(
            config.trader_id,
            ClientId::from(name),
            crate::common::venue(),
            OmsType::Netting,
            config.account_id,
            AccountType::Margin,
            None,
            cache,
        );

        Ok(Box::new(LighterExecutionClient::new(core, config.config)?))
    }

    fn name(&self) -> &'static str {
        "LIGHTER"
    }

    fn config_type(&self) -> &'static str {
        "LighterExecFactoryConfig"
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
    use nautilus_common::{cache::Cache, clock::TestClock};
    use nautilus_model::identifiers::{AccountId, TraderId};
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_lighter_data_client_factory_creation() {
        let factory = LighterDataClientFactory::new();
        assert_eq!(factory.name(), "LIGHTER");
        assert_eq!(factory.config_type(), "LighterDataClientConfig");
    }

    #[rstest]
    fn test_lighter_data_client_factory_default() {
        let factory = LighterDataClientFactory;
        assert_eq!(factory.name(), "LIGHTER");
    }

    #[rstest]
    fn test_lighter_execution_client_factory_creation() {
        let factory = LighterExecutionClientFactory::new();
        assert_eq!(factory.name(), "LIGHTER");
        assert_eq!(factory.config_type(), "LighterExecFactoryConfig");
    }

    #[rstest]
    fn test_lighter_execution_client_factory_default() {
        let factory = LighterExecutionClientFactory;
        assert_eq!(factory.name(), "LIGHTER");
    }

    #[rstest]
    fn test_lighter_data_client_config_implements_client_config() {
        let config = LighterDataClientConfig::default();
        let boxed_config: Box<dyn ClientConfig> = Box::new(config);
        let downcasted = boxed_config
            .as_any()
            .downcast_ref::<LighterDataClientConfig>();

        assert!(downcasted.is_some());
    }

    #[rstest]
    fn test_lighter_exec_factory_config_implements_client_config() {
        let config = LighterExecFactoryConfig {
            trader_id: TraderId::from("TRADER-001"),
            account_id: AccountId::from("LIGHTER-001"),
            config: LighterExecClientConfig::default(),
        };

        let boxed_config: Box<dyn ClientConfig> = Box::new(config);
        let downcasted = boxed_config
            .as_any()
            .downcast_ref::<LighterExecFactoryConfig>();

        assert!(downcasted.is_some());
    }

    #[rstest]
    fn test_lighter_data_client_factory_rejects_wrong_config_type() {
        let factory = LighterDataClientFactory::new();
        let wrong_config = LighterExecFactoryConfig {
            trader_id: TraderId::from("TRADER-001"),
            account_id: AccountId::from("LIGHTER-001"),
            config: LighterExecClientConfig::default(),
        };

        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(TestClock::new()));

        let result = factory.create("LIGHTER-TEST", &wrong_config, cache, clock);

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
    fn test_lighter_execution_client_factory_rejects_wrong_config_type() {
        let factory = LighterExecutionClientFactory::new();
        let wrong_config = LighterDataClientConfig::default();

        let cache = Rc::new(RefCell::new(Cache::default()));

        let result = factory.create("LIGHTER-TEST", &wrong_config, cache);

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
