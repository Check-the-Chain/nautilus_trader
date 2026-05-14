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

//! Factory functions for creating Interactive Brokers clients and components.

use std::{any::Any, cell::RefCell, rc::Rc, sync::Arc};

use nautilus_common::{
    cache::Cache,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{AccountId, ClientId, TraderId},
};

use crate::{
    common::consts::IB_VENUE,
    config::{
        InteractiveBrokersDataClientConfig, InteractiveBrokersExecClientConfig,
        InteractiveBrokersInstrumentProviderConfig,
    },
    data::InteractiveBrokersDataClient,
    execution::InteractiveBrokersExecutionClient,
    providers::instruments::InteractiveBrokersInstrumentProvider,
};

/// Configuration for creating Interactive Brokers data clients via factory.
///
/// The data client requires both connection settings and an instrument provider. Keeping the
/// provider config with the client config lets Rust `LiveNode` launches mirror the Python adapter
/// startup path where instruments can be loaded before subscriptions are issued.
#[derive(Clone, Debug)]
pub struct InteractiveBrokersDataFactoryConfig {
    /// The underlying data client configuration.
    pub config: InteractiveBrokersDataClientConfig,
    /// Instrument provider configuration used by the data client.
    pub instrument_provider: InteractiveBrokersInstrumentProviderConfig,
}

impl ClientConfig for InteractiveBrokersDataFactoryConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Configuration for creating Interactive Brokers execution clients via factory.
#[derive(Clone, Debug)]
pub struct InteractiveBrokersExecFactoryConfig {
    /// The trader ID for the execution client.
    pub trader_id: TraderId,
    /// The underlying execution client configuration.
    pub config: InteractiveBrokersExecClientConfig,
    /// Instrument provider configuration used by the execution client.
    pub instrument_provider: InteractiveBrokersInstrumentProviderConfig,
}

impl ClientConfig for InteractiveBrokersExecFactoryConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating Interactive Brokers data clients.
#[derive(Debug, Clone)]
pub struct InteractiveBrokersDataClientFactory;

impl InteractiveBrokersDataClientFactory {
    /// Creates a new [`InteractiveBrokersDataClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for InteractiveBrokersDataClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl DataClientFactory for InteractiveBrokersDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: Rc<RefCell<Cache>>,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let factory_config = config
            .as_any()
            .downcast_ref::<InteractiveBrokersDataFactoryConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for InteractiveBrokersDataClientFactory. Expected InteractiveBrokersDataFactoryConfig, was {config:?}",
                )
            })?
            .clone();

        let instrument_provider = Arc::new(InteractiveBrokersInstrumentProvider::new(
            factory_config.instrument_provider,
        ));
        let client = InteractiveBrokersDataClient::new(
            ClientId::from(name),
            factory_config.config,
            instrument_provider,
        )?;

        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        "IB"
    }

    fn config_type(&self) -> &'static str {
        "InteractiveBrokersDataFactoryConfig"
    }
}

/// Factory for creating Interactive Brokers execution clients.
#[derive(Debug, Clone)]
pub struct InteractiveBrokersExecutionClientFactory;

impl InteractiveBrokersExecutionClientFactory {
    /// Creates a new [`InteractiveBrokersExecutionClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for InteractiveBrokersExecutionClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionClientFactory for InteractiveBrokersExecutionClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: Rc<RefCell<Cache>>,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let factory_config = config
            .as_any()
            .downcast_ref::<InteractiveBrokersExecFactoryConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for InteractiveBrokersExecutionClientFactory. Expected InteractiveBrokersExecFactoryConfig, was {config:?}",
                )
            })?
            .clone();
        let mut config = factory_config.config;
        let account_id = ib_account_id(config.account_id.as_deref())?;
        config.account_id = Some(account_id.to_string());

        let core = ExecutionClientCore::new(
            factory_config.trader_id,
            ClientId::from(name),
            *IB_VENUE,
            OmsType::Netting,
            account_id,
            AccountType::Margin,
            None,
            cache,
        );

        let instrument_provider = Arc::new(InteractiveBrokersInstrumentProvider::new(
            factory_config.instrument_provider,
        ));
        let client = InteractiveBrokersExecutionClient::new(core, config, instrument_provider)?;

        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        "IB"
    }

    fn config_type(&self) -> &'static str {
        "InteractiveBrokersExecFactoryConfig"
    }
}

fn ib_account_id(value: Option<&str>) -> anyhow::Result<AccountId> {
    let value = value.filter(|value| !value.is_empty()).ok_or_else(|| {
        anyhow::anyhow!("InteractiveBrokersExecClientConfig.account_id is required")
    })?;

    if value.starts_with("IB-") {
        Ok(AccountId::from(value))
    } else {
        Ok(AccountId::from(format!("IB-{value}")))
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        clock::TestClock,
        factories::{DataClientFactory, ExecutionClientFactory},
    };
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn data_factory_rejects_wrong_config_type() {
        let factory = InteractiveBrokersDataClientFactory::new();
        let wrong_config = InteractiveBrokersExecFactoryConfig {
            trader_id: TraderId::from("TRADER-001"),
            config: InteractiveBrokersExecClientConfig {
                account_id: Some("DU12345".to_string()),
                ..Default::default()
            },
            instrument_provider: InteractiveBrokersInstrumentProviderConfig::default(),
        };

        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(TestClock::new()));

        let result = factory.create("IB-TEST", &wrong_config, cache, clock);

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
    fn execution_factory_requires_config_account_id() {
        let factory = InteractiveBrokersExecutionClientFactory::new();
        let config = InteractiveBrokersExecFactoryConfig {
            trader_id: TraderId::from("TRADER-001"),
            config: InteractiveBrokersExecClientConfig::default(),
            instrument_provider: InteractiveBrokersInstrumentProviderConfig::default(),
        };

        let cache = Rc::new(RefCell::new(Cache::default()));

        let result = factory.create("IB-TEST", &config, cache);

        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("account_id is required")
        );
    }

    #[rstest]
    fn execution_factory_uses_normalized_config_account_id() {
        let factory = InteractiveBrokersExecutionClientFactory::new();
        let config = InteractiveBrokersExecFactoryConfig {
            trader_id: TraderId::from("TRADER-001"),
            config: InteractiveBrokersExecClientConfig {
                account_id: Some("DU12345".to_string()),
                ..Default::default()
            },
            instrument_provider: InteractiveBrokersInstrumentProviderConfig::default(),
        };

        let cache = Rc::new(RefCell::new(Cache::default()));

        let client = factory.create("IB-TEST", &config, cache).unwrap();

        assert_eq!(client.account_id(), AccountId::from("IB-DU12345"));
    }
}
