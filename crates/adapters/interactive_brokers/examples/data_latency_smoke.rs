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

//! Interactive Brokers live data latency smoke test.
//!
//! Run with:
//! `cargo run -p nautilus-interactive-brokers --example interactive-brokers-data-latency-smoke --features examples`
//!
//! Optional environment variables:
//! - `IB_INSTRUMENT_ID`, defaults to `AAPL.NASDAQ`
//! - `IB_HOST`, defaults to `127.0.0.1`
//! - `IB_PORT`, defaults to `4002` for paper IB Gateway
//! - `IB_CLIENT_ID`, defaults to `1`
//! - `IB_MARKET_DATA_TYPE`, one of `realtime`, `delayed`, `frozen`, `delayed_frozen`
//! - `IB_BATCH_QUOTES`, defaults to `true`
//! - `SMOKE_DURATION_SECS`, defaults to 30

use std::{collections::HashSet, env, time::Duration};

use nautilus_common::enums::Environment;
use nautilus_interactive_brokers::{
    config::{
        InteractiveBrokersDataClientConfig, InteractiveBrokersInstrumentProviderConfig,
        MarketDataType,
    },
    factories::{InteractiveBrokersDataClientFactory, InteractiveBrokersDataFactoryConfig},
};
use nautilus_live::node::LiveNode;
use nautilus_model::{
    enums::BookType,
    identifiers::{ClientId, InstrumentId, TraderId},
    stubs::TestDefault,
};
use nautilus_testkit::testers::{DataTester, DataTesterConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let instrument_id = env::var("IB_INSTRUMENT_ID")
        .unwrap_or_else(|_| "AAPL.NASDAQ".to_string())
        .parse::<InstrumentId>()?;
    let host = env::var("IB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("IB_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(4002);
    let client_id = env::var("IB_CLIENT_ID")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(1);
    let duration_secs = env::var("SMOKE_DURATION_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30);
    let market_data_type = match env::var("IB_MARKET_DATA_TYPE")
        .unwrap_or_else(|_| "realtime".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "delayed" => MarketDataType::Delayed,
        "frozen" => MarketDataType::Frozen,
        "delayed_frozen" | "delayed-frozen" => MarketDataType::DelayedFrozen,
        _ => MarketDataType::Realtime,
    };
    let batch_quotes = env::var("IB_BATCH_QUOTES").map_or(true, |value| {
        matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes")
    });

    let data_config = InteractiveBrokersDataClientConfig {
        host,
        port,
        client_id,
        market_data_type,
        batch_quotes,
        ..Default::default()
    };
    let provider_config = InteractiveBrokersInstrumentProviderConfig {
        load_ids: HashSet::from([instrument_id]),
        ..Default::default()
    };

    let mut node = LiveNode::builder(TraderId::test_default(), Environment::Live)?
        .with_name("IB-DATA-LATENCY-SMOKE".to_string())
        .with_delay_post_stop_secs(2)
        .add_data_client(
            None,
            Box::new(InteractiveBrokersDataClientFactory::new()),
            Box::new(InteractiveBrokersDataFactoryConfig {
                config: data_config,
                instrument_provider: provider_config,
            }),
        )?
        .build()?;

    let tester_config = DataTesterConfig::builder()
        .client_id(ClientId::from("IB"))
        .instrument_ids(vec![instrument_id])
        .subscribe_book_deltas(true)
        .subscribe_quotes(true)
        .subscribe_trades(true)
        .book_type(BookType::L2_MBP)
        .manage_book(false)
        .log_latency(true)
        .stats_interval_secs(0)
        .build();
    node.add_actor(DataTester::new(tester_config))?;

    let handle = node.handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(duration_secs)).await;
        handle.stop();
    });

    node.run().await
}
