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

//! Lighter live data latency smoke test.
//!
//! Run with:
//! `cargo run -p nautilus-lighter --example lighter-data-latency-smoke --features examples`
//!
//! Optional environment variables:
//! - `LIGHTER_INSTRUMENT_ID`, defaults to `BTC-USDC-PERP.LIGHTER`
//! - `LIGHTER_ENV`, use `testnet` for testnet, defaults to mainnet
//! - `LIGHTER_BASE_URL_HTTP` / `LIGHTER_BASE_URL_WS`, for endpoint overrides
//! - `LIGHTER_PROXY_URL`, for HTTP/WebSocket proxying
//! - `SMOKE_DURATION_SECS`, defaults to 30

use std::{env, time::Duration};

use nautilus_common::enums::Environment;
use nautilus_lighter::{
    config::{LighterDataClientConfig, LighterEnvironment},
    factories::LighterDataClientFactory,
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
    let instrument_id = env::var("LIGHTER_INSTRUMENT_ID")
        .unwrap_or_else(|_| "BTC-USDC-PERP.LIGHTER".to_string())
        .parse::<InstrumentId>()?;
    let lighter_environment = match env::var("LIGHTER_ENV")
        .unwrap_or_else(|_| "mainnet".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "testnet" => LighterEnvironment::Testnet,
        _ => LighterEnvironment::Mainnet,
    };
    let duration_secs = env::var("SMOKE_DURATION_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30);
    let data_config = LighterDataClientConfig {
        base_url_http: env::var("LIGHTER_BASE_URL_HTTP").ok(),
        base_url_ws: env::var("LIGHTER_BASE_URL_WS").ok(),
        proxy_url: env::var("LIGHTER_PROXY_URL").ok(),
        environment: lighter_environment,
        ..Default::default()
    };

    let mut node = LiveNode::builder(TraderId::test_default(), Environment::Live)?
        .with_name("LIGHTER-DATA-LATENCY-SMOKE".to_string())
        .with_delay_post_stop_secs(2)
        .add_data_client(
            None,
            Box::new(LighterDataClientFactory::new()),
            Box::new(data_config),
        )?
        .build()?;

    let tester_config = DataTesterConfig::builder()
        .client_id(ClientId::from("LIGHTER"))
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
