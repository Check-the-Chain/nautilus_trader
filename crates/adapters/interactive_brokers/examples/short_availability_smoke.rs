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

//! Interactive Brokers live short-availability custom-data smoke test.
//!
//! Data-only smoke:
//! `cargo run -p nautilus-interactive-brokers --example ib-short-availability-smoke --features examples`
//!
//! Optional environment variables:
//! - `IB_INSTRUMENT_ID`, defaults to `AAPL.NASDAQ`
//! - `IB_HOST`, defaults to `127.0.0.1`
//! - `IB_PORT`, defaults to `4002`
//! - `IB_CLIENT_ID`, defaults to `1`
//! - `IB_MARKET_DATA_TYPE`, one of `realtime`, `delayed`, `frozen`, `delayed_frozen`
//! - `SMOKE_DURATION_SECS`, defaults to `30`

use std::{collections::HashSet, env, sync::Arc, time::Duration};

use nautilus_common::{
    clients::DataClient,
    live::runner::replace_data_event_sender,
    messages::{DataEvent, data::SubscribeCustomData},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_interactive_brokers::{
    common::consts::IB,
    config::{
        InteractiveBrokersDataClientConfig, InteractiveBrokersInstrumentProviderConfig,
        MarketDataType,
    },
    data::InteractiveBrokersDataClient,
    data_types::IbkrShortAvailability,
    providers::instruments::InteractiveBrokersInstrumentProvider,
};
use nautilus_model::{
    data::Data,
    identifiers::{ClientId, InstrumentId},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let instrument_id = env_string("IB_INSTRUMENT_ID", "AAPL.NASDAQ").parse::<InstrumentId>()?;
    let host = env_string("IB_HOST", "127.0.0.1");
    let port = env_u16("IB_PORT", 4002);
    let client_id = env_i32("IB_CLIENT_ID", 1);
    let duration_secs = env_u64("SMOKE_DURATION_SECS", 30);
    let market_data_type = market_data_type_from_env();

    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(sender);

    let provider_config = InteractiveBrokersInstrumentProviderConfig {
        load_ids: HashSet::from([instrument_id]),
        ..Default::default()
    };
    let provider = Arc::new(InteractiveBrokersInstrumentProvider::new(
        provider_config.clone(),
    ));
    let data_config = InteractiveBrokersDataClientConfig {
        host,
        port,
        client_id,
        market_data_type,
        instrument_provider: provider_config,
        ..Default::default()
    };

    let mut client =
        InteractiveBrokersDataClient::new(ClientId::from(IB), data_config, Arc::clone(&provider))?;

    client.connect().await?;
    client.start()?;

    let data_type = IbkrShortAvailability::data_type_for_instrument(instrument_id);
    client.subscribe(SubscribeCustomData::new(
        Some(ClientId::from(IB)),
        None,
        data_type,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    ))?;

    println!("subscribed instrument_id={instrument_id} duration_secs={duration_secs}");

    let deadline = tokio::time::sleep(Duration::from_secs(duration_secs));
    tokio::pin!(deadline);
    let mut custom_updates = 0usize;
    let mut other_events = 0usize;

    loop {
        tokio::select! {
            () = &mut deadline => {
                break;
            }
            event = receiver.recv() => {
                let Some(event) = event else {
                    break;
                };

                match event {
                    DataEvent::Data(Data::Custom(custom)) => {
                        if let Some(update) = custom.data.as_any().downcast_ref::<IbkrShortAvailability>() {
                            custom_updates += 1;
                            println!(
                                "short_availability instrument_id={} score_e6={:?} shares={:?} ts_event={}",
                                update.instrument_id,
                                update.shortable_score_e6,
                                update.shortable_shares,
                                update.ts_event,
                            );
                            break;
                        }
                    }
                    _ => {
                        other_events += 1;
                    }
                }
            }
        }
    }

    client.stop()?;
    println!("summary custom_updates={custom_updates} other_events={other_events}");

    if custom_updates == 0 {
        anyhow::bail!(
            "no IBKR short-availability custom data received for {instrument_id} within {duration_secs}s"
        );
    }

    Ok(())
}

fn env_string(key: &str, default: &str) -> String {
    env::var(key)
        .or_else(|_| env::var(format!("NAUTILUS_{key}")))
        .unwrap_or_else(|_| default.to_string())
}

fn env_i32(key: &str, default: i32) -> i32 {
    env_string(key, &default.to_string())
        .parse()
        .unwrap_or(default)
}

fn env_u16(key: &str, default: u16) -> u16 {
    env_string(key, &default.to_string())
        .parse()
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env_string(key, &default.to_string())
        .parse()
        .unwrap_or(default)
}

fn market_data_type_from_env() -> MarketDataType {
    match env_string("IB_MARKET_DATA_TYPE", "realtime")
        .to_ascii_lowercase()
        .as_str()
    {
        "delayed" => MarketDataType::Delayed,
        "frozen" => MarketDataType::Frozen,
        "delayed_frozen" | "delayed-frozen" => MarketDataType::DelayedFrozen,
        _ => MarketDataType::Realtime,
    }
}
