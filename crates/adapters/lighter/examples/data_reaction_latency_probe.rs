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

//! Lighter live data-to-strategy latency probe.
//!
//! This is read-only: it subscribes to live data and never places orders.
//!
//! Run with:
//! `cargo run -p nautilus-lighter --example lighter-data-reaction-latency-probe --features examples`
//!
//! Optional environment variables:
//! - `LIGHTER_INSTRUMENT_ID`, defaults to `BTC-PERP.LIGHTER`
//! - `LIGHTER_ENV`, use `testnet` for testnet, defaults to mainnet
//! - `LIGHTER_BASE_URL_HTTP` / `LIGHTER_BASE_URL_WS`, for endpoint overrides
//! - `LIGHTER_PROXY_URL`, for HTTP/WebSocket proxying
//! - `NAUTILUS_LATENCY_PROBE=1`, enables internal runner/data-engine/msgbus stage timings
//! - `PROBE_DURATION_SECS`, defaults to 60
//! - `PROBE_STATS_INTERVAL_SECS`, defaults to 5
//! - `PROBE_SAMPLE_LIMIT`, defaults to 20000 samples per stream

use std::{collections::VecDeque, env, time::Duration};

use log::LevelFilter;
use nautilus_common::{actor::DataActor, enums::Environment, logging::logger::LoggerConfig};
#[cfg(feature = "latency-probe")]
use nautilus_core::latency;
use nautilus_lighter::{
    config::{LighterDataClientConfig, LighterEnvironment},
    factories::LighterDataClientFactory,
};
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::{OrderBookDeltas, QuoteTick},
    enums::BookType,
    identifiers::{ClientId, InstrumentId, StrategyId, TraderId},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};

#[derive(Debug)]
struct LatencyProbeConfig {
    base: StrategyConfig,
    client_id: ClientId,
    instrument_id: InstrumentId,
    stats_interval_ns: u64,
    sample_limit: usize,
}

#[derive(Debug)]
struct LatencyProbeStrategy {
    core: StrategyCore,
    client_id: ClientId,
    instrument_id: InstrumentId,
    stats_interval_ns: u64,
    quote_stats: LatencyStats,
    book_stats: LatencyStats,
    last_stats_ns: u64,
}

impl LatencyProbeStrategy {
    fn new(config: LatencyProbeConfig) -> Self {
        Self {
            core: StrategyCore::new(config.base),
            client_id: config.client_id,
            instrument_id: config.instrument_id,
            stats_interval_ns: config.stats_interval_ns,
            quote_stats: LatencyStats::new("quote", config.sample_limit),
            book_stats: LatencyStats::new("book_deltas", config.sample_limit),
            last_stats_ns: 0,
        }
    }

    fn record_quote(&mut self, quote: &QuoteTick) {
        let callback_start_ns = self.clock().timestamp_ns().as_u64();
        #[cfg(feature = "latency-probe")]
        latency::record_since_init("strategy.quote_callback_start", quote.ts_init);
        let callback_end_ns = self.clock().timestamp_ns().as_u64();
        self.quote_stats.record(
            quote.ts_event.as_u64(),
            quote.ts_init.as_u64(),
            callback_start_ns,
            callback_end_ns,
        );
        self.maybe_log(callback_start_ns);
    }

    fn record_book_deltas(&mut self, deltas: &OrderBookDeltas) {
        let callback_start_ns = self.clock().timestamp_ns().as_u64();
        #[cfg(feature = "latency-probe")]
        latency::record_since_init("strategy.deltas_callback_start", deltas.ts_init);
        let callback_end_ns = self.clock().timestamp_ns().as_u64();
        self.book_stats.record(
            deltas.ts_event.as_u64(),
            deltas.ts_init.as_u64(),
            callback_start_ns,
            callback_end_ns,
        );
        self.maybe_log(callback_start_ns);
    }

    fn maybe_log(&mut self, now_ns: u64) {
        if self.last_stats_ns == 0 {
            self.last_stats_ns = now_ns;
            return;
        }

        if now_ns.saturating_sub(self.last_stats_ns) < self.stats_interval_ns {
            return;
        }

        self.quote_stats.log();
        self.book_stats.log();
        self.last_stats_ns = now_ns;
    }
}

impl DataActor for LatencyProbeStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        Strategy::on_start(self)?;
        self.subscribe_instrument(self.instrument_id, Some(self.client_id), None);
        self.subscribe_quotes(self.instrument_id, Some(self.client_id), None);
        self.subscribe_book_deltas(
            self.instrument_id,
            BookType::L2_MBP,
            None,
            Some(self.client_id),
            false,
            None,
        );
        log::info!(
            "Started Lighter latency probe for {}; no orders will be submitted",
            self.instrument_id
        );
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.unsubscribe_quotes(self.instrument_id, Some(self.client_id), None);
        self.unsubscribe_book_deltas(self.instrument_id, Some(self.client_id), None);
        self.quote_stats.log();
        self.book_stats.log();
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        if quote.instrument_id == self.instrument_id {
            self.record_quote(quote);
        }
        Ok(())
    }

    fn on_book_deltas(&mut self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        if deltas.instrument_id == self.instrument_id {
            self.record_book_deltas(deltas);
        }
        Ok(())
    }
}

nautilus_strategy!(LatencyProbeStrategy);

#[derive(Debug)]
struct LatencyStats {
    name: &'static str,
    sample_limit: usize,
    ingest_to_callback_us: VecDeque<u64>,
    event_age_us: VecDeque<u64>,
    handler_us: VecDeque<u64>,
    total_count: u64,
}

impl LatencyStats {
    fn new(name: &'static str, sample_limit: usize) -> Self {
        Self {
            name,
            sample_limit,
            ingest_to_callback_us: VecDeque::with_capacity(sample_limit.min(1024)),
            event_age_us: VecDeque::with_capacity(sample_limit.min(1024)),
            handler_us: VecDeque::with_capacity(sample_limit.min(1024)),
            total_count: 0,
        }
    }

    fn record(
        &mut self,
        ts_event_ns: u64,
        ts_init_ns: u64,
        callback_start_ns: u64,
        callback_end_ns: u64,
    ) {
        self.total_count += 1;
        push_capped(
            &mut self.ingest_to_callback_us,
            self.sample_limit,
            callback_start_ns.saturating_sub(ts_init_ns) / 1_000,
        );
        push_capped(
            &mut self.event_age_us,
            self.sample_limit,
            callback_start_ns.saturating_sub(ts_event_ns) / 1_000,
        );
        push_capped(
            &mut self.handler_us,
            self.sample_limit,
            callback_end_ns.saturating_sub(callback_start_ns) / 1_000,
        );
    }

    fn log(&self) {
        if self.ingest_to_callback_us.is_empty() {
            log::info!("latency {}: no samples yet", self.name);
            return;
        }

        let ingest = Summary::from_samples(&self.ingest_to_callback_us);
        let event_age = Summary::from_samples(&self.event_age_us);
        let handler = Summary::from_samples(&self.handler_us);
        log::info!(
            "latency {} samples={} window={} ingest_to_strategy_us={} event_age_us={} handler_us={}",
            self.name,
            self.total_count,
            self.ingest_to_callback_us.len(),
            ingest,
            event_age,
            handler,
        );
    }
}

#[derive(Debug)]
struct Summary {
    min: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

impl Summary {
    fn from_samples(samples: &VecDeque<u64>) -> Self {
        let mut sorted = samples.iter().copied().collect::<Vec<_>>();
        sorted.sort_unstable();
        Self {
            min: sorted[0],
            p50: percentile(&sorted, 50),
            p95: percentile(&sorted, 95),
            p99: percentile(&sorted, 99),
            max: sorted[sorted.len() - 1],
        }
    }
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "min={} p50={} p95={} p99={} max={}",
            self.min, self.p50, self.p95, self.p99, self.max
        )
    }
}

fn push_capped(samples: &mut VecDeque<u64>, limit: usize, value: u64) {
    if samples.len() == limit {
        samples.pop_front();
    }
    samples.push_back(value);
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let rank = (sorted.len().saturating_sub(1) * percentile) / 100;
    sorted[rank]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let instrument_id = env::var("LIGHTER_INSTRUMENT_ID")
        .unwrap_or_else(|_| "BTC-PERP.LIGHTER".to_string())
        .parse::<InstrumentId>()?;
    let duration_secs = parse_env_or("PROBE_DURATION_SECS", 60);
    let stats_interval_secs = parse_env_or("PROBE_STATS_INTERVAL_SECS", 5);
    let sample_limit = parse_env_or("PROBE_SAMPLE_LIMIT", 20_000).max(1);
    let lighter_environment = match env::var("LIGHTER_ENV")
        .unwrap_or_else(|_| "mainnet".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "testnet" => LighterEnvironment::Testnet,
        _ => LighterEnvironment::Mainnet,
    };

    let data_config = LighterDataClientConfig {
        base_url_http: env::var("LIGHTER_BASE_URL_HTTP").ok(),
        base_url_ws: env::var("LIGHTER_BASE_URL_WS").ok(),
        proxy_url: env::var("LIGHTER_PROXY_URL").ok(),
        environment: lighter_environment,
        ..Default::default()
    };
    let client_id = ClientId::from("LIGHTER");
    let trader_id = TraderId::from("LIGHTER-LATENCY-001");
    let strategy = LatencyProbeStrategy::new(LatencyProbeConfig {
        base: StrategyConfig {
            strategy_id: Some(StrategyId::from("LIGHTER-LATENCY-PROBE-001")),
            ..Default::default()
        },
        client_id,
        instrument_id,
        stats_interval_ns: stats_interval_secs * 1_000_000_000,
        sample_limit,
    });

    let log_config = LoggerConfig {
        stdout_level: LevelFilter::Info,
        ..Default::default()
    };
    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_name("LIGHTER-DATA-REACTION-LATENCY-PROBE".to_string())
        .with_logging(log_config)
        .with_delay_post_stop_secs(2)
        .add_data_client(
            None,
            Box::new(LighterDataClientFactory::new()),
            Box::new(data_config),
        )?
        .build()?;

    node.add_strategy(strategy)?;
    let handle = node.handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(duration_secs)).await;
        handle.stop();
    });

    node.run().await
}

fn parse_env_or<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<T>().ok())
        .unwrap_or(default)
}
