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

//! Optional live latency probe helpers.
//!
//! The probe is disabled by default and is intended for ad-hoc diagnostics.
//! Enable with `NAUTILUS_LATENCY_PROBE=1`.

use std::{
    collections::VecDeque,
    env,
    sync::{Mutex, OnceLock},
};

use ahash::AHashMap;

use crate::{UnixNanos, time::get_atomic_clock_realtime};

const ENABLED_ENV: &str = "NAUTILUS_LATENCY_PROBE";
const INTERVAL_ENV: &str = "NAUTILUS_LATENCY_PROBE_INTERVAL_SECS";
const SAMPLE_LIMIT_ENV: &str = "NAUTILUS_LATENCY_PROBE_SAMPLE_LIMIT";

static CONFIG: OnceLock<LatencyProbeConfig> = OnceLock::new();
static STATE: OnceLock<Mutex<LatencyProbeState>> = OnceLock::new();

/// Returns whether the optional latency probe is enabled.
#[must_use]
pub fn enabled() -> bool {
    config().enabled
}

/// Returns a monotonic realtime timestamp suitable for latency probe measurements.
#[must_use]
pub fn timestamp_ns() -> u64 {
    get_atomic_clock_realtime().get_time_ns().as_u64()
}

/// Records elapsed time from `ts_init` to the current realtime clock.
///
/// This is a no-op unless `NAUTILUS_LATENCY_PROBE=1`.
pub fn record_since_init(stage: &'static str, ts_init: UnixNanos) {
    if !enabled() {
        return;
    }

    let now_ns = timestamp_ns();
    let elapsed_us = now_ns.saturating_sub(ts_init.as_u64()) / 1_000;
    record_sample(stage, elapsed_us, now_ns);
}

/// Records a direct duration between two realtime clock samples.
///
/// This is a no-op unless `NAUTILUS_LATENCY_PROBE=1`.
pub fn record_duration(stage: &'static str, start_ns: u64, end_ns: u64) {
    if !enabled() {
        return;
    }

    let elapsed_us = end_ns.saturating_sub(start_ns) / 1_000;
    record_sample(stage, elapsed_us, end_ns);
}

fn config() -> &'static LatencyProbeConfig {
    CONFIG.get_or_init(LatencyProbeConfig::from_env)
}

fn state() -> &'static Mutex<LatencyProbeState> {
    STATE.get_or_init(|| Mutex::new(LatencyProbeState::new()))
}

fn record_sample(stage: &'static str, elapsed_us: u64, now_ns: u64) {
    let Ok(mut probe_state) = state().lock() else {
        return;
    };
    probe_state.record(stage, elapsed_us);
    probe_state.maybe_log(config(), now_ns);
}

#[derive(Debug, Clone, Copy)]
struct LatencyProbeConfig {
    enabled: bool,
    interval_ns: u64,
    sample_limit: usize,
}

impl LatencyProbeConfig {
    fn from_env() -> Self {
        let enabled = env::var(ENABLED_ENV).is_ok_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        });
        let interval_secs = parse_env_or(INTERVAL_ENV, 5_u64).max(1);
        let sample_limit = parse_env_or(SAMPLE_LIMIT_ENV, 20_000_usize).max(1);

        Self {
            enabled,
            interval_ns: interval_secs.saturating_mul(1_000_000_000),
            sample_limit,
        }
    }
}

#[derive(Debug)]
struct LatencyProbeState {
    stages: AHashMap<&'static str, StageStats>,
    last_log_ns: u64,
}

impl LatencyProbeState {
    fn new() -> Self {
        Self {
            stages: AHashMap::new(),
            last_log_ns: 0,
        }
    }

    fn record(&mut self, stage: &'static str, elapsed_us: u64) {
        let sample_limit = config().sample_limit;
        self.stages
            .entry(stage)
            .or_insert_with(|| StageStats::new(sample_limit))
            .record(elapsed_us, sample_limit);
    }

    fn maybe_log(&mut self, config: &LatencyProbeConfig, now_ns: u64) {
        if self.last_log_ns == 0 {
            self.last_log_ns = now_ns;
            return;
        }
        if now_ns.saturating_sub(self.last_log_ns) < config.interval_ns {
            return;
        }

        for (stage, stats) in &self.stages {
            if let Some(summary) = stats.summary() {
                log::info!(
                    "latency_probe stage={} samples={} window={} us={}",
                    stage,
                    stats.total_count,
                    stats.samples.len(),
                    summary,
                );
            }
        }
        self.last_log_ns = now_ns;
    }
}

#[derive(Debug)]
struct StageStats {
    samples: VecDeque<u64>,
    total_count: u64,
}

impl StageStats {
    fn new(sample_limit: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(sample_limit.min(1024)),
            total_count: 0,
        }
    }

    fn record(&mut self, elapsed_us: u64, sample_limit: usize) {
        self.total_count += 1;
        if self.samples.len() == sample_limit {
            self.samples.pop_front();
        }
        self.samples.push_back(elapsed_us);
    }

    fn summary(&self) -> Option<LatencySummary> {
        if self.samples.is_empty() {
            return None;
        }

        let mut sorted = self.samples.iter().copied().collect::<Vec<_>>();
        sorted.sort_unstable();
        Some(LatencySummary {
            min: sorted[0],
            p50: percentile(&sorted, 50),
            p95: percentile(&sorted, 95),
            p99: percentile(&sorted, 99),
            max: sorted[sorted.len() - 1],
        })
    }
}

#[derive(Debug)]
struct LatencySummary {
    min: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

impl std::fmt::Display for LatencySummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "min={} p50={} p95={} p99={} max={}",
            self.min, self.p50, self.p95, self.p99, self.max
        )
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let rank = sorted.len().saturating_sub(1) * percentile / 100;
    sorted[rank]
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
