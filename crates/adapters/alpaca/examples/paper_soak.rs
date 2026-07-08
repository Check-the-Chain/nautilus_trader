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

//! Short full-coverage soak test for the Alpaca adapter (paper account).
//!
//! Runs everything concurrently for `SOAK_SECS` during market hours:
//!
//! - Market data WebSocket (msgpack, IEX feed) on real symbols: trades,
//!   quotes, and bars, with per-type counts, decode-gap tracking, and
//!   `ts_event`->`ts_init` latency percentiles.
//! - Mid-soak subscription churn: unsubscribe one symbol's trades, subscribe
//!   another symbol, verify acks reflect both.
//! - Historical REST sprinkled through the soak (bars/trades/quotes) to
//!   exercise concurrent REST + WS on the shared rate limiter.
//! - Trade-updates stream + order coverage: marketable buy (fill), resting
//!   limit (cancel), marketable sell back to flat.
//! - End: reconcile stream-observed fills against `GET /v2/account/activities`.
//!
//! Run with:
//! `cargo run --example alpaca-paper-soak -p nautilus-alpaca`

use std::time::{Duration, Instant};

use chrono::{Timelike, Utc};
use nautilus_alpaca::{
    common::{
        credential::Credential,
        enums::{
            AlpacaDataFeed, AlpacaEnvironment, AlpacaOrderSide, AlpacaOrderType,
            AlpacaTimeInForce, AlpacaTradeUpdateEvent,
        },
    },
    http::{
        client::AlpacaHttpClient,
        query::{
            GetAccountActivitiesParamsBuilder, GetStockBarsParamsBuilder,
            GetStockQuotesParamsBuilder, GetStockTradesParamsBuilder, PostOrderParamsBuilder,
        },
    },
    websocket::{
        client::{AlpacaTradeUpdatesWebSocketClient, AlpacaWebSocketClient},
        messages::{AlpacaInstrumentInfo, NautilusWsMessage, WsFormat},
    },
};
use nautilus_model::{identifiers::InstrumentId, instruments::Instrument};
use nautilus_network::websocket::TransportBackend;

const SOAK_SECS: u64 = 300;
const SYMBOLS: [&str; 4] = ["AAPL", "SPY", "TSLA", "NVDA"];
const CHURN_UNSUB: &str = "TSLA";
const CHURN_SUB: &str = "MSFT";
const ORDER_SYMBOL: &str = "AAPL";
const ORDER_QTY: u32 = 5;
const FILL_WAIT_SECS: u64 = 60;

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

fn instrument_id(symbol: &str) -> InstrumentId {
    InstrumentId::from(format!("{symbol}.ALPACA").as_str())
}

fn is_regular_hours() -> bool {
    // Regular session: 13:30-20:00 UTC (09:30-16:00 ET) on weekdays.
    let now = Utc::now();
    let minutes = now.hour() * 60 + now.minute();
    (13 * 60 + 30..20 * 60).contains(&minutes)
}

#[derive(Default)]
struct MarketDataStats {
    trades: u64,
    quotes: u64,
    bars: u64,
    acks: u64,
    errors: u64,
    reconnects: u64,
    latencies_ns: Vec<u64>,
    churn_unsub_acked: bool,
    churn_sub_seen: bool,
}

impl MarketDataStats {
    fn record_latency(&mut self, ts_event: u64, ts_init: u64) {
        self.latencies_ns.push(ts_init.saturating_sub(ts_event));
    }

    fn percentile_ms(&mut self, pct: f64) -> f64 {
        if self.latencies_ns.is_empty() {
            return 0.0;
        }
        self.latencies_ns.sort_unstable();
        let idx = ((self.latencies_ns.len() as f64 - 1.0) * pct) as usize;
        self.latencies_ns[idx] as f64 / 1_000_000.0
    }
}

async fn await_terminal_event(
    updates: &mut AlpacaTradeUpdatesWebSocketClient,
    wanted: &[AlpacaTradeUpdateEvent],
    fail_on: &[AlpacaTradeUpdateEvent],
    wait_secs: u64,
) -> anyhow::Result<(AlpacaTradeUpdateEvent, Option<String>, Option<String>)> {
    let deadline = Instant::now() + Duration::from_secs(wait_secs);
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, updates.next_event()).await {
            Ok(Some(NautilusWsMessage::TradeUpdate(update))) => {
                println!(
                    "  event={:?} status={:?} price={:?} qty={:?}",
                    update.event, update.order.status, update.price, update.qty,
                );
                if wanted.contains(&update.event) {
                    return Ok((update.event, update.price.clone(), update.qty.clone()));
                }
                if fail_on.contains(&update.event) {
                    anyhow::bail!("unexpected terminal event: {:?}", update.event);
                }
            }
            Ok(Some(_)) => {}
            Ok(None) => anyhow::bail!("trade-updates stream ended unexpectedly"),
            Err(_) => break,
        }
    }
    anyhow::bail!("no terminal event within {wait_secs}s")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    let env = AlpacaEnvironment::Paper;
    let credential = Credential::resolve(None, None, env)?
        .ok_or_else(|| anyhow::anyhow!("set ALPACA_PAPER_API_KEY and ALPACA_PAPER_API_SECRET"))?;
    let http = AlpacaHttpClient::from_env(env)?;
    let run_started = Utc::now();
    let mut failures: Vec<String> = Vec::new();

    banner("SETUP");
    let instruments = http.request_instruments().await?;
    println!("instruments cached: {}", instruments.len());

    // ------------------------------------------------ market data collector
    let mut ws = AlpacaWebSocketClient::new(
        None,
        AlpacaDataFeed::Iex,
        credential.clone(),
        WsFormat::Msgpack,
        TransportBackend::default(),
        None,
    );
    let all_symbols: Vec<&str> = SYMBOLS.iter().copied().chain([CHURN_SUB]).collect();
    ws.initialize_instruments(
        all_symbols
            .iter()
            .filter_map(|s| {
                let instrument = http.get_instrument(&ustr::Ustr::from(s))?;
                Some(AlpacaInstrumentInfo {
                    instrument_id: instrument.id(),
                    price_precision: instrument.price_precision(),
                    size_precision: instrument.size_precision(),
                })
            })
            .collect(),
    );
    ws.connect().await?;
    for symbol in SYMBOLS {
        ws.subscribe_trades(instrument_id(symbol)).await?;
        ws.subscribe_quotes(instrument_id(symbol)).await?;
        ws.subscribe_bars(instrument_id(symbol)).await?;
    }
    println!("market data connected (msgpack, IEX): {} symbols x trades/quotes/bars", SYMBOLS.len());

    // The original client owns the event receiver and moves into the
    // collector task; the clone shares the command channel for churn and
    // disconnect.
    let mut churn_handle = ws.clone();
    let mut collector_ws = ws;
    let collector_task = tokio::spawn(async move {
        let mut stats = MarketDataStats::default();
        let deadline = Instant::now() + Duration::from_secs(SOAK_SECS);
        let churn_sub_id = ustr::Ustr::from(CHURN_SUB);
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, collector_ws.next_event()).await {
                Ok(Some(NautilusWsMessage::Trades(trades))) => {
                    for trade in &trades {
                        stats.record_latency(trade.ts_event.as_u64(), trade.ts_init.as_u64());
                        if trade.instrument_id.symbol.inner() == churn_sub_id {
                            stats.churn_sub_seen = true;
                        }
                    }
                    stats.trades += trades.len() as u64;
                }
                Ok(Some(NautilusWsMessage::Quote(quote))) => {
                    stats.record_latency(quote.ts_event.as_u64(), quote.ts_init.as_u64());
                    if quote.instrument_id.symbol.inner() == churn_sub_id {
                        stats.churn_sub_seen = true;
                    }
                    stats.quotes += 1;
                }
                Ok(Some(NautilusWsMessage::Bar(_))) => stats.bars += 1,
                Ok(Some(NautilusWsMessage::SubscriptionAck(ack))) => {
                    stats.acks += 1;
                    let unsub = ustr::Ustr::from(CHURN_UNSUB);
                    if !ack.trades.contains(&unsub) && stats.acks > 1 {
                        stats.churn_unsub_acked = true;
                    }
                }
                Ok(Some(NautilusWsMessage::Error { .. })) => stats.errors += 1,
                Ok(Some(NautilusWsMessage::Reconnected)) => stats.reconnects += 1,
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => break,
            }
        }
        stats
    });

    // ------------------------------------------------------- REST sprinkler
    let rest_http = http.clone();
    let rest_task = tokio::spawn(async move {
        let mut ok = 0u32;
        let mut err = 0u32;
        let deadline = Instant::now() + Duration::from_secs(SOAK_SECS);
        while Instant::now() < deadline {
            let bars = rest_http
                .get_stock_bars(
                    &GetStockBarsParamsBuilder::default()
                        .symbols("SPY")
                        .timeframe("1Min")
                        .limit(5u32)
                        .feed(AlpacaDataFeed::Iex)
                        .build()
                        .expect("valid params"),
                )
                .await;
            let trades = rest_http
                .get_stock_trades_paginated(
                    &GetStockTradesParamsBuilder::default()
                        .symbols("AAPL")
                        .limit(5u32)
                        .feed(AlpacaDataFeed::Iex)
                        .build()
                        .expect("valid params"),
                )
                .await;
            let quotes = rest_http
                .get_stock_quotes_paginated(
                    &GetStockQuotesParamsBuilder::default()
                        .symbols("NVDA")
                        .limit(5u32)
                        .feed(AlpacaDataFeed::Iex)
                        .build()
                        .expect("valid params"),
                )
                .await;
            for result in [bars.is_ok(), trades.is_ok(), quotes.is_ok()] {
                if result {
                    ok += 1;
                } else {
                    err += 1;
                }
            }
            tokio::time::sleep(Duration::from_secs(15)).await;
        }
        (ok, err)
    });

    // ------------------------------------------- orders on the updates stream
    banner("ORDER COVERAGE (concurrent with soak)");
    let mut updates = AlpacaTradeUpdatesWebSocketClient::new(
        None,
        env,
        credential,
        TransportBackend::default(),
        None,
    );
    updates.connect().await?;

    let reference = {
        let trades = http
            .get_stock_trades_paginated(
                &GetStockTradesParamsBuilder::default()
                    .symbols(ORDER_SYMBOL)
                    .limit(1u32)
                    .sort("desc")
                    .feed(AlpacaDataFeed::Iex)
                    .build()?,
            )
            .await?;
        trades
            .get(ORDER_SYMBOL)
            .and_then(|t| t.first())
            .map(|t| t.p)
            .ok_or_else(|| anyhow::anyhow!("no reference price"))?
    };
    println!("reference {ORDER_SYMBOL}: {reference:.2}");

    let mut stream_fill_qty = 0.0f64;
    let extended_hours = !is_regular_hours();
    let mut bought = false;
    println!("session: extended_hours={extended_hours}");

    // 1) Marketable buy -> expect fill.
    println!("-> marketable BUY {ORDER_QTY}");
    let buy_order = http
        .submit_order(
            &PostOrderParamsBuilder::default()
                .symbol(ORDER_SYMBOL)
                .qty(ORDER_QTY.to_string())
                .side(AlpacaOrderSide::Buy)
                .order_type(AlpacaOrderType::Limit)
                .time_in_force(AlpacaTimeInForce::Day)
                .limit_price(format!("{:.2}", reference * 1.02))
                .extended_hours(extended_hours)
                .build()?,
        )
        .await?;
    match await_terminal_event(
        &mut updates,
        &[AlpacaTradeUpdateEvent::Fill],
        &[
            AlpacaTradeUpdateEvent::Canceled,
            AlpacaTradeUpdateEvent::Rejected,
            AlpacaTradeUpdateEvent::Expired,
        ],
        FILL_WAIT_SECS,
    )
    .await
    {
        Ok((_, _, qty)) => {
            stream_fill_qty += qty.as_deref().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
            bought = true;
        }
        Err(e) => {
            failures.push(format!("buy fill: {e}"));
            // Cancel the unfilled buy so the sell leg cannot trip the
            // venue's opposite-side wash-trade rejection (code 40310000).
            if http.cancel_order(&buy_order.id).await.is_ok() {
                println!("  unfilled buy canceled");
            }
        }
    }

    // 2) Resting limit far below -> cancel -> expect canceled.
    println!("-> resting BUY (to cancel)");
    let resting = http
        .submit_order(
            &PostOrderParamsBuilder::default()
                .symbol(ORDER_SYMBOL)
                .qty("1")
                .side(AlpacaOrderSide::Buy)
                .order_type(AlpacaOrderType::Limit)
                .time_in_force(AlpacaTimeInForce::Day)
                .limit_price(format!("{:.2}", reference * 0.80))
                .extended_hours(extended_hours)
                .build()?,
        )
        .await?;
    http.cancel_order(&resting.id).await?;
    if let Err(e) = await_terminal_event(
        &mut updates,
        &[AlpacaTradeUpdateEvent::Canceled],
        &[AlpacaTradeUpdateEvent::Fill, AlpacaTradeUpdateEvent::Rejected],
        30,
    )
    .await
    {
        failures.push(format!("resting cancel: {e}"));
    }

    // 3) Marketable sell back to flat -> expect fill (skipped if the buy
    //    never filled: an opposite-side order would either wash-trade reject
    //    or open a short).
    if bought {
        println!("-> marketable SELL {ORDER_QTY} (flatten)");
        let sell_result = http
            .submit_order(
                &PostOrderParamsBuilder::default()
                    .symbol(ORDER_SYMBOL)
                    .qty(ORDER_QTY.to_string())
                    .side(AlpacaOrderSide::Sell)
                    .order_type(AlpacaOrderType::Limit)
                    .time_in_force(AlpacaTimeInForce::Day)
                    .limit_price(format!("{:.2}", reference * 0.98))
                    .extended_hours(extended_hours)
                    .build()?,
            )
            .await;
        match sell_result {
            Ok(_) => match await_terminal_event(
                &mut updates,
                &[AlpacaTradeUpdateEvent::Fill],
                &[
                    AlpacaTradeUpdateEvent::Canceled,
                    AlpacaTradeUpdateEvent::Rejected,
                    AlpacaTradeUpdateEvent::Expired,
                ],
                FILL_WAIT_SECS,
            )
            .await
            {
                Ok((_, _, qty)) => {
                    stream_fill_qty +=
                        qty.as_deref().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                }
                Err(e) => failures.push(format!("sell fill: {e}")),
            },
            Err(e) => failures.push(format!("sell submit: {e}")),
        }
    } else {
        println!("-> SELL skipped (no position)");
    }

    let positions = http.get_positions().await?;
    let flat = !positions
        .iter()
        .any(|p| p.symbol.as_str() == ORDER_SYMBOL);
    if !flat {
        failures.push(format!("{ORDER_SYMBOL} position not flat after round trip"));
    }
    println!("position flat: {flat}");

    // -------------------------------------------------- mid-soak churn
    banner("SUBSCRIPTION CHURN (mid-soak)");
    tokio::time::sleep(Duration::from_secs(10)).await;
    churn_handle.unsubscribe_trades(instrument_id(CHURN_UNSUB)).await?;
    churn_handle.subscribe_trades(instrument_id(CHURN_SUB)).await?;
    churn_handle.subscribe_quotes(instrument_id(CHURN_SUB)).await?;
    println!("unsubscribed {CHURN_UNSUB} trades; subscribed {CHURN_SUB} trades+quotes");

    // ------------------------------------------------------- wait for soak
    banner("SOAKING");
    let mut stats = collector_task.await?;
    let (rest_ok, rest_err) = rest_task.await?;

    // ------------------------------------------------- fill reconciliation
    banner("FILL RECONCILIATION");
    let mut activity_fill_qty = 0.0f64;
    for attempt in 1..=3u32 {
        tokio::time::sleep(Duration::from_secs(5 * u64::from(attempt))).await;
        let activities = http
            .get_account_activities(
                &GetAccountActivitiesParamsBuilder::default()
                    .activity_types("FILL")
                    .after((run_started - chrono::Duration::minutes(1)).to_rfc3339())
                    .direction("asc")
                    .build()?,
            )
            .await?;
        activity_fill_qty = activities
            .iter()
            .filter_map(|a| a.qty.as_deref()?.parse::<f64>().ok())
            .sum();
        println!(
            "attempt {attempt}: {} FILL activities, total qty {activity_fill_qty}",
            activities.len(),
        );
        if (activity_fill_qty - stream_fill_qty).abs() < f64::EPSILON {
            break;
        }
    }
    if (activity_fill_qty - stream_fill_qty).abs() > f64::EPSILON {
        failures.push(format!(
            "fill qty mismatch: stream={stream_fill_qty} activities={activity_fill_qty}",
        ));
    }

    updates.disconnect().await?;
    churn_handle.disconnect().await.ok();

    // -------------------------------------------------------------- report
    banner("SOAK REPORT");
    let p50 = stats.percentile_ms(0.50);
    let p95 = stats.percentile_ms(0.95);
    let p100 = stats.percentile_ms(1.0);
    println!("duration: {SOAK_SECS}s | symbols: {SYMBOLS:?} (+{CHURN_SUB}, -{CHURN_UNSUB} trades)");
    println!(
        "market data: {} trades, {} quotes, {} bars, {} acks, {} errors, {} reconnects",
        stats.trades, stats.quotes, stats.bars, stats.acks, stats.errors, stats.reconnects,
    );
    println!("tick latency (venue->parsed): p50={p50:.1}ms p95={p95:.1}ms max={p100:.1}ms");
    println!("REST sprinkler: {rest_ok} ok, {rest_err} errors");
    println!(
        "churn: unsub acked={}, new symbol ticked={}",
        stats.churn_unsub_acked, stats.churn_sub_seen,
    );
    println!("stream fill qty={stream_fill_qty} vs activities qty={activity_fill_qty}");

    if stats.trades + stats.quotes == 0 {
        failures.push("no market data received".to_string());
    }
    if stats.bars == 0 {
        failures.push("no bars received".to_string());
    }
    if rest_err > 0 {
        failures.push(format!("{rest_err} REST errors during soak"));
    }
    if stats.errors > 0 {
        failures.push(format!("{} WS venue errors", stats.errors));
    }

    if failures.is_empty() {
        println!("\nRESULT: PASS (all checks green)");
        Ok(())
    } else {
        println!("\nRESULT: FAIL");
        for failure in &failures {
            println!("  - {failure}");
        }
        anyhow::bail!("{} soak check(s) failed", failures.len())
    }
}
