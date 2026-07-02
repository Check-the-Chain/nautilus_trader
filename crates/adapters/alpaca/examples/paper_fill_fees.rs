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

//! Paper-account fill and fee reconciliation test for the Alpaca adapter.
//!
//! Buys `QTY` shares with a marketable limit, observes the `fill` event on the
//! trade-updates stream, sells the position back to flat, then reads
//! `GET /v2/account/activities` to compare stream-reported executions with the
//! venue's booked `FILL` activities and any `FEE` postings (SEC / FINRA TAF on
//! sells). Requires `ALPACA_PAPER_API_KEY` / `ALPACA_PAPER_API_SECRET` and a
//! session where AAPL is tradable (regular hours, or pre/post market via the
//! extended-hours flag this example sets automatically).
//!
//! Run with:
//! `cargo run --example alpaca-paper-fill-fees -p nautilus-alpaca`

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
            GetAccountActivitiesParamsBuilder, GetStockTradesParamsBuilder, PostOrderParamsBuilder,
        },
    },
    websocket::{client::AlpacaTradeUpdatesWebSocketClient, messages::NautilusWsMessage},
};
use nautilus_network::websocket::TransportBackend;

const SYMBOL: &str = "AAPL";
const QTY: u32 = 50;
const FILL_WAIT_SECS: u64 = 90;
const FEE_POLL_ROUNDS: u32 = 4;
const FEE_POLL_DELAY_SECS: u64 = 10;

/// FINRA TAF per share sold (cap applies far above this trade size).
const TAF_PER_SHARE: f64 = 0.000_166;

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

fn is_regular_hours() -> bool {
    // Regular session: 13:30-20:00 UTC (09:30-16:00 ET) on weekdays.
    let now = Utc::now();
    let minutes = now.hour() * 60 + now.minute();
    (13 * 60 + 30..20 * 60).contains(&minutes)
}

struct FillCapture {
    price: f64,
    qty: f64,
    execution_id: Option<String>,
    position_qty: Option<String>,
}

async fn last_trade_price(http: &AlpacaHttpClient) -> anyhow::Result<f64> {
    let trades = http
        .get_stock_trades_paginated(
            &GetStockTradesParamsBuilder::default()
                .symbols(SYMBOL)
                .limit(1u32)
                .sort("desc")
                .feed(AlpacaDataFeed::Iex)
                .build()?,
        )
        .await?;
    trades
        .get(SYMBOL)
        .and_then(|t| t.first())
        .map(|t| t.p)
        .ok_or_else(|| anyhow::anyhow!("no recent {SYMBOL} trades on IEX"))
}

async fn submit_and_await_fill(
    http: &AlpacaHttpClient,
    updates: &mut AlpacaTradeUpdatesWebSocketClient,
    side: AlpacaOrderSide,
    limit_price: f64,
    extended_hours: bool,
) -> anyhow::Result<FillCapture> {
    let order = http
        .submit_order(
            &PostOrderParamsBuilder::default()
                .symbol(SYMBOL)
                .qty(QTY.to_string())
                .side(side)
                .order_type(AlpacaOrderType::Limit)
                .time_in_force(AlpacaTimeInForce::Day)
                .limit_price(format!("{limit_price:.2}"))
                .extended_hours(extended_hours)
                .build()?,
        )
        .await?;
    println!(
        "submitted {side:?} {QTY} {SYMBOL} limit {limit_price:.2} (id={}, extended_hours={extended_hours})",
        order.id,
    );

    let mut filled_qty = 0.0f64;
    let mut weighted_px = 0.0f64;
    let mut execution_id = None;
    let mut position_qty = None;

    let deadline = Instant::now() + Duration::from_secs(FILL_WAIT_SECS);
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, updates.next_event()).await {
            Ok(Some(NautilusWsMessage::TradeUpdate(update))) => {
                println!(
                    "  event={:?} status={:?} price={:?} qty={:?} position_qty={:?}",
                    update.event, update.order.status, update.price, update.qty,
                    update.position_qty,
                );
                match update.event {
                    AlpacaTradeUpdateEvent::Fill | AlpacaTradeUpdateEvent::PartialFill => {
                        let px: f64 = update
                            .price
                            .as_deref()
                            .unwrap_or("0")
                            .parse()
                            .unwrap_or(0.0);
                        let qty: f64 =
                            update.qty.as_deref().unwrap_or("0").parse().unwrap_or(0.0);
                        weighted_px += px * qty;
                        filled_qty += qty;
                        execution_id = update.execution_id.clone().or(execution_id);
                        position_qty = update.position_qty.clone().or(position_qty);
                        if update.event == AlpacaTradeUpdateEvent::Fill {
                            return Ok(FillCapture {
                                price: weighted_px / filled_qty,
                                qty: filled_qty,
                                execution_id,
                                position_qty,
                            });
                        }
                    }
                    AlpacaTradeUpdateEvent::Canceled
                    | AlpacaTradeUpdateEvent::Expired
                    | AlpacaTradeUpdateEvent::Rejected => {
                        anyhow::bail!("order terminated without fill: {:?}", update.event);
                    }
                    _ => {}
                }
            }
            Ok(Some(other)) => println!("  other event: {other:?}"),
            Ok(None) => anyhow::bail!("trade-updates stream ended unexpectedly"),
            Err(_) => break,
        }
    }

    // No full fill within the window: cancel and report what happened.
    http.cancel_order(&order.id).await.ok();
    anyhow::bail!(
        "no complete fill within {FILL_WAIT_SECS}s (filled {filled_qty}/{QTY}); order canceled",
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    let env = AlpacaEnvironment::Paper;

    let credential = Credential::resolve(None, None, env)?
        .ok_or_else(|| anyhow::anyhow!("set ALPACA_PAPER_API_KEY and ALPACA_PAPER_API_SECRET"))?;
    let http = AlpacaHttpClient::from_env(env)?;

    let extended_hours = !is_regular_hours();
    let run_started = Utc::now();

    banner("1. Reference price");
    let reference = last_trade_price(&http).await?;
    println!("last {SYMBOL} trade on IEX: {reference:.2}");

    banner("2. Trade-updates stream");
    let mut updates = AlpacaTradeUpdatesWebSocketClient::new(
        None,
        env,
        credential,
        TransportBackend::default(),
        None,
    );
    updates.connect().await?;
    println!("connected: active={}", updates.is_active());

    banner("3. BUY leg (marketable limit)");
    let buy = submit_and_await_fill(
        &http,
        &mut updates,
        AlpacaOrderSide::Buy,
        reference * 1.02,
        extended_hours,
    )
    .await?;
    println!(
        "BUY filled: {} @ {:.4} (execution_id={:?}, position_qty={:?})",
        buy.qty, buy.price, buy.execution_id, buy.position_qty,
    );

    banner("4. SELL leg (marketable limit, back to flat)");
    let sell = submit_and_await_fill(
        &http,
        &mut updates,
        AlpacaOrderSide::Sell,
        reference * 0.98,
        extended_hours,
    )
    .await?;
    println!(
        "SELL filled: {} @ {:.4} (execution_id={:?}, position_qty={:?})",
        sell.qty, sell.price, sell.execution_id, sell.position_qty,
    );

    let positions = http.get_positions().await?;
    let open = positions.iter().find(|p| p.symbol.as_str() == SYMBOL);
    println!("post-trade {SYMBOL} position: {:?}", open.map(|p| &p.qty));

    banner("5. Account activities: FILL entries vs stream");
    let after = (run_started - chrono::Duration::minutes(1)).to_rfc3339();
    let fills = http
        .get_account_activities(
            &GetAccountActivitiesParamsBuilder::default()
                .activity_types("FILL")
                .after(after.clone())
                .direction("asc")
                .build()?,
        )
        .await?;
    println!("FILL activities since run start: {}", fills.len());
    let mut booked_sell_notional = 0.0f64;
    let mut booked_sell_shares = 0.0f64;
    for fill in &fills {
        let px: f64 = fill.price.as_deref().unwrap_or("0").parse().unwrap_or(0.0);
        let qty: f64 = fill.qty.as_deref().unwrap_or("0").parse().unwrap_or(0.0);
        println!(
            "  {} {} {} {} @ {} (order {})",
            fill.transaction_time.as_deref().unwrap_or("?"),
            fill.fill_type.as_deref().unwrap_or("?"),
            fill.side.as_deref().unwrap_or("?"),
            fill.qty.as_deref().unwrap_or("?"),
            fill.price.as_deref().unwrap_or("?"),
            fill.order_id.as_deref().unwrap_or("?"),
        );
        if fill.side.as_deref() == Some("sell") {
            booked_sell_notional += px * qty;
            booked_sell_shares += qty;
        }
    }

    banner("6. Fee reconciliation");
    let expected_taf = booked_sell_shares * TAF_PER_SHARE;
    println!("sell leg: {booked_sell_shares} shares, notional ${booked_sell_notional:.2}");
    println!(
        "expected FINRA TAF: ${expected_taf:.6} ({booked_sell_shares} x {TAF_PER_SHARE})",
    );
    println!(
        "expected SEC fee: notional x rate (rate changes periodically; ~$27.80/$1M => ${:.6})",
        booked_sell_notional * 27.80 / 1_000_000.0,
    );

    let mut fee_entries = Vec::new();
    for round in 1..=FEE_POLL_ROUNDS {
        let fees = http
            .get_account_activities(
                &GetAccountActivitiesParamsBuilder::default()
                    .activity_types("FEE")
                    .after(after.clone())
                    .build()?,
            )
            .await?;
        if fees.is_empty() {
            println!("poll {round}/{FEE_POLL_ROUNDS}: no FEE activities yet");
            tokio::time::sleep(Duration::from_secs(FEE_POLL_DELAY_SECS)).await;
        } else {
            fee_entries = fees;
            break;
        }
    }
    if fee_entries.is_empty() {
        // Also check without the time filter in case fees post with an
        // activity date rather than a timestamp inside our window.
        let today = run_started.format("%Y-%m-%d").to_string();
        fee_entries = http
            .get_account_activities(
                &GetAccountActivitiesParamsBuilder::default()
                    .activity_types("FEE")
                    .date(today)
                    .build()?,
            )
            .await?;
    }

    if fee_entries.is_empty() {
        println!(
            "no FEE activities posted yet. Alpaca books SEC/TAF as end-of-day \
             activities (and paper accounts may not simulate them at all) - \
             re-check tomorrow with activity_types=FEE",
        );
    } else {
        for fee in &fee_entries {
            println!(
                "  FEE {} net_amount={} per_share={} qty={} symbol={} description={:?}",
                fee.date.as_deref().unwrap_or("?"),
                fee.net_amount.as_deref().unwrap_or("?"),
                fee.per_share_amount.as_deref().unwrap_or("-"),
                fee.qty.as_deref().unwrap_or("-"),
                fee.symbol.as_deref().unwrap_or("-"),
                fee.description,
            );
        }
    }

    updates.disconnect().await?;

    banner("SUMMARY");
    println!("buy: {} @ {:.4} | sell: {} @ {:.4}", buy.qty, buy.price, sell.qty, sell.price);
    println!(
        "round-trip PnL before fees: ${:.2}",
        (sell.price - buy.price) * sell.qty,
    );
    println!("stream fills matched venue FILL activities: {}", fills.len() >= 2);
    println!("FEE activities observed: {}", fee_entries.len());

    Ok(())
}
