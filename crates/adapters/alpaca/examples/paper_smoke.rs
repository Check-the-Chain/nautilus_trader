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

//! End-to-end paper-account smoke test for the Alpaca adapter.
//!
//! Requires `ALPACA_PAPER_API_KEY` / `ALPACA_PAPER_API_SECRET` (read from the
//! environment or an ancestor `.env`). Exercises, in order:
//!
//! 1. Trading REST: account, assets/instruments, positions, open orders.
//! 2. Historical market data REST: bars, trades, quotes (IEX feed).
//! 3. Market data WebSocket on the `test` feed (`FAKEPACA`, live 24/7):
//!    trades, quotes, and bars subscriptions with resubscribe bookkeeping.
//! 4. Trade-updates WebSocket + order lifecycle: submit a far-from-market
//!    limit order, observe the `new` event, cancel it, observe `canceled`.
//!
//! Run with:
//! `cargo run --example alpaca-paper-smoke -p nautilus-alpaca`

use std::time::{Duration, Instant};

use nautilus_alpaca::{
    common::{credential::Credential, enums::{AlpacaDataFeed, AlpacaEnvironment}},
    http::{
        client::AlpacaHttpClient,
        query::{
            GetOrdersParamsBuilder, GetStockBarsParamsBuilder, GetStockQuotesParamsBuilder,
            GetStockTradesParamsBuilder, PostOrderParamsBuilder,
        },
    },
    websocket::{
        client::{AlpacaTradeUpdatesWebSocketClient, AlpacaWebSocketClient},
        messages::{AlpacaInstrumentInfo, NautilusWsMessage},
    },
};
use nautilus_model::{identifiers::InstrumentId, instruments::Instrument};
use nautilus_network::websocket::TransportBackend;

const SYMBOL: &str = "AAPL";
const TEST_SYMBOL: &str = "FAKEPACA";
const WS_COLLECT_SECS: u64 = 75;
const EVENT_WAIT_SECS: u64 = 20;

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    let env = AlpacaEnvironment::Paper;

    let credential = Credential::resolve(None, None, env)?
        .ok_or_else(|| anyhow::anyhow!("set ALPACA_PAPER_API_KEY and ALPACA_PAPER_API_SECRET"))?;

    // ---------------------------------------------------------------- REST
    banner("1. Trading REST");
    let http = AlpacaHttpClient::from_env(env)?;

    let account = http.get_account().await?;
    println!(
        "account: id={:?} status={:?} currency={:?} cash={:?} equity={:?} buying_power={:?}",
        account.id, account.status, account.currency, account.cash, account.equity,
        account.buying_power,
    );

    let t = Instant::now();
    let instruments = http.request_instruments().await?;
    println!(
        "instruments: {} active tradable us_equity assets in {:?}",
        instruments.len(),
        t.elapsed(),
    );
    let aapl = http
        .get_instrument(&ustr::Ustr::from(SYMBOL))
        .ok_or_else(|| anyhow::anyhow!("{SYMBOL} missing from instrument cache"))?;
    println!(
        "{SYMBOL}: id={} price_precision={} size_precision={}",
        aapl.id(),
        aapl.price_precision(),
        aapl.size_precision(),
    );

    let positions = http.get_positions().await?;
    println!("positions: {}", positions.len());

    let open_orders = http
        .get_orders(&GetOrdersParamsBuilder::default().status("open").build()?)
        .await?;
    println!("open orders: {}", open_orders.len());

    // ---------------------------------------------- Historical market data
    banner("2. Historical market data REST (IEX)");
    let bars = http
        .get_stock_bars(
            &GetStockBarsParamsBuilder::default()
                .symbols(SYMBOL)
                .timeframe("1Min")
                .limit(10u32)
                .feed(AlpacaDataFeed::Iex)
                .build()?,
        )
        .await?;
    let bar_count = bars.bars.get(SYMBOL).map_or(0, Vec::len);
    println!("bars: {bar_count} (sample: {:?})", bars.bars.get(SYMBOL).and_then(|b| b.first()));

    let trades = http
        .get_stock_trades_paginated(
            &GetStockTradesParamsBuilder::default()
                .symbols(SYMBOL)
                .limit(10u32)
                .feed(AlpacaDataFeed::Iex)
                .build()?,
        )
        .await?;
    let trade_count = trades.get(SYMBOL).map_or(0, Vec::len);
    println!("trades: {trade_count}");

    let quotes = http
        .get_stock_quotes_paginated(
            &GetStockQuotesParamsBuilder::default()
                .symbols(SYMBOL)
                .limit(10u32)
                .feed(AlpacaDataFeed::Iex)
                .build()?,
        )
        .await?;
    let quote_count = quotes.get(SYMBOL).map_or(0, Vec::len);
    println!("quotes: {quote_count}");

    // ------------------------------------------------- Market data stream
    banner("3. Market data WebSocket (test feed, FAKEPACA)");
    let mut ws = AlpacaWebSocketClient::new(
        None,
        AlpacaDataFeed::Test,
        credential.clone(),
        TransportBackend::default(),
        None,
    );
    let test_instrument = InstrumentId::from(format!("{TEST_SYMBOL}.ALPACA").as_str());
    ws.initialize_instruments(vec![AlpacaInstrumentInfo {
        instrument_id: test_instrument,
        price_precision: 2,
        size_precision: 0,
    }]);
    ws.connect().await?;
    println!("connected + authenticated: active={}", ws.is_active());

    ws.subscribe_trades(test_instrument).await?;
    ws.subscribe_quotes(test_instrument).await?;
    ws.subscribe_bars(test_instrument).await?;
    println!("subscribed trades/quotes/bars for {TEST_SYMBOL}");

    let (mut n_trades, mut n_quotes, mut n_bars, mut n_acks) = (0u32, 0u32, 0u32, 0u32);
    let deadline = Instant::now() + Duration::from_secs(WS_COLLECT_SECS);
    let mut first_trade_shown = false;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, ws.next_event()).await {
            Ok(Some(NautilusWsMessage::Trades(trades))) => {
                n_trades += u32::try_from(trades.len()).unwrap_or(u32::MAX);
                if !first_trade_shown {
                    println!("first trade tick: {:?}", trades.first());
                    first_trade_shown = true;
                }
            }
            Ok(Some(NautilusWsMessage::Quote(q))) => {
                if n_quotes == 0 {
                    println!("first quote tick: {q:?}");
                }
                n_quotes += 1;
            }
            Ok(Some(NautilusWsMessage::Bar(b))) => {
                if n_bars == 0 {
                    println!("first bar: {b:?}");
                }
                n_bars += 1;
            }
            Ok(Some(NautilusWsMessage::SubscriptionAck(ack))) => {
                n_acks += 1;
                println!("subscription ack: {ack:?}");
            }
            Ok(Some(NautilusWsMessage::Error { code, msg })) => {
                println!("WS ERROR {code:?}: {msg}");
            }
            Ok(Some(other)) => println!("other event: {other:?}"),
            Ok(None) => anyhow::bail!("market data stream ended unexpectedly"),
            Err(_) => break, // collection window elapsed
        }
    }
    println!(
        "collected in {WS_COLLECT_SECS}s: {n_trades} trades, {n_quotes} quotes, {n_bars} bars, {n_acks} acks",
    );
    anyhow::ensure!(
        n_trades + n_quotes + n_bars > 0,
        "no market data received from the test feed",
    );
    ws.disconnect().await?;
    println!("market data stream disconnected");

    // -------------------------------------- Trade updates + order lifecycle
    banner("4. Trade updates stream + order lifecycle");
    let mut updates = AlpacaTradeUpdatesWebSocketClient::new(
        None,
        env,
        credential,
        TransportBackend::default(),
        None,
    );
    updates.connect().await?;
    println!("trade-updates stream connected + listening: active={}", updates.is_active());

    // Far-from-market limit buy: 1 share of AAPL at $1.00 (never fills).
    let submitted = http
        .submit_order(
            &PostOrderParamsBuilder::default()
                .symbol(SYMBOL)
                .qty("1")
                .side(nautilus_alpaca::common::enums::AlpacaOrderSide::Buy)
                .order_type(nautilus_alpaca::common::enums::AlpacaOrderType::Limit)
                .time_in_force(nautilus_alpaca::common::enums::AlpacaTimeInForce::Day)
                .limit_price("1.00")
                .client_order_id(format!(
                    "smoke-{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)?
                        .as_millis(),
                ))
                .build()?,
        )
        .await?;
    let order_id = submitted.id.clone();
    anyhow::ensure!(!order_id.is_empty(), "submitted order has no id");
    println!(
        "submitted: id={order_id} status={:?} limit={:?}",
        submitted.status, submitted.limit_price,
    );

    let mut saw_new = false;
    let mut saw_canceled = false;
    let mut cancel_sent = false;
    let deadline = Instant::now() + Duration::from_secs(EVENT_WAIT_SECS);
    while Instant::now() < deadline && !saw_canceled {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, updates.next_event()).await {
            Ok(Some(NautilusWsMessage::TradeUpdate(update))) => {
                println!(
                    "trade update: event={:?} order_status={:?}",
                    update.event, update.order.status,
                );
                use nautilus_alpaca::common::enums::AlpacaTradeUpdateEvent;
                match update.event {
                    AlpacaTradeUpdateEvent::New | AlpacaTradeUpdateEvent::PendingNew => {
                        saw_new = true;
                        if !cancel_sent {
                            http.cancel_order(&order_id.to_string()).await?;
                            println!("cancel sent for {order_id}");
                            cancel_sent = true;
                        }
                    }
                    AlpacaTradeUpdateEvent::Canceled => saw_canceled = true,
                    _ => {}
                }
            }
            Ok(Some(other)) => println!("other event: {other:?}"),
            Ok(None) => anyhow::bail!("trade-updates stream ended unexpectedly"),
            Err(_) => break,
        }
    }

    if !cancel_sent {
        // No stream event arrived in the window (e.g. venue latency); cancel
        // via REST regardless so the paper account is left clean.
        http.cancel_order(&order_id.to_string()).await?;
        println!("cancel sent for {order_id} (no stream event within {EVENT_WAIT_SECS}s)");
    }

    let final_order = http.get_order(&order_id.to_string()).await?;
    println!("final order status: {:?}", final_order.status);

    updates.disconnect().await?;
    println!("trade-updates stream disconnected");

    banner("SUMMARY");
    println!("REST account/instruments/positions/orders: OK");
    println!("historical bars/trades/quotes: {bar_count}/{trade_count}/{quote_count} rows");
    println!("market data WS: {n_trades} trades, {n_quotes} quotes, {n_bars} bars");
    println!(
        "order lifecycle: submitted + canceled (stream events: new={saw_new}, canceled={saw_canceled}), final status={:?}",
        final_order.status,
    );

    Ok(())
}
