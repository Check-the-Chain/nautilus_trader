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

//! Live Lighter execution probe.
//!
//! This sends real orders. It uses the BTC market by default, the minimum venue size, and
//! immediately cleans up resting orders.
//!
//! Required environment variables:
//! - `LIGHTER_ACCOUNT_INDEX`
//! - `LIGHTER_API_KEY_INDEX`
//! - `LIGHTER_PRIVATE_KEY`
//!
//! Optional environment variables:
//! - `LIGHTER_SIGNER_LIB_PATH`, signer library path
//! - `LIGHTER_ENV`, use `testnet` for testnet, defaults to mainnet
//! - `LIGHTER_MARKET_ID`, defaults to BTC market `1`
//! - `LIGHTER_RUN_TAKER`, defaults to `true`; set `false` to skip the market IOC round-trip
//! - `LIGHTER_CLOSE_POSITION_ONLY`, defaults to `false`; set `true` to IOC-close the current market position

use std::{
    collections::HashMap,
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, bail};
use nautilus_lighter::{
    client::{LighterCancelOrderRequest, LighterSubmitOrderRequest},
    config::{Config, LighterEnvironment},
    http::client::LighterHttpClient,
    models::{
        order::Order,
        order_book::PerpsOrderBookDetail,
        transaction::{RespSendTx, RespSendTxBatch},
    },
    nonce::NonceManagerType,
};

const ORDER_TYPE_LIMIT: i32 = 0;
const ORDER_TYPE_MARKET: i32 = 1;
const TIF_IOC: i32 = 0;
const TIF_GTT: i32 = 1;
const TIF_POST_ONLY: i32 = 2;
const BTC_MARKET_ID: i64 = 1;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let account_index = required_env("LIGHTER_ACCOUNT_INDEX")?
        .parse::<i64>()
        .context("LIGHTER_ACCOUNT_INDEX must be an integer")?;
    let api_key_index = required_env("LIGHTER_API_KEY_INDEX")?
        .parse::<u8>()
        .context("LIGHTER_API_KEY_INDEX must be an integer")?;
    let private_key = required_env("LIGHTER_PRIVATE_KEY")?;
    let market_id = env::var("LIGHTER_MARKET_ID")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(BTC_MARKET_ID);
    let run_taker = env::var("LIGHTER_RUN_TAKER").map_or(true, |value| {
        !matches!(value.to_ascii_lowercase().as_str(), "0" | "false" | "no")
    });

    let environment = match env::var("LIGHTER_ENV")
        .unwrap_or_else(|_| "mainnet".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "testnet" => LighterEnvironment::Testnet,
        _ => LighterEnvironment::Mainnet,
    };

    let mut config = Config::for_environment(environment);
    if let Ok(path) = env::var("LIGHTER_SIGNER_LIB_PATH") {
        config = config.with_signer_lib_path(path);
    }

    let mut api_private_keys = HashMap::new();
    api_private_keys.insert(api_key_index, private_key);
    let http = LighterHttpClient::with_signer(
        config,
        account_index,
        api_private_keys,
        NonceManagerType::Optimistic,
    )?;

    let auth = http.create_auth_token(0, Some(api_key_index)).await?;
    println!("auth: ok");

    let market = load_market(&http, market_id).await?;
    let close_position_only = env::var("LIGHTER_CLOSE_POSITION_ONLY")
        .is_ok_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"));
    let cancel_all_only = env::var("LIGHTER_CANCEL_ALL_ONLY")
        .is_ok_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"));
    if cancel_all_only {
        let cancel_all = parse_send_tx(
            &http
                .cancel_all_orders(0, 0, Some(api_key_index), None)
                .await?,
        )?;
        ensure_ok("cancel_all_orders", &cancel_all)?;
        println!(
            "cancel_all_orders: code={} tx={:?}",
            cancel_all.code, cancel_all.tx_hash
        );
        println!("live_execution_probe: ok");
        return Ok(());
    }
    if close_position_only {
        close_market_position(
            &http,
            account_index,
            market_id,
            &auth,
            &market,
            api_key_index,
        )
        .await?;
        println!("live_execution_probe: ok");
        return Ok(());
    }

    let base_amount = min_base_amount(&market)?;
    let prices = BookPrices::load(&http, market_id).await?;
    let resting_bid = scale_price(prices.best_bid * 0.90, price_precision(&market)) as i32;
    let resting_bid_modified = scale_price(prices.best_bid * 0.89, price_precision(&market)) as i64;
    println!(
        "market: id={} symbol={} min_base={} best_bid={} best_ask={}",
        market_id,
        market.symbol.as_deref().unwrap_or(""),
        base_amount,
        prices.best_bid,
        prices.best_ask,
    );

    let pre_existing_pending = pending_order_count(&http, account_index).await?;
    println!("pre_existing_pending_orders: {pre_existing_pending}");

    let single_client_id = next_client_order_index(1);
    let single = submit_order(
        &http,
        market_id,
        single_client_id,
        base_amount,
        resting_bid,
        false,
        ORDER_TYPE_LIMIT,
        TIF_POST_ONLY,
        false,
        api_key_index,
    )
    .await?;
    println!(
        "submit_post_only: code={} tx={:?}",
        single.code, single.tx_hash
    );
    let single_order =
        wait_active_order(&http, account_index, market_id, &auth, single_client_id).await?;
    println!(
        "post_only_active: order_index={} status={}",
        single_order.order_index, single_order.status
    );

    let modify = parse_send_tx(
        &http
            .modify_order(
                market_id as i32,
                single_order.order_index,
                base_amount,
                resting_bid_modified,
                0,
                Some(api_key_index),
                None,
            )
            .await?,
    )?;
    ensure_ok("modify_order", &modify)?;
    println!("modify_order: code={} tx={:?}", modify.code, modify.tx_hash);
    let modified =
        wait_active_order(&http, account_index, market_id, &auth, single_client_id).await?;
    println!(
        "modified_active: order_index={} price={} status={}",
        modified.order_index, modified.price, modified.status
    );

    let cancel = parse_send_tx(
        &http
            .cancel_order(
                market_id as i32,
                single_order.order_index,
                Some(api_key_index),
                None,
            )
            .await?,
    )?;
    ensure_ok("cancel_order", &cancel)?;
    wait_order_not_active(&http, account_index, market_id, &auth, single_client_id).await?;
    println!("cancel_order: code={} tx={:?}", cancel.code, cancel.tx_hash);

    let batch_ids = [next_client_order_index(2), next_client_order_index(3)];
    let batch_requests = batch_ids
        .iter()
        .enumerate()
        .map(|(idx, client_order_index)| LighterSubmitOrderRequest {
            market_index: market_id as i32,
            client_order_index: *client_order_index,
            base_amount,
            price: scale_price(
                prices.best_bid * (0.88 - idx as f64 * 0.01),
                price_precision(&market),
            ) as i32,
            is_ask: false,
            order_type: ORDER_TYPE_LIMIT,
            time_in_force: TIF_POST_ONLY,
            reduce_only: false,
            trigger_price: 0,
            order_expiry: now_ms() + 10 * 60 * 1000,
            api_key_index: Some(api_key_index),
        })
        .collect::<Vec<_>>();
    let batch = parse_send_tx_batch(&http.submit_order_batch(batch_requests).await?)?;
    ensure_batch_ok("submit_order_batch", &batch)?;
    println!(
        "submit_order_batch: code={} tx_count={}",
        batch.code,
        batch.tx_hash.as_ref().map_or(0, Vec::len)
    );
    let mut batch_orders = Vec::new();
    for client_id in batch_ids {
        batch_orders
            .push(wait_active_order(&http, account_index, market_id, &auth, client_id).await?);
    }
    let cancel_batch_requests = batch_orders
        .iter()
        .map(|order| LighterCancelOrderRequest {
            market_index: market_id as i32,
            order_index: order.order_index,
            api_key_index: Some(api_key_index),
        })
        .collect::<Vec<_>>();
    let cancel_batch = parse_send_tx_batch(&http.cancel_order_batch(cancel_batch_requests).await?)?;
    ensure_batch_ok("cancel_order_batch", &cancel_batch)?;
    for client_id in batch_ids {
        wait_order_not_active(&http, account_index, market_id, &auth, client_id).await?;
    }
    println!(
        "cancel_order_batch: code={} tx_count={}",
        cancel_batch.code,
        cancel_batch.tx_hash.as_ref().map_or(0, Vec::len)
    );

    if pre_existing_pending == 0 {
        let cancel_all_client_id = next_client_order_index(4);
        submit_order(
            &http,
            market_id,
            cancel_all_client_id,
            base_amount,
            scale_price(prices.best_bid * 0.87, price_precision(&market)) as i32,
            false,
            ORDER_TYPE_LIMIT,
            TIF_POST_ONLY,
            false,
            api_key_index,
        )
        .await?;
        wait_active_order(&http, account_index, market_id, &auth, cancel_all_client_id).await?;
        let cancel_all = parse_send_tx(
            &http
                .cancel_all_orders(0, 0, Some(api_key_index), None)
                .await?,
        )?;
        ensure_ok("cancel_all_orders", &cancel_all)?;
        wait_order_not_active(&http, account_index, market_id, &auth, cancel_all_client_id).await?;
        println!(
            "cancel_all_orders: code={} tx={:?}",
            cancel_all.code, cancel_all.tx_hash
        );
    } else {
        println!("cancel_all_orders: skipped_existing_pending_orders");
    }

    if run_taker {
        run_market_ioc_round_trip(
            &http,
            account_index,
            market_id,
            &auth,
            &market,
            base_amount,
            api_key_index,
        )
        .await?;
    } else {
        println!("market_ioc_round_trip: skipped");
    }

    println!("live_execution_probe: ok");
    Ok(())
}

async fn close_market_position(
    http: &LighterHttpClient,
    account_index: i64,
    market_id: i64,
    auth: &str,
    market: &PerpsOrderBookDetail,
    api_key_index: u8,
) -> anyhow::Result<()> {
    let before = signed_position(http, account_index, market_id, auth).await?;
    if before.abs() < min_size_as_float(market) / 2.0 {
        println!("close_position: skipped flat before={before}");
        return Ok(());
    }

    let prices = BookPrices::load(http, market_id).await?;
    let is_ask = before > 0.0;
    let price = if is_ask {
        scale_price(prices.best_bid * 0.99, price_precision(market)) as i32
    } else {
        scale_price(prices.best_ask * 1.01, price_precision(market)) as i32
    };
    let amount = scale_size(before.abs(), size_precision(market));
    let response = submit_order(
        http,
        market_id,
        next_client_order_index(9),
        amount,
        price,
        is_ask,
        ORDER_TYPE_MARKET,
        TIF_IOC,
        false,
        api_key_index,
    )
    .await?;
    println!(
        "close_position_ioc: code={} tx={:?}",
        response.code, response.tx_hash
    );
    tokio::time::sleep(Duration::from_millis(800)).await;
    let after = signed_position(http, account_index, market_id, auth).await?;
    println!("close_position: before={before} after={after}");
    Ok(())
}

async fn run_market_ioc_round_trip(
    http: &LighterHttpClient,
    account_index: i64,
    market_id: i64,
    auth: &str,
    market: &PerpsOrderBookDetail,
    base_amount: i64,
    api_key_index: u8,
) -> anyhow::Result<()> {
    let before = signed_position(http, account_index, market_id, auth).await?;
    let prices = BookPrices::load(http, market_id).await?;
    let buy_client_id = next_client_order_index(5);
    let buy = submit_order(
        http,
        market_id,
        buy_client_id,
        base_amount,
        scale_price(prices.best_ask * 1.01, price_precision(market)) as i32,
        false,
        ORDER_TYPE_MARKET,
        TIF_IOC,
        false,
        api_key_index,
    )
    .await?;
    println!("market_ioc_buy: code={} tx={:?}", buy.code, buy.tx_hash);
    tokio::time::sleep(Duration::from_millis(800)).await;
    let after_buy = signed_position(http, account_index, market_id, auth).await?;
    let filled_delta = after_buy - before;
    if filled_delta.abs() < min_size_as_float(market) / 2.0 {
        println!("market_ioc_buy_fill: none_or_tiny delta={filled_delta}");
        return Ok(());
    }

    let sell_amount = scale_size(filled_delta.abs(), size_precision(market));
    let prices = BookPrices::load(http, market_id).await?;
    let sell_client_id = next_client_order_index(6);
    let sell = submit_order(
        http,
        market_id,
        sell_client_id,
        sell_amount,
        scale_price(prices.best_bid * 0.99, price_precision(market)) as i32,
        true,
        ORDER_TYPE_MARKET,
        TIF_IOC,
        false,
        api_key_index,
    )
    .await?;
    println!("market_ioc_sell: code={} tx={:?}", sell.code, sell.tx_hash);
    tokio::time::sleep(Duration::from_millis(800)).await;
    let after_sell = signed_position(http, account_index, market_id, auth).await?;
    println!(
        "market_ioc_round_trip: before={before} after_buy={after_buy} after_sell={after_sell}"
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn submit_order(
    http: &LighterHttpClient,
    market_id: i64,
    client_order_index: i64,
    base_amount: i64,
    price: i32,
    is_ask: bool,
    order_type: i32,
    time_in_force: i32,
    reduce_only: bool,
    api_key_index: u8,
) -> anyhow::Result<RespSendTx> {
    let response = parse_send_tx(
        &http
            .submit_order(
                market_id as i32,
                client_order_index,
                base_amount,
                price,
                is_ask,
                order_type,
                time_in_force,
                reduce_only,
                0,
                if time_in_force == TIF_GTT || time_in_force == TIF_POST_ONLY {
                    now_ms() + 10 * 60 * 1000
                } else {
                    0
                },
                Some(api_key_index),
                None,
            )
            .await?,
    )?;
    ensure_ok("submit_order", &response)?;
    Ok(response)
}

async fn load_market(
    http: &LighterHttpClient,
    market_id: i64,
) -> anyhow::Result<PerpsOrderBookDetail> {
    let details = http.rest().get_order_book_details(market_id).await?;
    details
        .order_book_details
        .into_iter()
        .find(|detail| detail.market_id == Some(market_id))
        .with_context(|| format!("market {market_id} not found"))
}

struct BookPrices {
    best_bid: f64,
    best_ask: f64,
}

impl BookPrices {
    async fn load(http: &LighterHttpClient, market_id: i64) -> anyhow::Result<Self> {
        let book = http.rest().get_order_book_orders(market_id, 1).await?;
        let best_bid = book
            .bids
            .first()
            .and_then(|order| order.price_f64())
            .context("missing best bid")?;
        let best_ask = book
            .asks
            .first()
            .and_then(|order| order.price_f64())
            .context("missing best ask")?;
        Ok(Self { best_bid, best_ask })
    }
}

async fn wait_active_order(
    http: &LighterHttpClient,
    account_index: i64,
    market_id: i64,
    auth: &str,
    client_order_index: i64,
) -> anyhow::Result<Order> {
    for _ in 0..30 {
        let orders = http
            .rest()
            .get_account_active_orders(account_index, market_id, auth)
            .await?;
        if let Some(order) = orders
            .orders
            .into_iter()
            .find(|order| order.client_order_index == client_order_index)
        {
            return Ok(order);
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    bail!("active order {client_order_index} not found");
}

async fn wait_order_not_active(
    http: &LighterHttpClient,
    account_index: i64,
    market_id: i64,
    auth: &str,
    client_order_index: i64,
) -> anyhow::Result<()> {
    for _ in 0..30 {
        let active = http
            .rest()
            .get_account_active_orders(account_index, market_id, auth)
            .await?;
        if active
            .orders
            .iter()
            .all(|order| order.client_order_index != client_order_index)
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    bail!("order {client_order_index} remained active");
}

async fn pending_order_count(http: &LighterHttpClient, account_index: i64) -> anyhow::Result<i64> {
    let account = http.rest().get_account_by_index(account_index).await?;
    Ok(account
        .accounts
        .first()
        .and_then(|account| account.pending_order_count)
        .unwrap_or_default())
}

async fn signed_position(
    http: &LighterHttpClient,
    account_index: i64,
    market_id: i64,
    auth: &str,
) -> anyhow::Result<f64> {
    let account = http
        .rest()
        .get_detailed_account_by_index(account_index, auth)
        .await?;
    let position = account
        .accounts
        .first()
        .and_then(|account| account.positions.as_ref())
        .and_then(|positions| {
            positions
                .iter()
                .find(|position| position.market_id == market_id)
        })
        .map_or(0.0, |position| position.signed_position());
    Ok(position)
}

fn parse_send_tx(response: &str) -> anyhow::Result<RespSendTx> {
    serde_json::from_str(response).context("failed to parse send_tx response")
}

fn parse_send_tx_batch(response: &str) -> anyhow::Result<RespSendTxBatch> {
    serde_json::from_str(response).context("failed to parse send_tx_batch response")
}

fn ensure_ok(context: &str, response: &RespSendTx) -> anyhow::Result<()> {
    if response.code == 200 {
        Ok(())
    } else {
        bail!(
            "{context} failed: code={} message={}",
            response.code,
            response.message.as_deref().unwrap_or("")
        )
    }
}

fn ensure_batch_ok(context: &str, response: &RespSendTxBatch) -> anyhow::Result<()> {
    if response.code == 200 {
        Ok(())
    } else {
        bail!(
            "{context} failed: code={} message={}",
            response.code,
            response.message.as_deref().unwrap_or("")
        )
    }
}

fn required_env(name: &str) -> anyhow::Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
}

fn next_client_order_index(sequence: i64) -> i64 {
    now_ms() * 10 + sequence
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_millis() as i64
}

fn price_precision(market: &PerpsOrderBookDetail) -> u8 {
    market
        .price_decimals
        .or(market.supported_price_decimals)
        .unwrap_or(0) as u8
}

fn size_precision(market: &PerpsOrderBookDetail) -> u8 {
    market
        .size_decimals
        .or(market.supported_size_decimals)
        .unwrap_or(0) as u8
}

fn min_size_as_float(market: &PerpsOrderBookDetail) -> f64 {
    market.min_base_amount.unwrap_or(0.0)
}

fn min_base_amount(market: &PerpsOrderBookDetail) -> anyhow::Result<i64> {
    let min = market.min_base_amount.context("missing min_base_amount")?;
    let scaled = scale_size(min, size_precision(market));
    if scaled <= 0 {
        bail!("invalid min_base_amount {min}");
    }
    Ok(scaled)
}

fn scale_price(value: f64, precision: u8) -> i64 {
    let factor = 10_i64.pow(u32::from(precision)) as f64;
    (value * factor).round() as i64
}

fn scale_size(value: f64, precision: u8) -> i64 {
    let factor = 10_i64.pow(u32::from(precision)) as f64;
    (value * factor).round() as i64
}
