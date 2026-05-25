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

//! Read-only Lighter private feed probe.
//!
//! Run with:
//! `LIGHTER_ACCOUNT_INDEX=... LIGHTER_API_KEY_INDEX=... LIGHTER_PRIVATE_KEY=... cargo run -p nautilus-lighter --example lighter-private-readonly-probe --features examples`
//!
//! Optional environment variables:
//! - `LIGHTER_ENV`, use `testnet` for testnet, defaults to mainnet
//! - `LIGHTER_BASE_URL_HTTP` / `LIGHTER_BASE_URL_WS`, for endpoint overrides
//! - `LIGHTER_SIGNER_LIB_PATH`, signer library path
//! - `LIGHTER_PROBE_DURATION_SECS`, defaults to 10

use std::{
    collections::HashMap,
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use nautilus_lighter::{
    common::LIGHTER_FEE_SCALE,
    config::{Config, LighterEnvironment, lighter_ws_base_url_for_environment},
    http::client::LighterHttpClient,
    models::ws::{WsAccountAllUpdate, WsMessage},
    nonce::NonceManagerType,
    websocket::client::LighterWebSocketClient,
};
use serde_json::Value;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let account_index = required_env("LIGHTER_ACCOUNT_INDEX")?
        .parse::<i64>()
        .context("LIGHTER_ACCOUNT_INDEX must be an integer")?;
    let api_key_index = required_env("LIGHTER_API_KEY_INDEX")?
        .parse::<u8>()
        .context("LIGHTER_API_KEY_INDEX must be an integer")?;
    let private_key = required_env("LIGHTER_PRIVATE_KEY")?;
    let duration = Duration::from_secs(
        env::var("LIGHTER_PROBE_DURATION_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(10),
    );

    let environment = match env::var("LIGHTER_ENV")
        .unwrap_or_else(|_| "mainnet".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "testnet" => LighterEnvironment::Testnet,
        _ => LighterEnvironment::Mainnet,
    };

    let mut config = Config::for_environment(environment);
    if let Ok(url) = env::var("LIGHTER_BASE_URL_HTTP") {
        config = config.with_http_base_url(url);
    }
    if let Ok(url) = env::var("LIGHTER_BASE_URL_WS") {
        config = config.with_ws_base_url(url);
    }
    if let Ok(path) = env::var("LIGHTER_SIGNER_LIB_PATH") {
        config = config.with_signer_lib_path(path);
    }

    let mut api_private_keys = HashMap::new();
    api_private_keys.insert(api_key_index, private_key);
    let http = LighterHttpClient::with_signer(
        config.clone(),
        account_index,
        api_private_keys,
        NonceManagerType::Optimistic,
    )?;

    let token_deadline = SystemTime::now()
        .checked_add(Duration::from_secs(300))
        .context("auth token deadline overflow")?
        .duration_since(UNIX_EPOCH)?
        .as_secs() as i64;
    let auth_token = http
        .create_auth_token(token_deadline, Some(api_key_index))
        .await?;
    println!("auth: ok");

    let account = http
        .rest()
        .get_detailed_account_by_index(account_index, &auth_token)
        .await?;
    let first = account.accounts.first();
    println!(
        "rest_account: accounts={} account_type={:?} assets={} positions={}",
        account.accounts.len(),
        first.and_then(|account| account.account_type),
        first
            .and_then(|account| account.assets.as_ref())
            .map_or(0, Vec::len),
        first
            .and_then(|account| account.positions.as_ref())
            .map_or(0, Vec::len),
    );

    let limits = http
        .rest()
        .get_account_limits(account_index, &auth_token)
        .await?;
    println!(
        "account_limits: user_tier={:?} user_tier_name={:?} maker_fee_tick={:?} taker_fee_tick={:?} maker_fee_rate={:?} taker_fee_rate={:?}",
        limits.user_tier,
        limits.user_tier_name,
        limits.current_maker_fee_tick,
        limits.current_taker_fee_tick,
        limits
            .current_maker_fee_tick
            .map(|tick| tick as f64 / LIGHTER_FEE_SCALE as f64),
        limits
            .current_taker_fee_tick
            .map(|tick| tick as f64 / LIGHTER_FEE_SCALE as f64),
    );

    let ws_url = env::var("LIGHTER_BASE_URL_WS").unwrap_or_else(|_| {
        format!(
            "{}?readonly=true",
            lighter_ws_base_url_for_environment(environment)
        )
    });
    let ws = LighterWebSocketClient::new(ws_url, Some(auth_token));
    ws.connect().await?;
    ws.subscribe(format!("account_all/{account_index}"), None)
        .await?;
    ws.subscribe(format!("account_all_positions/{account_index}"), None)
        .await?;
    ws.subscribe(format!("account_all_assets/{account_index}"), None)
        .await?;
    ws.subscribe(format!("account_all_orders/{account_index}"), None)
        .await?;
    ws.subscribe(format!("account_all_trades/{account_index}"), None)
        .await?;
    ws.subscribe(format!("user_stats/{account_index}"), None)
        .await?;

    let deadline = tokio::time::Instant::now() + duration;
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut account_all_snapshot = None;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }

        let message = match tokio::time::timeout(remaining, ws.next_message()).await {
            Ok(Some(message)) => message,
            Ok(None) | Err(_) => break,
        };
        let header: WsMessage = match serde_json::from_str(&message) {
            Ok(header) => header,
            Err(_) => continue,
        };

        *counts.entry(header.msg_type.clone()).or_default() += 1;
        if account_all_snapshot.is_none()
            && matches!(
                header.msg_type.as_str(),
                "subscribed/account_all" | "update/account_all"
            )
            && let Ok(payload) = serde_json::from_str::<WsAccountAllUpdate>(&message)
        {
            let position_count = payload.positions.values().map(Vec::len).sum::<usize>();
            account_all_snapshot = Some((
                header.msg_type.clone(),
                payload.assets.len(),
                position_count,
            ));
        }

        if header.msg_type.starts_with("subscribed/") {
            println!(
                "ws_subscribed: type={} channel={}",
                header.msg_type,
                header.channel.as_deref().unwrap_or("")
            );
        } else if header.msg_type.starts_with("update/") {
            print_update_summary(&header, &message);
        }
    }

    if let Some((msg_type, assets, positions)) = account_all_snapshot {
        println!("account_all_snapshot: type={msg_type} assets={assets} positions={positions}");
    } else {
        println!("account_all_snapshot: missing");
    }
    println!("ws_counts: {}", serde_json::to_string(&counts)?);

    ws.close().await?;
    Ok(())
}

fn required_env(name: &str) -> anyhow::Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
}

fn print_update_summary(header: &WsMessage, raw: &str) {
    let value: Value = match serde_json::from_str(raw) {
        Ok(value) => value,
        Err(_) => return,
    };
    println!(
        "ws_update: type={} channel={} keys={}",
        header.msg_type,
        header.channel.as_deref().unwrap_or(""),
        value.as_object().map_or(0, serde_json::Map::len),
    );
}
