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

use alloy::{
    dyn_abi::SolType,
    primitives::{Address, U160},
    sol,
};
use nautilus_model::defi::{
    PoolIdentifier,
    rpc::RpcLog,
    tick_map::tick_math::{MAX_SQRT_RATIO, MIN_SQRT_RATIO, get_tick_at_sqrt_ratio},
};

use crate::{
    events::pool_created::PoolCreatedEvent,
    hypersync::{
        HypersyncLog,
        helpers::{extract_block_number, validate_event_signature_hash},
    },
    rpc::helpers as rpc_helpers,
};

const INITIALIZE_EVENT_SIGNATURE_HASH: &str =
    "dd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438";

fn parse_indexed_currency(topic: &[u8], name: &str) -> anyhow::Result<Address> {
    anyhow::ensure!(
        topic.len() == 32,
        "{name} topic must be 32 bytes, was {}",
        topic.len()
    );
    anyhow::ensure!(
        topic[..12].iter().all(|byte| *byte == 0),
        "{name} topic has non-zero address padding"
    );
    Ok(Address::from_slice(&topic[12..]))
}

fn validate_initialize_params(sqrt_price_x96: U160, tick: i32) -> anyhow::Result<()> {
    anyhow::ensure!(
        sqrt_price_x96 >= MIN_SQRT_RATIO && sqrt_price_x96 < MAX_SQRT_RATIO,
        "Initialize sqrt price is out of bounds: {sqrt_price_x96}"
    );
    let calculated_tick = get_tick_at_sqrt_ratio(sqrt_price_x96);
    anyhow::ensure!(
        tick == calculated_tick,
        "Initialize tick {tick} does not match tick {calculated_tick} calculated from sqrt price"
    );
    Ok(())
}

// Define sol macro for parsing Initialize event data
// Topics contain: [signature, poolId, currency0, currency1]
// Data contains 5 parameters: fee, tickSpacing, hooks, sqrtPriceX96, tick
sol! {
    struct InitializeEventData {
        uint24 fee;
        int24 tick_spacing;
        address hooks;
        uint160 sqrtPriceX96;
        int24 tick;
    }
}

/// Parses a UniswapV4 Initialize event from a HyperSync log.
///
/// UniswapV4 uses the Initialize event for pool discovery (no separate PoolCreated event).
/// The PoolManager is a singleton contract that manages all V4 pools.
///
/// Initialize event signature:
/// ```solidity
/// event Initialize(
///     PoolId indexed id,          // bytes32 (topic1)
///     Currency indexed currency0, // address (topic2)
///     Currency indexed currency1, // address (topic3)
///     uint24 fee,                // (data)
///     int24 tickSpacing,         // (data)
///     IHooks hooks,              // address (data)
///     uint160 sqrtPriceX96,      // (data)
///     int24 tick                 // (data)
/// );
/// ```
///
/// # Errors
///
/// Returns an error if the log parsing fails or if the event data is invalid.
///
pub fn parse_initialize_event_hypersync(log: HypersyncLog) -> anyhow::Result<PoolCreatedEvent> {
    validate_event_signature_hash("InitializeEvent", INITIALIZE_EVENT_SIGNATURE_HASH, &log)?;

    let block_number = extract_block_number(&log)?;

    // The pool address for V4 is the PoolManager contract address (the event emitter)
    let pool_manager_bytes = log
        .address
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing PoolManager address"))?
        .as_ref();
    anyhow::ensure!(
        pool_manager_bytes.len() == Address::len_bytes(),
        "PoolManager address must be 20 bytes, was {}",
        pool_manager_bytes.len()
    );
    let pool_manager_address = Address::from_slice(pool_manager_bytes);

    // Extract currency0 and currency1 from topics
    // topics[0] = event signature
    // topics[1] = poolId (bytes32)
    // topics[2] = currency0 (indexed)
    // topics[3] = currency1 (indexed)
    let topics = &log.topics;
    if topics.len() < 4 {
        anyhow::bail!(
            "Initialize event missing topics: expected 4, was {}",
            topics.len()
        );
    }

    // Extract Pool ID from topics[1] - this is the unique identifier for V4 pools
    let pool_id_bytes = topics[1]
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing poolId topic"))?
        .as_ref();
    let pool_identifier = PoolIdentifier::from_pool_id_bytes(pool_id_bytes)?;

    let currency0 = parse_indexed_currency(
        topics[2]
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Missing currency0 topic"))?
            .as_ref(),
        "Currency0",
    )?;

    let currency1 = parse_indexed_currency(
        topics[3]
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Missing currency1 topic"))?
            .as_ref(),
        "Currency1",
    )?;

    if let Some(data) = log.data {
        let data_bytes = data.as_ref();

        // Validate minimum data length (5 fields × 32 bytes = 160 bytes)
        if data_bytes.len() < 160 {
            anyhow::bail!(
                "Initialize event data too short: expected at least 160 bytes, was {}",
                data_bytes.len()
            );
        }

        let decoded = <InitializeEventData as SolType>::abi_decode(data_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to decode initialize event data: {e}"))?;

        let tick_spacing = u32::try_from(i32::try_from(decoded.tick_spacing)?)
            .map_err(|_| anyhow::anyhow!("Tick spacing must be positive"))?;
        anyhow::ensure!(tick_spacing > 0, "Tick spacing must be positive");
        let tick = i32::try_from(decoded.tick)?;
        validate_initialize_params(decoded.sqrtPriceX96, tick)?;
        let mut event = PoolCreatedEvent::new(
            block_number,
            currency0,
            currency1,
            pool_manager_address, // V4 pools are managed by PoolManager
            pool_identifier,
            Some(decoded.fee.to::<u32>()),
            Some(tick_spacing),
        );

        event.set_initialize_params(decoded.sqrtPriceX96, tick);
        event.set_hooks(decoded.hooks);

        Ok(event)
    } else {
        Err(anyhow::anyhow!("Missing data in initialize event log"))
    }
}

/// Parses a UniswapV4 Initialize event from an RPC log.
///
/// # Errors
///
/// Returns an error if the log parsing fails or if the event data is invalid.
pub fn parse_initialize_event_rpc(log: &RpcLog) -> anyhow::Result<PoolCreatedEvent> {
    rpc_helpers::validate_event_signature(log, INITIALIZE_EVENT_SIGNATURE_HASH, "InitializeEvent")?;

    let block_number = rpc_helpers::extract_block_number(log)?;

    // Pool address is the PoolManager contract (event emitter)
    let pool_manager_bytes = rpc_helpers::decode_hex(&log.address)?;
    anyhow::ensure!(
        pool_manager_bytes.len() == Address::len_bytes(),
        "PoolManager address must be 20 bytes, was {}",
        pool_manager_bytes.len()
    );
    let pool_manager_address = Address::from_slice(&pool_manager_bytes);

    // Extract currency0 and currency1 from topics
    // topics[0] = event signature
    // topics[1] = poolId (bytes32)
    // topics[2] = currency0 (indexed)
    // topics[3] = currency1 (indexed)
    if log.topics.len() < 4 {
        anyhow::bail!(
            "Initialize event missing topics: expected 4, was {}",
            log.topics.len()
        );
    }

    // Extract Pool ID from topics[1] - this is the unique identifier for V4 pools
    let pool_id_bytes = rpc_helpers::decode_hex(&log.topics[1])?;
    let pool_identifier = PoolIdentifier::from_pool_id_bytes(&pool_id_bytes)?;

    let currency0_bytes = rpc_helpers::decode_hex(&log.topics[2])?;
    let currency0 = parse_indexed_currency(&currency0_bytes, "Currency0")?;

    let currency1_bytes = rpc_helpers::decode_hex(&log.topics[3])?;
    let currency1 = parse_indexed_currency(&currency1_bytes, "Currency1")?;

    // Extract and decode event data
    let data_bytes = rpc_helpers::extract_data_bytes(log)?;

    // Validate minimum data length (5 fields × 32 bytes = 160 bytes)
    if data_bytes.len() < 160 {
        anyhow::bail!(
            "Initialize event data too short: expected at least 160 bytes, was {}",
            data_bytes.len()
        );
    }

    let decoded = <InitializeEventData as SolType>::abi_decode(&data_bytes)
        .map_err(|e| anyhow::anyhow!("Failed to decode initialize event data: {e}"))?;

    let tick_spacing = u32::try_from(i32::try_from(decoded.tick_spacing)?)
        .map_err(|_| anyhow::anyhow!("Tick spacing must be positive"))?;
    anyhow::ensure!(tick_spacing > 0, "Tick spacing must be positive");
    let tick = i32::try_from(decoded.tick)?;
    validate_initialize_params(decoded.sqrtPriceX96, tick)?;
    let mut event = PoolCreatedEvent::new(
        block_number,
        currency0,
        currency1,
        pool_manager_address,
        pool_identifier,
        Some(decoded.fee.to::<u32>()),
        Some(tick_spacing),
    );

    event.set_initialize_params(decoded.sqrtPriceX96, tick);
    event.set_hooks(decoded.hooks);

    Ok(event)
}

#[cfg(test)]
mod tests {
    use nautilus_core::hex;
    use rstest::{fixture, rstest};
    use serde_json::json;

    use super::*;

    fn replace_data_word(log: &mut RpcLog, index: usize, word: [u8; 32]) {
        let mut data = rpc_helpers::decode_hex(&log.data).expect("valid fixture data");
        data[index * 32..(index + 1) * 32].copy_from_slice(&word);
        log.data = hex::encode_prefixed(data);
    }

    // Real UniswapV4 Initialize event from Arbitrum
    // Pool Manager: 0x360E68faCcca8cA495c1B759Fd9EEe466db9FB32
    // WETH-USDC pool
    // Block: 0x11c44853 (297879635)
    // Tx: 0xdb973062b20333d61a57f4dc14b33c044e044a97c7d3db2900acc61e04179738

    #[fixture]
    fn hypersync_log_weth_usdc() -> HypersyncLog {
        let log_json = json!({
            "removed": null,
            "log_index": "0x1",
            "transaction_index": "0x3",
            "transaction_hash": "0xdb973062b20333d61a57f4dc14b33c044e044a97c7d3db2900acc61e04179738",
            "block_hash": null,
            "block_number": "0x11c44853",
            "address": "0x360e68faccca8ca495c1b759fd9eee466db9fb32",
            "data": "0x0000000000000000000000000000000000000000000000000000000000000bb8000000000000000000000000000000000000000000000000000000000000003c000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003e08ab0dd488513a6f62efffffffffffffffffffffffffffffffffffffffffffffffffffffffffffd0765",
            "topics": [
                "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438",
                "0xc9bc8043294146424a4e4607d8ad837d6a659142822bbaaabc83bb57e7447461",
                "0x00000000000000000000000082af49447d8a07e3bd95bd0d56f35241523fbab1",
                "0x000000000000000000000000af88d065e77c8cc2239327c5edb3a432268e5831"
            ]
        });
        serde_json::from_value(log_json).expect("Failed to deserialize HyperSync log")
    }

    #[fixture]
    fn rpc_log_weth_usdc() -> RpcLog {
        let log_json = json!({
            "removed": false,
            "logIndex": "0x1",
            "transactionIndex": "0x3",
            "transactionHash": "0xdb973062b20333d61a57f4dc14b33c044e044a97c7d3db2900acc61e04179738",
            "blockHash": "0x4f72d534028d2322fa2dcaa3f470467a264eda2e20f73eeb1ece370361bb0ee7",
            "blockNumber": "0x11c44853",
            "address": "0x360e68faccca8ca495c1b759fd9eee466db9fb32",
            "data": "0x0000000000000000000000000000000000000000000000000000000000000bb8000000000000000000000000000000000000000000000000000000000000003c000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003e08ab0dd488513a6f62efffffffffffffffffffffffffffffffffffffffffffffffffffffffffffd0765",
            "topics": [
                "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438",
                "0xc9bc8043294146424a4e4607d8ad837d6a659142822bbaaabc83bb57e7447461",
                "0x00000000000000000000000082af49447d8a07e3bd95bd0d56f35241523fbab1",
                "0x000000000000000000000000af88d065e77c8cc2239327c5edb3a432268e5831"
            ]
        });
        serde_json::from_value(log_json).expect("Failed to deserialize RPC log")
    }

    #[fixture]
    fn rpc_log_robinhood_pool() -> RpcLog {
        let log_json = json!({
            "removed": false,
            "logIndex": "0x0",
            "transactionIndex": "0x1",
            "transactionHash": "0x9ac26a1db2e4b2322a84c688bb31bebb437a2a2587cdeb51d7b6052662ee1ebf",
            "blockHash": "0x00",
            "blockNumber": "0x2521",
            "address": "0x8366a39cc670b4001a1121b8f6a443a643e40951",
            "data": "0x0000000000000000000000000000000000000000000000000000000000000bb8000000000000000000000000000000000000000000000000000000000000003c000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "topics": [
                "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438",
                "0xdb2c20421239d46bb30a7a73029b7f9b7f166489bfb972057d33cbd7249413a5",
                "0x0000000000000000000000000bd7d308f8e1639fab988df18a8011f41eacad73",
                "0x00000000000000000000000042bcdf8d4116545d04dd5b76f48b614450f18b1b"
            ]
        });
        serde_json::from_value(log_json).expect("Failed to deserialize Robinhood RPC log")
    }

    #[rstest]
    fn test_parse_initialize_hypersync(hypersync_log_weth_usdc: HypersyncLog) {
        let event =
            parse_initialize_event_hypersync(hypersync_log_weth_usdc).expect("Failed to parse");

        assert_eq!(event.block_number, 298076243);
        assert_eq!(
            event.token0.to_string().to_lowercase(),
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
        );
        assert_eq!(
            event.token1.to_string().to_lowercase(),
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
        );
        assert_eq!(
            event.pool_identifier.to_string(),
            "0xc9bc8043294146424a4e4607d8ad837d6a659142822bbaaabc83bb57e7447461"
        );
        assert_eq!(event.fee, Some(3000));
        assert_eq!(event.tick_spacing, Some(60));
    }

    #[rstest]
    fn test_parse_initialize_rpc(rpc_log_weth_usdc: RpcLog) {
        let event = parse_initialize_event_rpc(&rpc_log_weth_usdc).expect("Failed to parse");

        assert_eq!(event.block_number, 298076243);
        assert_eq!(
            event.token0.to_string().to_lowercase(),
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
        );
        assert_eq!(
            event.token1.to_string().to_lowercase(),
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
        );
        assert_eq!(
            event.pool_identifier.to_string(),
            "0xc9bc8043294146424a4e4607d8ad837d6a659142822bbaaabc83bb57e7447461"
        );
        assert_eq!(event.fee, Some(3000));
        assert_eq!(event.tick_spacing, Some(60));
    }

    #[rstest]
    fn test_parse_robinhood_initialize_rpc(rpc_log_robinhood_pool: RpcLog) {
        let event = parse_initialize_event_rpc(&rpc_log_robinhood_pool).expect("Failed to parse");

        assert_eq!(event.block_number, 9505);
        assert_eq!(
            event.pool_address.to_string().to_lowercase(),
            "0x8366a39cc670b4001a1121b8f6a443a643e40951"
        );
        assert_eq!(
            event.pool_identifier.to_string(),
            "0xdb2c20421239d46bb30a7a73029b7f9b7f166489bfb972057d33cbd7249413a5"
        );
        assert_eq!(event.fee, Some(3000));
        assert_eq!(event.tick_spacing, Some(60));
        assert_eq!(event.hooks, Some(Address::ZERO));
        assert_eq!(event.tick, Some(0));
    }

    #[rstest]
    fn test_parse_initialize_rpc_rejects_short_currency_topic(mut rpc_log_robinhood_pool: RpcLog) {
        rpc_log_robinhood_pool.topics[2] = "0x01".to_string();

        let error = parse_initialize_event_rpc(&rpc_log_robinhood_pool)
            .expect_err("short currency topic should fail");

        assert!(
            error
                .to_string()
                .contains("Currency0 topic must be 32 bytes")
        );
    }

    #[rstest]
    fn test_parse_initialize_rpc_rejects_noncanonical_currency_topic(
        mut rpc_log_robinhood_pool: RpcLog,
    ) {
        rpc_log_robinhood_pool.topics[2].replace_range(2..4, "01");

        let error = parse_initialize_event_rpc(&rpc_log_robinhood_pool)
            .expect_err("non-canonical currency topic should fail");

        assert!(error.to_string().contains("non-zero address padding"));
    }

    #[rstest]
    fn test_parse_initialize_rpc_rejects_out_of_range_sqrt_price(
        mut rpc_log_robinhood_pool: RpcLog,
    ) {
        replace_data_word(&mut rpc_log_robinhood_pool, 3, [0; 32]);

        let error = parse_initialize_event_rpc(&rpc_log_robinhood_pool)
            .expect_err("out-of-range sqrt price should fail");

        assert!(error.to_string().contains("sqrt price is out of bounds"));
    }

    #[rstest]
    fn test_parse_initialize_rpc_rejects_tick_mismatch(mut rpc_log_weth_usdc: RpcLog) {
        replace_data_word(&mut rpc_log_weth_usdc, 4, [0; 32]);

        let error = parse_initialize_event_rpc(&rpc_log_weth_usdc)
            .expect_err("mismatched tick should fail");

        assert!(error.to_string().contains("does not match tick"));
    }

    #[rstest]
    fn test_hypersync_rpc_match(hypersync_log_weth_usdc: HypersyncLog, rpc_log_weth_usdc: RpcLog) {
        let hypersync_event =
            parse_initialize_event_hypersync(hypersync_log_weth_usdc).expect("HyperSync parse");
        let rpc_event = parse_initialize_event_rpc(&rpc_log_weth_usdc).expect("RPC parse");

        assert_eq!(hypersync_event.block_number, rpc_event.block_number);
        assert_eq!(hypersync_event.token0, rpc_event.token0);
        assert_eq!(hypersync_event.token1, rpc_event.token1);
        assert_eq!(hypersync_event.pool_identifier, rpc_event.pool_identifier);
        assert_eq!(hypersync_event.fee, rpc_event.fee);
        assert_eq!(hypersync_event.tick_spacing, rpc_event.tick_spacing);
    }
}
