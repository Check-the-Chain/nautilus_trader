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

use std::{str::FromStr, sync::Arc};

use alloy::primitives::{Address, B256, address};
use nautilus_blockchain::{
    exchanges::robinhood,
    rpc::http::BlockchainHttpRpcClient,
    services::{ExactUniswapV4QuoteEngine, UniswapV4MirrorConfig},
    testing::uniswap_v4::{
        UniswapV4HistoricalTransitionCase, validate_uniswap_v4_historical_transition_live,
    },
};

#[tokio::test]
#[ignore = "requires ALCHEMY_ROBINHOOD_HTTP_URL with archive Robinhood RPC access"]
async fn exact_engine_replays_robinhood_nvda_usdg_historical_transition() {
    let http_url = std::env::var("ALCHEMY_ROBINHOOD_HTTP_URL")
        .expect("ALCHEMY_ROBINHOOD_HTTP_URL must be set to an archive Robinhood RPC endpoint");
    let pool_id =
        B256::from_str("0x3bb34a44f1b2b5f32c034c38a53065a521a47b199700fa9bd19d60985ff24bf1")
            .unwrap();
    let case = UniswapV4HistoricalTransitionCase {
        dex: robinhood::UNISWAP_V4.dex.clone(),
        pool_manager: address!("8366a39CC670B4001A1121B8F6A443A643e40951"),
        state_view_address: address!("F3334192D15450CdD385c8B70e03f9A6bD9E673b"),
        mirror_config: UniswapV4MirrorConfig::new(
            pool_id,
            address!("5fc5360D0400a0Fd4f2af552ADD042D716F1d168"),
            address!("d0601CE157Db5BDc3162BbaC2a2C8aF5320D9EEC"),
            60,
            3_000,
            Address::ZERO,
        )
        .unwrap(),
        block_number: 16_567_548,
        multicall_calls_per_rpc_request: 100,
    };
    let client = Arc::new(BlockchainHttpRpcClient::new(http_url, Some(2), None));

    let report =
        validate_uniswap_v4_historical_transition_live(client, case, &ExactUniswapV4QuoteEngine)
            .await
            .unwrap();

    eprintln!("{report:#?}");
    assert!(report.swap_count > 0);
    assert!(report.final_state_validated);
}
