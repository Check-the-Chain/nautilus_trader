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

use alloy::primitives::{Address, B256, I256};
use nautilus_model::defi::SharedDex;

/// A provider-neutral Uniswap V4 `ModifyLiquidity` event emitted by `IPoolManager`.
#[derive(Debug, Clone)]
pub struct ModifyLiquidityEvent {
    pub dex: SharedDex,
    pub pool_id: B256,
    pub block_number: u64,
    pub transaction_hash: String,
    pub transaction_index: u32,
    pub log_index: u32,
    pub sender: Address,
    pub tick_lower: i32,
    pub tick_upper: i32,
    pub liquidity_delta: I256,
    pub salt: B256,
}

impl ModifyLiquidityEvent {
    #[must_use]
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        dex: SharedDex,
        pool_id: B256,
        block_number: u64,
        transaction_hash: String,
        transaction_index: u32,
        log_index: u32,
        sender: Address,
        tick_lower: i32,
        tick_upper: i32,
        liquidity_delta: I256,
        salt: B256,
    ) -> Self {
        Self {
            dex,
            pool_id,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            sender,
            tick_lower,
            tick_upper,
            liquidity_delta,
            salt,
        }
    }
}
