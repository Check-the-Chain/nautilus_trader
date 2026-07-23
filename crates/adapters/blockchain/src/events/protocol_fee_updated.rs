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

use alloy::primitives::B256;
use nautilus_model::defi::SharedDex;

/// A provider-neutral Uniswap V4 `ProtocolFeeUpdated` event.
#[derive(Debug, Clone)]
pub struct ProtocolFeeUpdatedEvent {
    pub dex: SharedDex,
    pub pool_id: B256,
    pub block_number: u64,
    pub transaction_hash: String,
    pub transaction_index: u32,
    pub log_index: u32,
    /// The packed one-for-zero and zero-for-one protocol fees.
    pub protocol_fee: u32,
}

impl ProtocolFeeUpdatedEvent {
    #[must_use]
    pub fn new(
        dex: SharedDex,
        pool_id: B256,
        block_number: u64,
        transaction_hash: String,
        transaction_index: u32,
        log_index: u32,
        protocol_fee: u32,
    ) -> Self {
        Self {
            dex,
            pool_id,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            protocol_fee,
        }
    }
}
