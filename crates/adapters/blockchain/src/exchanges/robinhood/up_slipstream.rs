// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::sync::LazyLock;

use nautilus_model::defi::{
    chain::chains,
    dex::{AmmType, Dex, DexType},
};

use crate::exchanges::{extended::DexExtended, parsing::slipstream};

/// UP Slipstream DEX on Robinhood Chain.
pub static UP_SLIPSTREAM: LazyLock<DexExtended> = LazyLock::new(|| {
    let dex = Dex::new_discovery_only(
        chains::ROBINHOOD.clone(),
        DexType::UpSlipstream,
        "0x1ac9dB4a2608ba45D6127B1737949b51Bb54B7F3",
        6_184_096,
        AmmType::CLAMM,
        "PoolCreated(address,address,int24,address)",
    );
    let mut dex_extended = DexExtended::new(dex);
    dex_extended
        .set_pool_created_event_rpc_parsing(slipstream::pool_created::parse_pool_created_event_rpc);
    dex_extended
});
