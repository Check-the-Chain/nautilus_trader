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

use std::{collections::HashMap, sync::LazyLock};

use nautilus_model::defi::DexType;

use crate::exchanges::extended::DexExtended;

mod factory_parsing;
mod giga_classic;
mod giga_v3;
mod swap_hood_v2;
mod swap_hood_v3;
mod uniswap_v4;
mod up_slipstream;
mod up_v2;

pub use giga_classic::GIGA_CLASSIC;
pub use giga_v3::GIGA_V3;
pub use swap_hood_v2::SWAP_HOOD_V2;
pub use swap_hood_v3::SWAP_HOOD_V3;
pub use uniswap_v4::UNISWAP_V4;
pub use up_slipstream::UP_SLIPSTREAM;
pub use up_v2::UP_V2;

pub static ROBINHOOD_DEX_EXTENDED_MAP: LazyLock<HashMap<DexType, &'static DexExtended>> =
    LazyLock::new(|| {
        HashMap::from([
            (GIGA_CLASSIC.dex.name, &*GIGA_CLASSIC),
            (GIGA_V3.dex.name, &*GIGA_V3),
            (SWAP_HOOD_V2.dex.name, &*SWAP_HOOD_V2),
            (SWAP_HOOD_V3.dex.name, &*SWAP_HOOD_V3),
            (UNISWAP_V4.dex.name, &*UNISWAP_V4),
            (UP_SLIPSTREAM.dex.name, &*UP_SLIPSTREAM),
            (UP_V2.dex.name, &*UP_V2),
        ])
    });

#[cfg(test)]
mod tests {
    use alloy::primitives::keccak256;
    use nautilus_core::hex;
    use nautilus_model::defi::{AmmType, DexType, rpc::RpcLog};
    use rstest::rstest;

    use super::*;
    use crate::{
        events::pool_created::PoolCreatedEvent,
        exchanges::parsing::{slipstream, solidly_v2},
    };

    #[rstest]
    #[case(
        DexType::UpV2,
        "0xFA5429AEBa338BEa2BFcc1b9a889862Ee395bc28",
        6_180_950,
        AmmType::CPAMM,
        "PoolCreated(address,address,bool,address,uint256)",
        solidly_v2::pool_created::parse_up_pool_created_event_rpc
    )]
    #[case(
        DexType::UpSlipstream,
        "0x1ac9dB4a2608ba45D6127B1737949b51Bb54B7F3",
        6_184_096,
        AmmType::CLAMM,
        "PoolCreated(address,address,int24,address)",
        slipstream::pool_created::parse_pool_created_event_rpc
    )]
    #[case(
        DexType::GigaClassic,
        "0x6Fdf38f92eAd1adFc04B73aaa947ab254f6c0916",
        10_357_446,
        AmmType::CPAMM,
        "PairCreated(address,address,bool,address,uint256)",
        solidly_v2::pool_created::parse_giga_pool_created_event_rpc
    )]
    #[case(
        DexType::GigaV3,
        "0xEce6eCd61177336ea6Fb9b17937AC439D85EE20B",
        10_357_399,
        AmmType::CLAMM,
        "PoolCreated(address,address,uint24,int24,address)",
        factory_parsing::parse_v3_pool_created_event_rpc
    )]
    #[case(
        DexType::SwapHoodV2,
        "0xE7206Ecac3A51afe7e6179182ad4130A26068dD1",
        5_399_882,
        AmmType::CPAMM,
        "PairCreated(address,address,address,uint256)",
        factory_parsing::parse_v2_pool_created_event_rpc
    )]
    #[case(
        DexType::SwapHoodV3,
        "0x0Ec554F0BfF0Be6C99d1e95C8015bb0950f6A2C7",
        6_052_562,
        AmmType::CLAMM,
        "PoolCreated(address,address,uint24,int24,address)",
        factory_parsing::parse_v3_pool_created_event_rpc
    )]
    fn robinhood_registration_is_rpc_discovery_only(
        #[case] dex_type: DexType,
        #[case] factory: &str,
        #[case] factory_creation_block: u64,
        #[case] amm_type: AmmType,
        #[case] pool_created_event: &str,
        #[case] rpc_parser: fn(&RpcLog) -> anyhow::Result<PoolCreatedEvent>,
    ) {
        let dex = ROBINHOOD_DEX_EXTENDED_MAP
            .get(&dex_type)
            .unwrap_or_else(|| panic!("{dex_type} should be registered on Robinhood"));

        assert_eq!(
            dex.factory.to_string().to_lowercase(),
            factory.to_lowercase()
        );
        assert_eq!(dex.factory_creation_block, factory_creation_block);
        assert_eq!(dex.amm_type, amm_type);
        assert_eq!(
            dex.pool_created_event.as_ref(),
            hex::encode_prefixed(keccak256(pool_created_event.as_bytes()))
        );
        assert!(std::ptr::fn_addr_eq(
            dex.parse_pool_created_event_rpc_fn
                .expect("RPC parser should be registered"),
            rpc_parser
        ));
        assert!(dex.supports_pool_discovery_rpc());
        assert!(!dex.supports_pool_discovery_hypersync());
        assert!(dex.parse_pool_created_event_hypersync_fn.is_none());
        assert!(dex.swap_created_event.is_empty());
        assert!(dex.mint_created_event.is_empty());
        assert!(dex.burn_created_event.is_empty());
        assert!(dex.collect_created_event.is_empty());
        assert!(!dex.missing_pool_analysis_parsers().is_empty());
        assert!(!dex.supports_fee_protocol_replay());
    }
}
