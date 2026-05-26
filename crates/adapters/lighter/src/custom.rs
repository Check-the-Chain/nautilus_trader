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

//! Lighter-specific Nautilus custom data types.

use std::{any::Any, sync::Arc};

use nautilus_core::{Params, UnixNanos};
use nautilus_model::data::{CustomData, CustomDataTrait, DataType, HasTsInit};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::models::ws::{PositionWithDiscount, WsAccountAllPositionsUpdate};

pub const LIGHTER_ACCOUNT_POSITIONS_TYPE: &str = "LighterAccountPositions";
pub const LIGHTER_ACCOUNT_INDEX_PARAM: &str = "account_index";
pub const ACCOUNT_ALL_POSITIONS_CHANNEL: &str = "account_all_positions";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LighterAccountPosition {
    pub account_index: i64,
    pub market_id: i64,
    pub symbol: String,
    pub initial_margin_fraction: String,
    pub open_order_count: i64,
    pub pending_order_count: i64,
    pub position_tied_order_count: i64,
    pub sign: i64,
    pub position: String,
    pub avg_entry_price: String,
    pub position_value: String,
    pub unrealized_pnl: String,
    pub realized_pnl: String,
    pub liquidation_price: String,
    pub total_funding_paid_out: String,
    pub margin_mode: i64,
    pub allocated_margin: String,
    pub total_discount: String,
}

impl LighterAccountPosition {
    #[must_use]
    pub fn from_ws(account_index: i64, position: PositionWithDiscount) -> Self {
        Self {
            account_index,
            market_id: position.market_id,
            symbol: position.symbol,
            initial_margin_fraction: position.initial_margin_fraction,
            open_order_count: position.open_order_count,
            pending_order_count: position.pending_order_count,
            position_tied_order_count: position.position_tied_order_count,
            sign: position.sign,
            position: position.position,
            avg_entry_price: position.avg_entry_price,
            position_value: position.position_value,
            unrealized_pnl: position.unrealized_pnl,
            realized_pnl: position.realized_pnl,
            liquidation_price: position.liquidation_price,
            total_funding_paid_out: position.total_funding_paid_out,
            margin_mode: position.margin_mode,
            allocated_margin: position.allocated_margin,
            total_discount: position.total_discount,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LighterAccountPositions {
    pub account_index: i64,
    pub positions: Vec<LighterAccountPosition>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl LighterAccountPositions {
    #[must_use]
    pub fn data_type(account_index: i64) -> DataType {
        let mut metadata = Params::new();
        metadata.insert(
            LIGHTER_ACCOUNT_INDEX_PARAM.to_string(),
            json!(account_index),
        );
        DataType::new(
            LIGHTER_ACCOUNT_POSITIONS_TYPE,
            Some(metadata),
            Some(account_index.to_string()),
        )
    }

    #[must_use]
    pub fn from_ws_update(
        account_index: i64,
        update: WsAccountAllPositionsUpdate,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> Self {
        Self::try_from_ws_update(account_index, update, ts_event, ts_init)
            .expect("account_all_positions payload should have valid market keys")
    }

    /// Converts an account positions websocket payload into Nautilus custom data.
    ///
    /// # Errors
    ///
    /// Returns an error when a keyed market id disagrees with a nested position market id.
    pub fn try_from_ws_update(
        account_index: i64,
        update: WsAccountAllPositionsUpdate,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Self> {
        let mut positions = update
            .positions
            .into_iter()
            .flat_map(|(market_key, positions)| {
                positions.into_iter().map(move |position| {
                    let keyed_market_id = market_key.parse::<i64>().ok();
                    (market_key.clone(), keyed_market_id, position)
                })
            })
            .map(|(market_key, keyed_market_id, position)| {
                if let Some(keyed_market_id) = keyed_market_id {
                    anyhow::ensure!(
                        keyed_market_id == position.market_id,
                        "account_all_positions market key {market_key} disagrees with nested market_id {}",
                        position.market_id
                    );
                }
                Ok(LighterAccountPosition::from_ws(account_index, position))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        positions.sort_by(|left, right| {
            left.market_id
                .cmp(&right.market_id)
                .then_with(|| left.symbol.cmp(&right.symbol))
        });

        Ok(Self {
            account_index,
            positions,
            ts_event,
            ts_init,
        })
    }

    #[must_use]
    pub fn into_custom_data(self) -> CustomData {
        let data_type = Self::data_type(self.account_index);
        CustomData::new(Arc::new(self), data_type)
    }
}

#[must_use]
pub fn account_positions_channel(account_index: i64) -> String {
    format!("{ACCOUNT_ALL_POSITIONS_CHANNEL}/{account_index}")
}

impl HasTsInit for LighterAccountPositions {
    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }
}

impl CustomDataTrait for LighterAccountPositions {
    fn type_name_static() -> &'static str
    where
        Self: Sized,
    {
        LIGHTER_ACCOUNT_POSITIONS_TYPE
    }

    fn type_name(&self) -> &'static str {
        LIGHTER_ACCOUNT_POSITIONS_TYPE
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
        Arc::new(self.clone())
    }

    fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|other| self == other)
    }

    fn from_json(value: serde_json::Value) -> anyhow::Result<Arc<dyn CustomDataTrait>>
    where
        Self: Sized,
    {
        Ok(Arc::new(serde_json::from_value::<Self>(value)?))
    }
}

#[must_use]
pub fn account_index_from_data_type(data_type: &DataType) -> Option<i64> {
    data_type
        .metadata()
        .and_then(|metadata| metadata.get_i64(LIGHTER_ACCOUNT_INDEX_PARAM))
        .or_else(|| data_type.identifier()?.parse::<i64>().ok())
}

#[must_use]
pub fn account_index_from_params(params: Option<&Params>) -> Option<i64> {
    params.and_then(|params| params.get_i64(LIGHTER_ACCOUNT_INDEX_PARAM))
}

#[must_use]
pub fn account_index_from_channel(channel: &str) -> Option<i64> {
    let (prefix, account_index) = channel
        .split_once('/')
        .or_else(|| channel.split_once(':'))?;
    (prefix == ACCOUNT_ALL_POSITIONS_CHANNEL)
        .then_some(account_index)?
        .parse::<i64>()
        .ok()
}

#[cfg(test)]
mod tests {
    use nautilus_model::data::CustomDataTrait;

    use super::{
        LighterAccountPositions, account_index_from_channel, account_index_from_data_type,
        account_positions_channel,
    };
    use crate::models::ws::WsAccountAllPositionsUpdate;

    #[test]
    fn account_position_data_type_carries_account_index() {
        let data_type = LighterAccountPositions::data_type(54255);

        assert_eq!(data_type.type_name(), "LighterAccountPositions");
        assert_eq!(account_index_from_data_type(&data_type), Some(54255));
    }

    #[test]
    fn account_index_parses_from_channel() {
        assert_eq!(
            account_index_from_channel("account_all_positions/54255"),
            Some(54255)
        );
        assert_eq!(
            account_index_from_channel("account_all_positions:54255"),
            Some(54255)
        );
        assert_eq!(
            account_index_from_channel("account_all_positions/not-an-int"),
            None
        );
        assert_eq!(account_index_from_channel("account_all_orders/54255"), None);
        assert_eq!(
            account_positions_channel(54255),
            "account_all_positions/54255"
        );
    }

    #[test]
    fn account_positions_custom_data_round_trips_json() {
        let payload = r#"{"type":"update/account_all_positions","channel":"account_all_positions:54255","positions":{"89":{"market_id":89,"symbol":"BTC-USDC","initial_margin_fraction":"100","open_order_count":2,"pending_order_count":0,"position_tied_order_count":0,"sign":1,"position":"0.01","avg_entry_price":"95000.00","position_value":"950.00","unrealized_pnl":"5.0","realized_pnl":"0.0","liquidation_price":"80000.0","total_funding_paid_out":"0.0","margin_mode":0,"allocated_margin":"0.0","total_discount":"0"}}}"#;
        let update: WsAccountAllPositionsUpdate = serde_json::from_str(payload).unwrap();
        let positions = LighterAccountPositions::from_ws_update(54255, update, 1.into(), 2.into());

        assert_eq!(
            LighterAccountPositions::type_name_static(),
            "LighterAccountPositions"
        );
        assert_eq!(positions.positions.len(), 1);
        assert_eq!(positions.positions[0].market_id, 89);
        let json = positions.to_json().unwrap();
        let decoded =
            LighterAccountPositions::from_json(serde_json::from_str(&json).unwrap()).unwrap();
        assert!(
            decoded
                .as_any()
                .downcast_ref::<LighterAccountPositions>()
                .is_some()
        );
    }

    #[test]
    fn account_positions_reject_mismatched_market_key() {
        let payload = r#"{"type":"update/account_all_positions","channel":"account_all_positions:54255","positions":{"90":{"market_id":89,"symbol":"BTC-USDC","initial_margin_fraction":"100","open_order_count":2,"pending_order_count":0,"position_tied_order_count":0,"sign":1,"position":"0.01","avg_entry_price":"95000.00","position_value":"950.00","unrealized_pnl":"5.0","realized_pnl":"0.0","liquidation_price":"80000.0","total_funding_paid_out":"0.0","margin_mode":0,"allocated_margin":"0.0","total_discount":"0"}}}"#;
        let update: WsAccountAllPositionsUpdate = serde_json::from_str(payload).unwrap();

        let error = LighterAccountPositions::try_from_ws_update(54255, update, 1.into(), 2.into())
            .unwrap_err();

        assert!(error.to_string().contains("disagrees"));
    }
}
