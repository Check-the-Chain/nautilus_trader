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

//! Interactive Brokers-specific custom data types.
//!
//! These types carry IBKR venue facts through the Nautilus data engine as
//! [`CustomData`](nautilus_model::data::CustomData). They intentionally do not
//! encode strategy economics such as Carry Quotes.

use std::sync::Arc;

use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    data::{CustomData, DataType},
    identifiers::InstrumentId,
};
use nautilus_persistence_macros::custom_data;

pub const IBKR_SHORT_AVAILABILITY_TYPE: &str = "IbkrShortAvailability";
pub const INSTRUMENT_ID_METADATA_KEY: &str = "instrument_id";
pub const SHORTABLE_SCORE_SCALE: i64 = 1_000_000;

/// IBKR short-sale availability update from generic tick `236`.
///
/// `shortable_score_e6` is the TWS indicative shortability score scaled by
/// [`SHORTABLE_SCORE_SCALE`]. `shortable_shares` is the exact share count when
/// TWS/Gateway emits tick type `ShortableShares`.
#[custom_data(no_arrow)]
pub struct IbkrShortAvailability {
    /// The Nautilus instrument ID for this IBKR stock.
    pub instrument_id: InstrumentId,
    /// Indicative IBKR shortability score scaled by 1e6.
    #[custom_data_field(serde)]
    pub shortable_score_e6: Option<i64>,
    /// Exact number of shares available to short when reported by IBKR.
    #[custom_data_field(serde)]
    pub shortable_shares: Option<u64>,
    /// UNIX timestamp (nanoseconds) when the data event occurred.
    pub ts_event: UnixNanos,
    /// UNIX timestamp (nanoseconds) when the instance was initialized.
    pub ts_init: UnixNanos,
}

impl IbkrShortAvailability {
    #[must_use]
    pub fn from_score(
        instrument_id: InstrumentId,
        shortable_score_e6: i64,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> Self {
        Self::new(
            instrument_id,
            Some(shortable_score_e6),
            None,
            ts_event,
            ts_init,
        )
    }

    #[must_use]
    pub fn from_shares(
        instrument_id: InstrumentId,
        shortable_shares: u64,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> Self {
        Self::new(
            instrument_id,
            None,
            Some(shortable_shares),
            ts_event,
            ts_init,
        )
    }

    #[must_use]
    pub fn data_type_for_instrument(instrument_id: InstrumentId) -> DataType {
        let mut metadata = Params::new();
        metadata.insert(
            INSTRUMENT_ID_METADATA_KEY.to_string(),
            serde_json::Value::String(instrument_id.to_string()),
        );
        DataType::new(
            IBKR_SHORT_AVAILABILITY_TYPE,
            Some(metadata),
            Some(instrument_id.to_string()),
        )
    }

    #[must_use]
    pub fn data_type(&self) -> DataType {
        Self::data_type_for_instrument(self.instrument_id)
    }

    #[must_use]
    pub fn into_custom_data(self) -> CustomData {
        let data_type = self.data_type();
        CustomData::new(Arc::new(self), data_type)
    }
}

/// Registers Interactive Brokers custom data types.
///
/// Safe to call multiple times.
pub fn register_interactive_brokers_custom_data() {
    let _ = nautilus_model::data::ensure_custom_data_json_registered::<IbkrShortAvailability>();
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_model::identifiers::{Symbol, Venue};
    use rstest::rstest;

    fn instrument_id() -> InstrumentId {
        InstrumentId::new(Symbol::from("AAPL"), Venue::from("XNAS"))
    }

    #[rstest]
    fn test_register_interactive_brokers_custom_data_is_idempotent() {
        register_interactive_brokers_custom_data();
        register_interactive_brokers_custom_data();
    }

    #[rstest]
    fn test_short_availability_data_type_is_instrument_scoped() {
        let data_type = IbkrShortAvailability::data_type_for_instrument(instrument_id());

        assert_eq!(data_type.type_name(), IBKR_SHORT_AVAILABILITY_TYPE);
        assert_eq!(data_type.identifier(), Some("AAPL.XNAS"));
        assert_eq!(
            data_type
                .metadata()
                .and_then(|m| m.get(INSTRUMENT_ID_METADATA_KEY))
                .and_then(|value| value.as_str()),
            Some("AAPL.XNAS")
        );
    }

    #[rstest]
    fn test_short_availability_wraps_as_custom_data() {
        let payload = IbkrShortAvailability::from_shares(
            instrument_id(),
            25_000,
            UnixNanos::from(10),
            UnixNanos::from(11),
        );

        let data = payload.into_custom_data();

        assert_eq!(data.data_type.type_name(), IBKR_SHORT_AVAILABILITY_TYPE);
        assert_eq!(data.data_type.identifier(), Some("AAPL.XNAS"));
    }
}
