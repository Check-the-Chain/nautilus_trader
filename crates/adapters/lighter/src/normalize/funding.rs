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

//! Funding normalization for Lighter perpetual market data.

use nautilus_core::{UnixNanos, datetime::NANOSECONDS_IN_MINUTE};
use nautilus_model::{
    data::FundingRateUpdate,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use rust_decimal::Decimal;

use crate::{
    models::{funding::FundingRate, market::PerpsMarketStats},
    normalize::timestamp::valid_epoch_to_unix_nanos,
};

pub const LIGHTER_FUNDING_INTERVAL_MINS: u16 = 60;

#[must_use]
pub fn current_update_from_market_stats(
    instrument: &InstrumentAny,
    market: &PerpsMarketStats,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> Option<FundingRateUpdate> {
    let rate = market.current_funding_rate.or(market.funding_rate)?;
    Some(FundingRateUpdate::new(
        instrument.id(),
        lighter_percent_to_rate(rate),
        Some(LIGHTER_FUNDING_INTERVAL_MINS),
        next_funding_ns_from_market(market),
        ts_event,
        ts_init,
    ))
}

#[must_use]
pub fn historical_update_from_funding_rate_endpoint(
    instrument_id: InstrumentId,
    funding_rate: &FundingRate,
    ts_init: UnixNanos,
) -> Option<FundingRateUpdate> {
    if !is_lighter_exchange(funding_rate) {
        return None;
    }

    let rate = funding_rate.funding_rate?;
    let ts_event = valid_epoch_to_unix_nanos(funding_rate.settlement_time)?;

    Some(FundingRateUpdate::new(
        instrument_id,
        lighter_percent_to_rate(rate),
        Some(LIGHTER_FUNDING_INTERVAL_MINS),
        None,
        ts_event,
        ts_init,
    ))
}

fn is_lighter_exchange(funding_rate: &FundingRate) -> bool {
    funding_rate
        .exchange
        .as_deref()
        .is_none_or(|exchange| exchange.eq_ignore_ascii_case("lighter"))
}

fn lighter_percent_to_rate(value: f64) -> Decimal {
    Decimal::from_str_exact(&value.to_string()).unwrap_or(Decimal::ZERO) / Decimal::from(100)
}

fn next_funding_ns_from_market(market: &PerpsMarketStats) -> Option<UnixNanos> {
    valid_epoch_to_unix_nanos(market.next_funding_time).or_else(|| {
        valid_epoch_to_unix_nanos(market.funding_timestamp).and_then(|value| {
            value.checked_add(u64::from(LIGHTER_FUNDING_INTERVAL_MINS) * NANOSECONDS_IN_MINUTE)
        })
    })
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;
    use nautilus_model::instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt};
    use rust_decimal::Decimal;

    use super::{current_update_from_market_stats, historical_update_from_funding_rate_endpoint};
    use crate::models::{funding::FundingRate, market::PerpsMarketStats};

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str_exact(value).unwrap()
    }

    fn test_market_stats(
        current_funding_rate: f64,
        funding_timestamp: Option<i64>,
        next_funding_time: Option<i64>,
    ) -> PerpsMarketStats {
        PerpsMarketStats {
            market_id: Some(110),
            symbol: Some("NVDA".to_string()),
            last_trade_price: None,
            mark_price: None,
            index_price: None,
            open_interest: None,
            next_funding_time,
            funding_timestamp,
            current_funding_rate: Some(current_funding_rate),
            funding_rate: None,
            funding_countdown: None,
            volume_24h: None,
            high_24h: None,
            low_24h: None,
            change_24h: None,
        }
    }

    #[test]
    fn market_stats_update_converts_lighter_percent_to_nautilus_rate() {
        let instrument = InstrumentAny::from(crypto_perpetual_ethusdt());
        let update = current_update_from_market_stats(
            &instrument,
            &test_market_stats(0.0021, None, None),
            0.into(),
            0.into(),
        )
        .expect("funding update");

        assert_eq!(update.rate, decimal("0.000021"));
        assert_eq!(update.interval, Some(60));
        assert_eq!(update.next_funding_ns, None);
    }

    #[test]
    fn market_stats_update_preserves_negative_sign() {
        let instrument = InstrumentAny::from(crypto_perpetual_ethusdt());
        let update = current_update_from_market_stats(
            &instrument,
            &test_market_stats(-0.0405, None, None),
            0.into(),
            0.into(),
        )
        .expect("funding update");

        assert_eq!(update.rate, decimal("-0.000405"));
    }

    #[test]
    fn market_stats_funding_timestamp_derives_next_funding_time() {
        let instrument = InstrumentAny::from(crypto_perpetual_ethusdt());
        let update = current_update_from_market_stats(
            &instrument,
            &test_market_stats(0.0021, Some(1_700_000_000_000), None),
            0.into(),
            0.into(),
        )
        .expect("funding update");

        assert_eq!(
            update.next_funding_ns,
            Some(UnixNanos::from(1_700_003_600_000_000_000))
        );
    }

    #[test]
    fn explicit_next_funding_time_takes_precedence() {
        let instrument = InstrumentAny::from(crypto_perpetual_ethusdt());
        let update = current_update_from_market_stats(
            &instrument,
            &test_market_stats(0.0021, Some(1_700_000_000_000), Some(1_700_000_123_000)),
            0.into(),
            0.into(),
        )
        .expect("funding update");

        assert_eq!(
            update.next_funding_ns,
            Some(UnixNanos::from(1_700_000_123_000_000_000))
        );
    }

    #[test]
    fn funding_rate_endpoint_update_has_no_next_funding_time() {
        let instrument = InstrumentAny::from(crypto_perpetual_ethusdt());
        let update = historical_update_from_funding_rate_endpoint(
            instrument.id(),
            &FundingRate {
                market_id: Some(110),
                exchange: Some("lighter".to_string()),
                symbol: Some("NVDA".to_string()),
                funding_rate: Some(0.0021),
                mark_price: None,
                index_price: None,
                settlement_time: Some(1_700_000_000_000),
            },
            UnixNanos::from(1),
        )
        .expect("funding update");

        assert_eq!(update.rate, decimal("0.000021"));
        assert_eq!(update.interval, Some(60));
        assert_eq!(update.next_funding_ns, None);
        assert_eq!(update.ts_event, UnixNanos::from(1_700_000_000_000_000_000));
    }

    #[test]
    fn funding_rate_endpoint_ignores_current_cross_exchange_rows() {
        let instrument = InstrumentAny::from(crypto_perpetual_ethusdt());

        let binance_update = historical_update_from_funding_rate_endpoint(
            instrument.id(),
            &FundingRate {
                market_id: Some(110),
                exchange: Some("binance".to_string()),
                symbol: Some("NVDA".to_string()),
                funding_rate: Some(0.0021),
                mark_price: None,
                index_price: None,
                settlement_time: Some(1_700_000_000_000),
            },
            UnixNanos::from(1),
        );
        let unsettled_lighter_update = historical_update_from_funding_rate_endpoint(
            instrument.id(),
            &FundingRate {
                market_id: Some(110),
                exchange: Some("lighter".to_string()),
                symbol: Some("NVDA".to_string()),
                funding_rate: Some(0.0021),
                mark_price: None,
                index_price: None,
                settlement_time: None,
            },
            UnixNanos::from(1),
        );

        assert!(binance_update.is_none());
        assert!(unsettled_lighter_update.is_none());
    }
}
