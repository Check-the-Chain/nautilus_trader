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

//! Timestamp normalization for Lighter payloads.

use nautilus_core::UnixNanos;

#[must_use]
pub fn epoch_to_unix_nanos(value: Option<i64>) -> UnixNanos {
    let Some(value) = value else {
        return UnixNanos::default();
    };
    if value <= 0 {
        return UnixNanos::default();
    }

    let digits = value.unsigned_abs().ilog10() + 1;
    let nanos = if digits <= 10 {
        value.saturating_mul(1_000_000_000)
    } else if digits <= 13 {
        value.saturating_mul(1_000_000)
    } else if digits <= 16 {
        value.saturating_mul(1_000)
    } else {
        value
    };

    UnixNanos::from(nanos.max(0) as u64)
}

#[must_use]
pub fn valid_epoch_to_unix_nanos(value: Option<i64>) -> Option<UnixNanos> {
    let nanos = epoch_to_unix_nanos(value);
    (!nanos.is_zero()).then_some(nanos)
}

#[must_use]
pub fn ticker_event_time(
    ticker_last_updated_at: Option<i64>,
    message_last_updated_at: Option<i64>,
    message_timestamp: Option<i64>,
    fallback: UnixNanos,
) -> UnixNanos {
    valid_epoch_to_unix_nanos(ticker_last_updated_at)
        .or_else(|| valid_epoch_to_unix_nanos(message_last_updated_at))
        .or_else(|| valid_epoch_to_unix_nanos(message_timestamp))
        .unwrap_or(fallback)
}

#[must_use]
pub fn message_event_time(message_timestamp: Option<i64>, fallback: UnixNanos) -> UnixNanos {
    valid_epoch_to_unix_nanos(message_timestamp).unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;

    use super::{epoch_to_unix_nanos, ticker_event_time, valid_epoch_to_unix_nanos};

    #[test]
    fn epoch_to_unix_nanos_supports_seconds_millis_micros_and_nanos() {
        assert_eq!(
            epoch_to_unix_nanos(Some(1_700_000_000)),
            UnixNanos::from(1_700_000_000_000_000_000)
        );
        assert_eq!(
            epoch_to_unix_nanos(Some(1_700_000_000_000)),
            UnixNanos::from(1_700_000_000_000_000_000)
        );
        assert_eq!(
            epoch_to_unix_nanos(Some(1_700_000_000_000_000)),
            UnixNanos::from(1_700_000_000_000_000_000)
        );
        assert_eq!(
            epoch_to_unix_nanos(Some(1_700_000_000_000_000_000)),
            UnixNanos::from(1_700_000_000_000_000_000)
        );
    }

    #[test]
    fn valid_epoch_to_unix_nanos_rejects_missing_zero_and_negative_values() {
        assert_eq!(valid_epoch_to_unix_nanos(None), None);
        assert_eq!(valid_epoch_to_unix_nanos(Some(0)), None);
        assert_eq!(valid_epoch_to_unix_nanos(Some(-1)), None);
    }

    #[test]
    fn ticker_event_time_prefers_matching_engine_timestamp() {
        let event_time = ticker_event_time(
            Some(1_700_000_000_000_001),
            Some(1_700_000_000_000_002),
            Some(1_700_000_000_003),
            UnixNanos::from(9),
        );

        assert_eq!(event_time, UnixNanos::from(1_700_000_000_000_001_000));
    }
}
