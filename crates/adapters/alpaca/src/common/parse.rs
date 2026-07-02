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

//! Cross-cutting parsing helpers shared between HTTP and WebSocket layers.
//!
//! Alpaca encodes prices and quantities as JSON numbers (market data) or
//! decimal strings (trading API), and timestamps as RFC 3339 strings with
//! nanosecond precision. The helpers here convert those into Nautilus value
//! types at instrument precision.

use std::str::FromStr;

use nautilus_core::UnixNanos;
use nautilus_model::types::{Price, Quantity, fixed::FIXED_PRECISION};
use rust_decimal::Decimal;

/// Maximum decimal places that fit into Nautilus [`Price`] / [`Quantity`].
pub const MAX_DECIMALS: u8 = FIXED_PRECISION;

/// Converts a decimal string into a Nautilus [`Price`] at the requested precision.
///
/// # Errors
///
/// Returns an error if the string is not a decimal, if `precision` exceeds
/// [`MAX_DECIMALS`], or if the resulting value is out of range.
pub fn parse_price(value: &str, precision: u8) -> anyhow::Result<Price> {
    anyhow::ensure!(
        precision <= MAX_DECIMALS,
        "price precision {precision} exceeds maximum {MAX_DECIMALS}",
    );
    let decimal =
        Decimal::from_str(value).map_err(|e| anyhow::anyhow!("invalid price `{value}`: {e}"))?;
    Price::from_decimal_dp(decimal, precision)
        .map_err(|e| anyhow::anyhow!("invalid price `{value}` at precision {precision}: {e}"))
}

/// Converts a decimal string into a non-negative Nautilus [`Quantity`].
///
/// Zero is allowed because order objects report `filled_qty: "0"` before any
/// fills arrive.
///
/// # Errors
///
/// Returns an error if the string is not a decimal, if `precision` exceeds
/// [`MAX_DECIMALS`], if the value is negative, or if the resulting quantity
/// is out of range.
pub fn parse_quantity(value: &str, precision: u8) -> anyhow::Result<Quantity> {
    anyhow::ensure!(
        precision <= MAX_DECIMALS,
        "size precision {precision} exceeds maximum {MAX_DECIMALS}",
    );
    let decimal =
        Decimal::from_str(value).map_err(|e| anyhow::anyhow!("invalid quantity `{value}`: {e}"))?;
    anyhow::ensure!(!decimal.is_sign_negative(), "negative quantity `{value}`");
    Quantity::from_decimal_dp(decimal, precision)
        .map_err(|e| anyhow::anyhow!("invalid quantity `{value}` at precision {precision}: {e}"))
}

/// Converts a JSON number into a Nautilus [`Price`] at the requested precision.
///
/// Market data payloads encode prices as JSON numbers; conversion routes
/// through [`Decimal`] to avoid accumulating binary floating-point error.
///
/// # Errors
///
/// Returns an error if the value is not representable, if `precision` exceeds
/// [`MAX_DECIMALS`], or if the resulting value is out of range.
pub fn parse_price_from_f64(value: f64, precision: u8) -> anyhow::Result<Price> {
    anyhow::ensure!(
        precision <= MAX_DECIMALS,
        "price precision {precision} exceeds maximum {MAX_DECIMALS}",
    );
    let decimal =
        Decimal::try_from(value).map_err(|e| anyhow::anyhow!("invalid price `{value}`: {e}"))?;
    Price::from_decimal_dp(decimal, precision)
        .map_err(|e| anyhow::anyhow!("invalid price `{value}` at precision {precision}: {e}"))
}

/// Converts a JSON number into a non-negative Nautilus [`Quantity`].
///
/// # Errors
///
/// Returns an error if the value is not representable, if `precision` exceeds
/// [`MAX_DECIMALS`], if the value is negative, or if the resulting quantity
/// is out of range.
pub fn parse_quantity_from_f64(value: f64, precision: u8) -> anyhow::Result<Quantity> {
    anyhow::ensure!(
        precision <= MAX_DECIMALS,
        "size precision {precision} exceeds maximum {MAX_DECIMALS}",
    );
    let decimal =
        Decimal::try_from(value).map_err(|e| anyhow::anyhow!("invalid quantity `{value}`: {e}"))?;
    anyhow::ensure!(!decimal.is_sign_negative(), "negative quantity `{value}`");
    Quantity::from_decimal_dp(decimal, precision)
        .map_err(|e| anyhow::anyhow!("invalid quantity `{value}` at precision {precision}: {e}"))
}

/// Parses an RFC 3339 timestamp string (nanosecond precision) into [`UnixNanos`].
///
/// # Errors
///
/// Returns an error if the string is not a valid RFC 3339 timestamp.
pub fn parse_rfc3339_timestamp(value: &str, field: &str) -> anyhow::Result<UnixNanos> {
    value
        .parse::<UnixNanos>()
        .map_err(|e| anyhow::anyhow!("failed to parse {field}=`{value}`: {e}"))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("189.05", 2, "189.05")]
    #[case("0.0001", 4, "0.0001")]
    #[case("42", 0, "42")]
    fn test_parse_price(#[case] value: &str, #[case] precision: u8, #[case] expected: &str) {
        assert_eq!(parse_price(value, precision).unwrap().to_string(), expected);
    }

    #[rstest]
    fn test_parse_price_invalid() {
        assert!(parse_price("not-a-number", 2).is_err());
    }

    #[rstest]
    #[case("100", 0, "100")]
    #[case("0.5", 9, "0.500000000")]
    #[case("0", 0, "0")]
    fn test_parse_quantity(#[case] value: &str, #[case] precision: u8, #[case] expected: &str) {
        assert_eq!(
            parse_quantity(value, precision).unwrap().to_string(),
            expected
        );
    }

    #[rstest]
    fn test_parse_quantity_rejects_negative() {
        assert!(parse_quantity("-1", 0).is_err());
    }

    #[rstest]
    fn test_parse_price_from_f64() {
        assert_eq!(
            parse_price_from_f64(189.05, 2).unwrap().to_string(),
            "189.05"
        );
    }

    #[rstest]
    fn test_parse_quantity_from_f64_rejects_negative() {
        assert!(parse_quantity_from_f64(-0.5, 2).is_err());
    }

    #[rstest]
    fn test_parse_rfc3339_timestamp() {
        let nanos = parse_rfc3339_timestamp("2026-01-05T14:30:00.123456789Z", "t").unwrap();
        assert_eq!(nanos.as_u64() % 1_000_000_000, 123_456_789);
    }

    #[rstest]
    fn test_parse_rfc3339_timestamp_invalid() {
        assert!(parse_rfc3339_timestamp("not-a-timestamp", "t").is_err());
    }
}
