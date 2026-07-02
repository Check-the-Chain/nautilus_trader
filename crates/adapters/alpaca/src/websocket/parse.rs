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

//! Parsing from Alpaca wire messages into Nautilus domain types.
//!
//! The market data hot path is a single typed deserialization pass over the
//! raw frame ([`serde_json::from_slice`] or [`rmp_serde::from_slice`] per the
//! negotiated [`WsFormat`]) followed by direct construction of Nautilus
//! values at instrument precision; timestamps convert to [`UnixNanos`] during
//! deserialization and symbols intern through [`Ustr`], so steady-state
//! parsing performs no per-message string allocations beyond the trade-ID
//! render.
//!
//! [`Ustr`]: ustr::Ustr

use std::sync::LazyLock;

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarSpecification, BarType, QuoteTick, TradeTick},
    enums::{AggregationSource, AggressorSide, BarAggregation, PriceType},
    identifiers::TradeId,
};

use super::{
    error::AlpacaWsError,
    messages::{
        AlpacaInstrumentInfo, AlpacaWsBar, AlpacaWsEvent, AlpacaWsQuote, AlpacaWsTrade, WsFormat,
    },
};
use crate::common::parse::{parse_price_from_f64, parse_quantity_from_f64};

/// Nanoseconds in one minute, for deriving bar close times from the venue's
/// bar-open timestamps.
const NANOS_PER_MINUTE: u64 = 60_000_000_000;

/// The 1-minute LAST bar specification used for Alpaca minute bars.
pub static BAR_SPEC_1_MINUTE_LAST: LazyLock<BarSpecification> =
    LazyLock::new(|| BarSpecification::new(1, BarAggregation::Minute, PriceType::Last));

/// Parses a raw market data frame into typed events in a single pass.
///
/// Alpaca frames the market data stream as arrays of `"T"`-tagged messages
/// (JSON text or MessagePack binary per the negotiated `format`); control
/// messages arrive alone while data messages may be batched.
///
/// # Errors
///
/// Returns an error if the payload is not a valid message array in the given
/// format.
pub fn parse_ws_events(raw: &[u8], format: WsFormat) -> Result<Vec<AlpacaWsEvent>, AlpacaWsError> {
    match format {
        WsFormat::Json => serde_json::from_slice::<Vec<AlpacaWsEvent>>(raw)
            .map_err(|e| AlpacaWsError::Parse(format!("invalid market data frame: {e}"))),
        WsFormat::Msgpack => rmp_serde::from_slice::<Vec<AlpacaWsEvent>>(raw)
            .map_err(|e| AlpacaWsError::Parse(format!("invalid market data frame: {e}"))),
    }
}

/// Parses a trade message into a Nautilus [`TradeTick`].
///
/// Alpaca trade prints carry no aggressor information, so the tick is stamped
/// [`AggressorSide::NoAggressor`].
///
/// # Errors
///
/// Returns an error if the price, size, or timestamp cannot be converted.
pub fn parse_ws_trade_tick(
    trade: &AlpacaWsTrade,
    info: &AlpacaInstrumentInfo,
    ts_init: UnixNanos,
) -> anyhow::Result<TradeTick> {
    let price = parse_price_from_f64(trade.price, info.price_precision)?;
    let size = parse_quantity_from_f64(trade.size as f64, info.size_precision)?;
    let ts_event = trade.timestamp.0;

    Ok(TradeTick::new(
        info.instrument_id,
        price,
        size,
        AggressorSide::NoAggressor,
        TradeId::new(trade.trade_id.to_string()),
        ts_event,
        ts_init,
    ))
}

/// Parses a quote message into a Nautilus [`QuoteTick`].
///
/// # Errors
///
/// Returns an error if any price, size, or the timestamp cannot be converted.
pub fn parse_ws_quote_tick(
    quote: &AlpacaWsQuote,
    info: &AlpacaInstrumentInfo,
    ts_init: UnixNanos,
) -> anyhow::Result<QuoteTick> {
    let bid_price = parse_price_from_f64(quote.bid_price, info.price_precision)?;
    let ask_price = parse_price_from_f64(quote.ask_price, info.price_precision)?;
    let bid_size = parse_quantity_from_f64(quote.bid_size as f64, info.size_precision)?;
    let ask_size = parse_quantity_from_f64(quote.ask_size as f64, info.size_precision)?;
    let ts_event = quote.timestamp.0;

    Ok(QuoteTick::new(
        info.instrument_id,
        bid_price,
        ask_price,
        bid_size,
        ask_size,
        ts_event,
        ts_init,
    ))
}

/// Parses a minute bar message into a Nautilus [`Bar`].
///
/// Alpaca stamps bars with their OPEN time; Nautilus convention is the close
/// time, so one minute is added. Only completed minute bars (`"T":"b"`) are
/// converted: daily bars are cumulative intraday snapshots and updated bars
/// are corrections to already-emitted minutes, neither of which maps onto an
/// immutable Nautilus [`Bar`], so the handler skips them.
///
/// # Errors
///
/// Returns an error if any price, the volume, or the timestamp cannot be
/// converted.
pub fn parse_ws_bar(
    bar: &AlpacaWsBar,
    info: &AlpacaInstrumentInfo,
    ts_init: UnixNanos,
) -> anyhow::Result<Bar> {
    let bar_type = BarType::new(
        info.instrument_id,
        *BAR_SPEC_1_MINUTE_LAST,
        AggregationSource::External,
    );
    let open = parse_price_from_f64(bar.open, info.price_precision)?;
    let high = parse_price_from_f64(bar.high, info.price_precision)?;
    let low = parse_price_from_f64(bar.low, info.price_precision)?;
    let close = parse_price_from_f64(bar.close, info.price_precision)?;
    let volume = parse_quantity_from_f64(bar.volume as f64, info.size_precision)?;
    let ts_event = UnixNanos::from(bar.timestamp.0.as_u64() + NANOS_PER_MINUTE);

    Ok(Bar::new(
        bar_type, open, high, low, close, volume, ts_event, ts_init,
    ))
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::InstrumentId;
    use rstest::rstest;

    use super::*;
    use crate::common::{consts::ALPACA_VENUE, testing::load_test_json};

    fn instrument_info(symbol: &str) -> AlpacaInstrumentInfo {
        AlpacaInstrumentInfo {
            instrument_id: InstrumentId::new(
                nautilus_model::identifiers::Symbol::new(symbol),
                *ALPACA_VENUE,
            ),
            price_precision: 2,
            size_precision: 0,
        }
    }

    #[rstest]
    fn test_parse_batched_market_data_fixture() {
        let json = load_test_json("ws_market_data_batch.json");
        let events = parse_ws_events(json.as_bytes(), WsFormat::Json).unwrap();
        assert_eq!(events.len(), 5);

        let trades = events
            .iter()
            .filter(|e| matches!(e, AlpacaWsEvent::Trade(_)))
            .count();
        let quotes = events
            .iter()
            .filter(|e| matches!(e, AlpacaWsEvent::Quote(_)))
            .count();
        let bars = events
            .iter()
            .filter(|e| matches!(e, AlpacaWsEvent::MinuteBar(_)))
            .count();
        assert_eq!(trades, 2);
        assert_eq!(quotes, 2);
        assert_eq!(bars, 1);
    }

    #[rstest]
    fn test_parse_trade_tick_values() {
        let json = load_test_json("ws_market_data_batch.json");
        let events = parse_ws_events(json.as_bytes(), WsFormat::Json).unwrap();
        let info = instrument_info("AAPL");
        let ts_init = UnixNanos::from(1);

        let AlpacaWsEvent::Trade(trade) = &events[0] else {
            panic!("expected trade first in fixture");
        };
        let tick = parse_ws_trade_tick(trade, &info, ts_init).unwrap();

        assert_eq!(tick.instrument_id, info.instrument_id);
        assert_eq!(tick.price.to_string(), "189.05");
        assert_eq!(tick.size.to_string(), "100");
        assert_eq!(tick.aggressor_side, AggressorSide::NoAggressor);
        assert_eq!(tick.trade_id.to_string(), "96921");
        assert_eq!(tick.ts_event.as_u64() % 1_000_000_000, 123_456_789);
        assert_eq!(tick.ts_init, ts_init);
    }

    #[rstest]
    fn test_parse_quote_tick_values() {
        let json = load_test_json("ws_market_data_batch.json");
        let events = parse_ws_events(json.as_bytes(), WsFormat::Json).unwrap();
        let info = instrument_info("MSFT");
        let ts_init = UnixNanos::from(1);

        let quote = events
            .iter()
            .find_map(|e| match e {
                AlpacaWsEvent::Quote(quote) => Some(quote),
                _ => None,
            })
            .expect("fixture contains a quote");
        let tick = parse_ws_quote_tick(quote, &info, ts_init).unwrap();

        assert_eq!(tick.bid_price.to_string(), "473.11");
        assert_eq!(tick.ask_price.to_string(), "473.13");
        assert_eq!(tick.bid_size.to_string(), "3");
        assert_eq!(tick.ask_size.to_string(), "2");
    }

    #[rstest]
    fn test_parse_bar_close_time_offset() {
        let json = load_test_json("ws_market_data_batch.json");
        let events = parse_ws_events(json.as_bytes(), WsFormat::Json).unwrap();
        let info = instrument_info("AAPL");

        let bar = events
            .iter()
            .find_map(|e| match e {
                AlpacaWsEvent::MinuteBar(bar) => Some(bar),
                _ => None,
            })
            .expect("fixture contains a minute bar");
        let parsed = parse_ws_bar(bar, &info, UnixNanos::from(1)).unwrap();

        assert_eq!(
            parsed.ts_event.as_u64(),
            bar.timestamp.0.as_u64() + 60_000_000_000
        );
        assert_eq!(parsed.open.to_string(), "189.01");
        assert_eq!(parsed.volume.to_string(), "49378");
        assert_eq!(parsed.bar_type.spec().aggregation, BarAggregation::Minute);
        assert_eq!(
            parsed.bar_type.aggregation_source(),
            AggregationSource::External
        );
    }

    #[rstest]
    fn test_parse_sub_penny_trade_rounds_to_precision() {
        let json = r#"[{"T":"t","S":"AAPL","i":1,"x":"V","p":189.0501,"s":10,"c":[],"z":"C","t":"2026-01-05T14:30:00Z"}]"#;
        let events = parse_ws_events(json.as_bytes(), WsFormat::Json).unwrap();
        let AlpacaWsEvent::Trade(trade) = &events[0] else {
            panic!("expected trade");
        };
        let tick =
            parse_ws_trade_tick(trade, &instrument_info("AAPL"), UnixNanos::from(1)).unwrap();
        assert_eq!(tick.price.to_string(), "189.05");
    }

    #[rstest]
    fn test_parse_invalid_payload_errors() {
        assert!(parse_ws_events(b"not-json", WsFormat::Json).is_err());
        assert!(parse_ws_events(b"{\"T\":\"t\"}", WsFormat::Json).is_err()); // not an array
        assert!(parse_ws_events(b"not-msgpack", WsFormat::Msgpack).is_err());
    }

    #[rstest]
    fn test_parse_invalid_timestamp_errors() {
        // Timestamps convert during deserialization, so a garbage value fails
        // the whole-frame parse.
        let json =
            r#"[{"T":"t","S":"AAPL","i":1,"x":"V","p":1.0,"s":1,"c":[],"z":"C","t":"garbage"}]"#;
        assert!(parse_ws_events(json.as_bytes(), WsFormat::Json).is_err());
    }

    #[rstest]
    fn test_parse_msgpack_transcoded_fixture_matches_json() {
        let json = load_test_json("ws_market_data_batch.json");
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let msgpack = rmp_serde::to_vec_named(&value).unwrap();

        let json_events = parse_ws_events(json.as_bytes(), WsFormat::Json).unwrap();
        let msgpack_events = parse_ws_events(&msgpack, WsFormat::Msgpack).unwrap();

        assert_eq!(json_events.len(), msgpack_events.len());
        let info = instrument_info("AAPL");
        let ts_init = UnixNanos::from(1);
        for (json_event, msgpack_event) in json_events.iter().zip(msgpack_events.iter()) {
            match (json_event, msgpack_event) {
                (AlpacaWsEvent::Trade(a), AlpacaWsEvent::Trade(b)) => {
                    let tick_a = parse_ws_trade_tick(a, &info, ts_init).unwrap();
                    let tick_b = parse_ws_trade_tick(b, &info, ts_init).unwrap();
                    assert_eq!(tick_a, tick_b);
                }
                (AlpacaWsEvent::Quote(a), AlpacaWsEvent::Quote(b)) => {
                    let tick_a = parse_ws_quote_tick(a, &info, ts_init).unwrap();
                    let tick_b = parse_ws_quote_tick(b, &info, ts_init).unwrap();
                    assert_eq!(tick_a, tick_b);
                }
                (AlpacaWsEvent::MinuteBar(a), AlpacaWsEvent::MinuteBar(b)) => {
                    let bar_a = parse_ws_bar(a, &info, ts_init).unwrap();
                    let bar_b = parse_ws_bar(b, &info, ts_init).unwrap();
                    assert_eq!(bar_a, bar_b);
                }
                (a, b) => panic!("event kind mismatch: {a:?} vs {b:?}"),
            }
        }
    }
}
