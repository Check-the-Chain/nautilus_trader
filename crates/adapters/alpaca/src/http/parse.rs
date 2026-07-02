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

//! Conversion helpers from Alpaca REST payloads to Nautilus domain types.

use std::str::FromStr;

use anyhow::Context;
use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    data::{Bar, BarType, QuoteTick, TradeTick},
    enums::{AggressorSide, OrderSide, OrderStatus, OrderType, TimeInForce},
    identifiers::{AccountId, ClientOrderId, InstrumentId, Symbol, TradeId, VenueOrderId},
    instruments::{Equity, InstrumentAny},
    reports::OrderStatusReport,
    types::{Currency, Price, Quantity},
};
use rust_decimal::Decimal;

use crate::{
    common::{
        consts::ALPACA_VENUE,
        enums::AlpacaAssetClass,
        parse::{
            parse_price, parse_price_from_f64, parse_quantity, parse_quantity_from_f64,
            parse_rfc3339_timestamp,
        },
    },
    http::models::{
        AlpacaAsset, AlpacaBar, AlpacaHistoricalQuote, AlpacaHistoricalTrade, AlpacaOrder,
    },
};

/// Price precision for US equities at or above $1.00 (sub-penny rule).
pub const US_EQUITY_PRICE_PRECISION: u8 = 2;

/// Quantity precision for fractionable assets (up to 9 decimal places).
pub const FRACTIONAL_SIZE_PRECISION: u8 = 9;

/// Parses an Alpaca US equity asset into a Nautilus [`Equity`] instrument.
///
/// Alpaca has no per-asset equity tick size; the venue-wide sub-penny rule is
/// max 2 decimals at or above $1.00 (4 below), so the price increment is fixed
/// at `0.01`. Fractionable assets accept quantities with up to 9 decimals,
/// carried via `min_quantity`.
///
/// # Errors
///
/// Returns an error if the asset is not a US equity or a field fails to parse.
pub fn parse_equity_instrument(
    asset: &AlpacaAsset,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    anyhow::ensure!(
        asset.class == AlpacaAssetClass::UsEquity,
        "unsupported asset class {} for symbol {}",
        asset.class,
        asset.symbol,
    );

    let symbol = Symbol::from_ustr_unchecked(asset.symbol);
    let instrument_id = InstrumentId::new(symbol, *ALPACA_VENUE);

    let (min_quantity, size_increment) = if asset.fractionable {
        let fractional = Quantity::new(1e-9, FRACTIONAL_SIZE_PRECISION);
        (fractional, fractional)
    } else {
        (Quantity::from(1), Quantity::from(1))
    };
    // Equity has no explicit size precision; min_quantity carries it so
    // downstream sizing uses 9 decimals for fractionable assets.
    let _ = size_increment;

    let margin_init = asset
        .margin_requirement_long
        .as_deref()
        .and_then(parse_margin_percent);
    let margin_maint = margin_init;

    let instrument = Equity::new(
        instrument_id,
        symbol,
        None, // isin
        Currency::USD(),
        US_EQUITY_PRICE_PRECISION,
        Price::new(0.01, US_EQUITY_PRICE_PRECISION),
        Some(Quantity::from(1)), // lot_size
        None,                    // max_quantity
        Some(min_quantity),
        None, // max_price
        None, // min_price
        margin_init,
        margin_maint,
        None, // maker_fee
        None, // taker_fee
        None, // tick_scheme
        Some(asset_info(asset)),
        ts_init,
        ts_init,
    );

    Ok(InstrumentAny::from(instrument))
}

/// Builds the instrument `info` map carrying Alpaca asset flags needed
/// downstream (fractionability drives order sizing).
fn asset_info(asset: &AlpacaAsset) -> Params {
    let mut info = Params::new();
    info.insert("id".to_string(), serde_json::Value::from(asset.id.clone()));
    info.insert(
        "exchange".to_string(),
        serde_json::Value::from(asset.exchange.clone()),
    );
    info.insert(
        "tradable".to_string(),
        serde_json::Value::from(asset.tradable),
    );
    info.insert(
        "marginable".to_string(),
        serde_json::Value::from(asset.marginable),
    );
    info.insert(
        "shortable".to_string(),
        serde_json::Value::from(asset.shortable),
    );
    info.insert(
        "fractionable".to_string(),
        serde_json::Value::from(asset.fractionable),
    );
    if !asset.attributes.is_empty() {
        info.insert(
            "attributes".to_string(),
            serde_json::Value::from(asset.attributes.clone()),
        );
    }
    info
}

fn parse_margin_percent(value: &str) -> Option<Decimal> {
    Decimal::from_str(value.trim())
        .ok()
        .map(|pct| pct / Decimal::ONE_HUNDRED)
}

/// Parses an Alpaca historical bar into a Nautilus [`Bar`].
///
/// # Errors
///
/// Returns an error if a field fails to parse or the bar is invalid.
pub fn parse_bar(
    bar: &AlpacaBar,
    bar_type: BarType,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<Bar> {
    let ts_event = parse_rfc3339_timestamp(&bar.t, "bar.t")?;
    let open = parse_price_from_f64(bar.o, price_precision)?;
    let high = parse_price_from_f64(bar.h, price_precision)?;
    let low = parse_price_from_f64(bar.l, price_precision)?;
    let close = parse_price_from_f64(bar.c, price_precision)?;
    let volume = parse_quantity_from_f64(bar.v as f64, size_precision)?;

    Bar::new_checked(bar_type, open, high, low, close, volume, ts_event, ts_init)
        .context("failed to construct Bar from Alpaca bar")
}

/// Parses an Alpaca historical trade into a Nautilus [`TradeTick`].
///
/// Alpaca trades carry no aggressor information, so the aggressor side is
/// always [`AggressorSide::NoAggressor`].
///
/// # Errors
///
/// Returns an error if a field fails to parse or the tick is invalid.
pub fn parse_historical_trade(
    trade: &AlpacaHistoricalTrade,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<TradeTick> {
    let price = parse_price_from_f64(trade.p, price_precision)?;
    let size = parse_quantity_from_f64(trade.s as f64, size_precision)?;
    let trade_id =
        TradeId::new_checked(trade.i.to_string()).context("invalid Alpaca trade identifier")?;
    let ts_event = parse_rfc3339_timestamp(&trade.t, "trade.t")?;

    TradeTick::new_checked(
        instrument_id,
        price,
        size,
        AggressorSide::NoAggressor,
        trade_id,
        ts_event,
        ts_init,
    )
    .context("failed to construct TradeTick from Alpaca trade")
}

/// Parses an Alpaca historical quote into a Nautilus [`QuoteTick`].
///
/// A zero bid or ask price means that side has no active quote; such rows are
/// rejected because Nautilus quotes require both sides.
///
/// # Errors
///
/// Returns an error if a field fails to parse, a side is missing, or the tick
/// is invalid.
pub fn parse_historical_quote(
    quote: &AlpacaHistoricalQuote,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<QuoteTick> {
    anyhow::ensure!(
        quote.bp > 0.0 && quote.ap > 0.0,
        "one-sided quote for {instrument_id} (bp={}, ap={})",
        quote.bp,
        quote.ap,
    );
    let bid_price = parse_price_from_f64(quote.bp, price_precision)?;
    let ask_price = parse_price_from_f64(quote.ap, price_precision)?;
    let bid_size = parse_quantity_from_f64(quote.bs as f64, size_precision)?;
    let ask_size = parse_quantity_from_f64(quote.ask_size as f64, size_precision)?;
    let ts_event = parse_rfc3339_timestamp(&quote.t, "quote.t")?;

    QuoteTick::new_checked(
        instrument_id,
        bid_price,
        ask_price,
        bid_size,
        ask_size,
        ts_event,
        ts_init,
    )
    .context("failed to construct QuoteTick from Alpaca quote")
}

/// Parses an Alpaca order into a Nautilus [`OrderStatusReport`].
///
/// Notional orders carry no `qty`; the filled quantity substitutes as the
/// reported quantity in that case (it grows as the order fills).
///
/// # Errors
///
/// Returns an error if required fields are missing or fail to parse.
pub fn parse_order_status_report(
    order: &AlpacaOrder,
    account_id: AccountId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    let symbol = order
        .symbol
        .context("order has no symbol (multi-leg parent orders are unsupported)")?;
    let instrument_id = InstrumentId::new(Symbol::from_ustr_unchecked(symbol), *ALPACA_VENUE);

    let client_order_id = order
        .client_order_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(ClientOrderId::new);
    let venue_order_id = VenueOrderId::new(order.id.as_str());

    let order_side: OrderSide = order.side.map(Into::into).context("order has no side")?;
    let order_type: OrderType = order
        .order_type
        .map(Into::into)
        .context("order has no type")?;
    let time_in_force: TimeInForce = order
        .time_in_force
        .map(Into::into)
        .context("order has no time in force")?;
    let order_status: OrderStatus = order.status.into();

    let filled_qty = match order.filled_qty.as_deref() {
        Some(value) => parse_quantity(value, size_precision)?,
        None => Quantity::new(0.0, size_precision),
    };
    let quantity = match order.qty.as_deref() {
        Some(value) => parse_quantity(value, size_precision)?,
        None => filled_qty,
    };

    let ts_accepted = match order
        .submitted_at
        .as_deref()
        .or(order.created_at.as_deref())
    {
        Some(value) => parse_rfc3339_timestamp(value, "order.submitted_at")?,
        None => ts_init,
    };
    let ts_last = match order
        .updated_at
        .as_deref()
        .or(order.filled_at.as_deref())
        .or(order.created_at.as_deref())
    {
        Some(value) => parse_rfc3339_timestamp(value, "order.updated_at")?,
        None => ts_accepted,
    };

    let mut report = OrderStatusReport::new(
        account_id,
        instrument_id,
        client_order_id,
        venue_order_id,
        order_side,
        order_type,
        time_in_force,
        order_status,
        quantity,
        filled_qty,
        ts_accepted,
        ts_last,
        ts_init,
        None,
    );

    if let Some(limit_price) = order.limit_price.as_deref() {
        report = report.with_price(parse_price(limit_price, price_precision)?);
    }

    if let Some(stop_price) = order.stop_price.as_deref() {
        report = report.with_trigger_price(parse_price(stop_price, price_precision)?);
    }

    if let Some(avg_px) = order.filled_avg_price.as_deref()
        && let Ok(decimal) = Decimal::from_str(avg_px)
    {
        report.avg_px = Some(decimal);
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use nautilus_model::{enums::AggregationSource, instruments::Instrument};
    use rstest::rstest;

    use super::*;
    use crate::common::testing::load_test_json;

    fn instrument_id() -> InstrumentId {
        InstrumentId::from("AAPL.ALPACA")
    }

    #[rstest]
    fn test_parse_equity_instrument_fractionable() {
        let json = load_test_json("http_get_assets.json");
        let assets: Vec<AlpacaAsset> = serde_json::from_str(&json).unwrap();

        let instrument = parse_equity_instrument(&assets[0], UnixNanos::default()).unwrap();

        assert_eq!(instrument.id(), instrument_id());
        assert_eq!(instrument.price_precision(), 2);
        assert_eq!(instrument.price_increment(), Price::new(0.01, 2));
        assert_eq!(instrument.quote_currency(), Currency::USD());
        assert_eq!(
            instrument.min_quantity(),
            Some(Quantity::new(1e-9, FRACTIONAL_SIZE_PRECISION))
        );
    }

    #[rstest]
    fn test_parse_equity_instrument_non_fractionable() {
        let json = load_test_json("http_get_assets.json");
        let assets: Vec<AlpacaAsset> = serde_json::from_str(&json).unwrap();

        let instrument = parse_equity_instrument(&assets[1], UnixNanos::default()).unwrap();

        assert_eq!(instrument.id(), InstrumentId::from("BRK.A.ALPACA"));
        assert_eq!(instrument.min_quantity(), Some(Quantity::from(1)));
    }

    #[rstest]
    fn test_parse_equity_instrument_rejects_crypto() {
        let json = load_test_json("http_get_assets.json");
        let mut assets: Vec<AlpacaAsset> = serde_json::from_str(&json).unwrap();
        assets[0].class = AlpacaAssetClass::Crypto;

        assert!(parse_equity_instrument(&assets[0], UnixNanos::default()).is_err());
    }

    #[rstest]
    fn test_parse_bar() {
        let json = load_test_json("http_get_stock_bars.json");
        let response: crate::http::models::AlpacaBarsResponse =
            serde_json::from_str(&json).unwrap();
        let bar_type = BarType::new(
            instrument_id(),
            nautilus_model::data::bar::BAR_SPEC_1_MINUTE_LAST,
            AggregationSource::External,
        );

        let bars = response.bars.get("AAPL").unwrap();
        let bar = parse_bar(&bars[0], bar_type, 2, 0, UnixNanos::default()).unwrap();

        assert_eq!(bar.open, Price::new(189.01, 2));
        assert_eq!(bar.close, Price::new(189.12, 2));
        assert_eq!(bar.volume, Quantity::from(12_345));
    }

    #[rstest]
    fn test_parse_historical_trade() {
        let json = load_test_json("http_get_stock_trades.json");
        let response: crate::http::models::AlpacaTradesResponse =
            serde_json::from_str(&json).unwrap();

        let trades = response.trades.get("AAPL").unwrap();
        let tick = parse_historical_trade(&trades[0], instrument_id(), 2, 0, UnixNanos::default())
            .unwrap();

        assert_eq!(tick.instrument_id, instrument_id());
        assert_eq!(tick.price, Price::new(189.05, 2));
        assert_eq!(tick.size, Quantity::from(100));
        assert_eq!(tick.aggressor_side, AggressorSide::NoAggressor);
        assert_eq!(tick.trade_id, TradeId::new("52983525029461"));
    }

    #[rstest]
    fn test_parse_historical_quote() {
        let json = load_test_json("http_get_stock_quotes.json");
        let response: crate::http::models::AlpacaQuotesResponse =
            serde_json::from_str(&json).unwrap();

        let quotes = response.quotes.get("AAPL").unwrap();
        let tick = parse_historical_quote(&quotes[0], instrument_id(), 2, 0, UnixNanos::default())
            .unwrap();

        assert_eq!(tick.bid_price, Price::new(189.04, 2));
        assert_eq!(tick.ask_price, Price::new(189.06, 2));
        assert_eq!(tick.bid_size, Quantity::from(200));
        assert_eq!(tick.ask_size, Quantity::from(300));
    }

    #[rstest]
    fn test_parse_historical_quote_rejects_one_sided() {
        let quote = AlpacaHistoricalQuote {
            t: "2026-01-05T14:30:00.123456789Z".to_string(),
            bx: "V".to_string(),
            bp: 0.0,
            bs: 0,
            ax: "V".to_string(),
            ap: 189.06,
            ask_size: 100,
            c: vec![],
            z: Some("C".to_string()),
        };

        assert!(
            parse_historical_quote(&quote, instrument_id(), 2, 0, UnixNanos::default()).is_err()
        );
    }

    #[rstest]
    fn test_parse_order_status_report() {
        let json = load_test_json("http_get_order.json");
        let order: AlpacaOrder = serde_json::from_str(&json).unwrap();
        let account_id = AccountId::new("ALPACA-001");

        let report =
            parse_order_status_report(&order, account_id, 2, 0, UnixNanos::default()).unwrap();

        assert_eq!(report.account_id, account_id);
        assert_eq!(report.instrument_id, instrument_id());
        assert_eq!(
            report.venue_order_id,
            VenueOrderId::new("61e69015-8549-4bfd-b9c3-01e75843f47d")
        );
        assert_eq!(
            report.client_order_id,
            Some(ClientOrderId::new("O-20260105-001"))
        );
        assert_eq!(report.order_side, OrderSide::Buy);
        assert_eq!(report.order_type, OrderType::Limit);
        assert_eq!(report.time_in_force, TimeInForce::Day);
        assert_eq!(report.order_status, OrderStatus::Filled);
        assert_eq!(report.quantity, Quantity::from(10));
        assert_eq!(report.filled_qty, Quantity::from(10));
        assert_eq!(report.price, Some(Price::new(189.05, 2)));
        assert_eq!(report.avg_px, Some(Decimal::from_str("189.04").unwrap()));
    }

    #[rstest]
    fn test_parse_order_status_report_notional_uses_filled_qty() {
        let json = load_test_json("http_get_order.json");
        let mut order: AlpacaOrder = serde_json::from_str(&json).unwrap();
        order.qty = None;
        order.notional = Some("1890.50".to_string());
        order.filled_qty = Some("5".to_string());

        let report = parse_order_status_report(
            &order,
            AccountId::new("ALPACA-001"),
            2,
            0,
            UnixNanos::default(),
        )
        .unwrap();

        assert_eq!(report.quantity, Quantity::from(5));
    }
}
