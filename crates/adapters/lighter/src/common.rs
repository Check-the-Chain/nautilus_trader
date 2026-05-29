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

//! Shared Nautilus domain parsing and metadata helpers for the Lighter adapter.

use ahash::AHashMap;
use blake2::{
    Blake2bVar,
    digest::{Update, VariableOutput},
};
use nautilus_core::{Params, UUID4, UnixNanos};
use nautilus_model::{
    data::{
        Bar, BarType, FundingRateUpdate, IndexPriceUpdate, MarkPriceUpdate, OrderBookDelta,
        OrderBookDeltas, QuoteTick, TradeTick,
    },
    enums::{
        AggressorSide, BarAggregation, BookAction, LiquiditySide, OrderSide, OrderStatus,
        OrderType, PositionSideSpecified, PriceType, RecordFlag, TimeInForce, TriggerType,
    },
    identifiers::{AccountId, ClientOrderId, InstrumentId, TradeId, Venue, VenueOrderId},
    instruments::{CryptoPerpetual, CurrencyPair, Instrument, InstrumentAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use rust_decimal::{Decimal, RoundingStrategy, prelude::ToPrimitive};
use serde_json::Value;

use crate::constants::{CROSS_MARGIN, ISOLATED_MARGIN};
use crate::http::client::LighterHttpClient;
use crate::models::{
    account::{AccountPosition, DetailedAccount},
    asset::Asset,
    candle::Candle,
    funding::FundingRate,
    market::PerpsMarketStats,
    order::{Order, OrderStatus as LighterOrderStatus},
    order_book::{PerpsOrderBookDetail, PriceLevel, SpotOrderBookDetail},
    trade::Trade,
    ws::WsTickerData,
};
use crate::normalize::{
    funding::{current_update_from_market_stats, historical_update_from_funding_rate_endpoint},
    timestamp::epoch_to_unix_nanos,
};

pub const LIGHTER: &str = "LIGHTER";
pub const LIGHTER_PERP_SUFFIX: &str = "PERP";
pub const LIGHTER_SPOT_SUFFIX: &str = "SPOT";
pub const LIGHTER_SETTLEMENT_CURRENCY: &str = "USDC";
pub const LIGHTER_FEE_SCALE: i64 = 1_000_000;
pub use crate::normalize::funding::LIGHTER_FUNDING_INTERVAL_MINS;
pub const LIGHTER_MAX_CLIENT_ORDER_INDEX: u64 = (1_u64 << 48) - 1;

#[derive(Clone, Debug)]
pub enum LighterMarketStatUpdate {
    Mark(MarkPriceUpdate),
    Index(IndexPriceUpdate),
    Funding(FundingRateUpdate),
}

pub fn venue() -> Venue {
    Venue::from(LIGHTER)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LighterMarketType {
    Perp,
    Spot,
}

impl LighterMarketType {
    #[must_use]
    pub fn is_perp(self) -> bool {
        matches!(self, Self::Perp)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LighterMarketMarginMode {
    Cross,
    Isolated,
    Other(i64),
}

impl LighterMarketMarginMode {
    #[must_use]
    pub const fn from_raw(value: i64) -> Self {
        if value == CROSS_MARGIN as i64 {
            Self::Cross
        } else if value == ISOLATED_MARGIN as i64 {
            Self::Isolated
        } else {
            Self::Other(value)
        }
    }

    #[must_use]
    pub const fn as_raw(self) -> i64 {
        match self {
            Self::Cross => CROSS_MARGIN as i64,
            Self::Isolated => ISOLATED_MARGIN as i64,
            Self::Other(value) => value,
        }
    }

    #[must_use]
    pub const fn supports_cross_margin(self) -> bool {
        matches!(self, Self::Cross)
    }
}

#[derive(Clone, Debug)]
pub struct LighterInstrumentMeta {
    pub market_id: i64,
    pub instrument: InstrumentAny,
    pub market_type: LighterMarketType,
    pub price_precision: u8,
    pub size_precision: u8,
}

impl LighterInstrumentMeta {
    #[must_use]
    pub fn market_margin_mode(&self) -> Option<LighterMarketMarginMode> {
        instrument_info(&self.instrument)
            .and_then(|info| info.get("market_config"))
            .and_then(|value| value.as_object())
            .and_then(|market_config| market_config.get("market_margin_mode"))
            .and_then(Value::as_i64)
            .map(LighterMarketMarginMode::from_raw)
    }

    #[must_use]
    pub fn supports_cross_margin(&self) -> bool {
        self.market_margin_mode()
            .is_none_or(LighterMarketMarginMode::supports_cross_margin)
    }
}

#[derive(Clone, Debug, Default)]
pub struct LighterInstrumentRegistry {
    by_market_id: AHashMap<i64, LighterInstrumentMeta>,
    by_instrument_id: AHashMap<InstrumentId, LighterInstrumentMeta>,
}

impl LighterInstrumentRegistry {
    pub fn insert(&mut self, meta: LighterInstrumentMeta) {
        self.by_instrument_id
            .insert(meta.instrument.id(), meta.clone());
        self.by_market_id.insert(meta.market_id, meta);
    }

    #[must_use]
    pub fn instrument_for_market_id(&self, market_id: i64) -> Option<InstrumentAny> {
        self.by_market_id
            .get(&market_id)
            .map(|meta| meta.instrument.clone())
    }

    #[must_use]
    pub fn meta_for_market_id(&self, market_id: i64) -> Option<&LighterInstrumentMeta> {
        self.by_market_id.get(&market_id)
    }

    #[must_use]
    pub fn meta_for_instrument_id(
        &self,
        instrument_id: &InstrumentId,
    ) -> Option<&LighterInstrumentMeta> {
        self.by_instrument_id.get(instrument_id)
    }

    #[must_use]
    pub fn instruments(&self) -> Vec<InstrumentAny> {
        self.by_market_id
            .values()
            .map(|meta| meta.instrument.clone())
            .collect()
    }

    #[must_use]
    pub fn market_ids(&self) -> Vec<i64> {
        let mut ids: Vec<_> = self.by_market_id.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub fn clear(&mut self) {
        self.by_market_id.clear();
        self.by_instrument_id.clear();
    }
}

#[allow(clippy::missing_const_for_fn)]
fn instrument_info(instrument: &InstrumentAny) -> Option<&Params> {
    match instrument {
        InstrumentAny::Betting(instrument) => instrument.info.as_ref(),
        InstrumentAny::BinaryOption(instrument) => instrument.info.as_ref(),
        InstrumentAny::Cfd(instrument) => instrument.info.as_ref(),
        InstrumentAny::Commodity(instrument) => instrument.info.as_ref(),
        InstrumentAny::CryptoFuture(instrument) => instrument.info.as_ref(),
        InstrumentAny::CryptoOption(instrument) => instrument.info.as_ref(),
        InstrumentAny::CryptoPerpetual(instrument) => instrument.info.as_ref(),
        InstrumentAny::CurrencyPair(instrument) => instrument.info.as_ref(),
        InstrumentAny::Equity(instrument) => instrument.info.as_ref(),
        InstrumentAny::FuturesContract(instrument) => instrument.info.as_ref(),
        InstrumentAny::FuturesSpread(instrument) => instrument.info.as_ref(),
        InstrumentAny::IndexInstrument(instrument) => instrument.info.as_ref(),
        InstrumentAny::OptionContract(instrument) => instrument.info.as_ref(),
        InstrumentAny::OptionSpread(instrument) => instrument.info.as_ref(),
        InstrumentAny::PerpetualContract(instrument) => instrument.info.as_ref(),
        InstrumentAny::TokenizedAsset(instrument) => instrument.info.as_ref(),
    }
}

pub async fn load_instrument_registry(
    http_client: &LighterHttpClient,
) -> anyhow::Result<LighterInstrumentRegistry> {
    let rest = http_client.rest();
    let asset_details = rest.get_asset_details().await?;

    let assets_by_id: AHashMap<i64, String> = asset_details
        .asset_details
        .into_iter()
        .map(|asset| (asset.asset_id, asset.symbol))
        .collect();

    let details = vec![rest.get_all_order_book_details().await?];

    let mut registry = LighterInstrumentRegistry::default();

    for detail in details {
        for perp in detail.order_book_details {
            if let Some(meta) = instrument_meta_from_perp_detail(&perp, &assets_by_id)? {
                registry.insert(meta);
            }
        }
        for spot in detail.spot_order_book_details {
            if let Some(meta) = instrument_meta_from_spot_detail(&spot, &assets_by_id)? {
                registry.insert(meta);
            }
        }
    }

    Ok(registry)
}

fn instrument_meta_from_perp_detail(
    detail: &PerpsOrderBookDetail,
    assets_by_id: &AHashMap<i64, String>,
) -> anyhow::Result<Option<LighterInstrumentMeta>> {
    let market_id = match detail.market_id {
        Some(market_id) => market_id,
        None => return Ok(None),
    };

    let (raw_symbol, base_code, quote_code) = resolve_symbol_metadata(
        LighterMarketType::Perp,
        detail.symbol.as_deref(),
        detail.base_asset_id,
        detail.quote_asset_id,
        assets_by_id,
    )?;

    let price_precision = detail
        .price_decimals
        .or(detail.supported_price_decimals)
        .unwrap_or(0) as u8;
    let size_precision = detail
        .size_decimals
        .or(detail.supported_size_decimals)
        .unwrap_or(0) as u8;

    let instrument_id = InstrumentId::new(
        format!("{raw_symbol}-{LIGHTER_PERP_SUFFIX}").into(),
        venue(),
    );
    let raw_symbol_value = raw_symbol.as_str().into();
    let base_currency =
        Currency::get_or_create_crypto_with_context(base_code.as_str(), Some("lighter perp base"));
    let quote_currency = Currency::get_or_create_crypto_with_context(
        quote_code.as_str(),
        Some("lighter perp quote"),
    );

    let info = detail_to_params(
        &serde_json::to_value(detail).unwrap_or(Value::Null),
        market_id,
        LighterMarketType::Perp,
        &raw_symbol,
        price_precision,
        size_precision,
    );

    let instrument = CryptoPerpetual::new(
        instrument_id,
        raw_symbol_value,
        base_currency,
        quote_currency,
        quote_currency,
        false,
        price_precision,
        size_precision,
        decimal_increment_price(price_precision),
        decimal_increment_qty(size_precision),
        None,
        Some(decimal_increment_qty(size_precision)),
        None,
        detail
            .min_base_amount
            .map(|value| qty_from_f64(value, size_precision)),
        None,
        None,
        None,
        None,
        detail
            .default_initial_margin_fraction
            .map(|value| Decimal::new(value, 4)),
        detail
            .maintenance_margin_fraction
            .map(|value| Decimal::new(value, 4)),
        detail.maker_fee.map(decimal_from_f64),
        detail.taker_fee.map(decimal_from_f64),
        info,
        UnixNanos::default(),
        UnixNanos::default(),
    )
    .into_any();

    Ok(Some(LighterInstrumentMeta {
        market_id,
        instrument,
        market_type: LighterMarketType::Perp,
        price_precision,
        size_precision,
    }))
}

fn instrument_meta_from_spot_detail(
    detail: &SpotOrderBookDetail,
    assets_by_id: &AHashMap<i64, String>,
) -> anyhow::Result<Option<LighterInstrumentMeta>> {
    let market_id = match detail.market_id {
        Some(market_id) => market_id,
        None => return Ok(None),
    };

    let (raw_symbol, base_code, quote_code) = resolve_symbol_metadata(
        LighterMarketType::Spot,
        detail.symbol.as_deref(),
        detail.base_asset_id,
        detail.quote_asset_id,
        assets_by_id,
    )?;

    let price_precision = detail
        .price_decimals
        .or(detail.supported_price_decimals)
        .unwrap_or(0) as u8;
    let size_precision = detail
        .size_decimals
        .or(detail.supported_size_decimals)
        .unwrap_or(0) as u8;

    let instrument_id = InstrumentId::new(
        format!("{raw_symbol}-{LIGHTER_SPOT_SUFFIX}").into(),
        venue(),
    );
    let raw_symbol_value = raw_symbol.as_str().into();
    let base_currency =
        Currency::get_or_create_crypto_with_context(base_code.as_str(), Some("lighter spot base"));
    let quote_currency = Currency::get_or_create_crypto_with_context(
        quote_code.as_str(),
        Some("lighter spot quote"),
    );

    let info = detail_to_params(
        &serde_json::to_value(detail).unwrap_or(Value::Null),
        market_id,
        LighterMarketType::Spot,
        &raw_symbol,
        price_precision,
        size_precision,
    );

    let instrument = CurrencyPair::new(
        instrument_id,
        raw_symbol_value,
        base_currency,
        quote_currency,
        price_precision,
        size_precision,
        decimal_increment_price(price_precision),
        decimal_increment_qty(size_precision),
        None,
        Some(decimal_increment_qty(size_precision)),
        None,
        detail
            .min_base_amount
            .map(|value| qty_from_f64(value, size_precision)),
        None,
        None,
        None,
        None,
        Some(Decimal::ZERO),
        Some(Decimal::ZERO),
        detail.maker_fee.map(decimal_from_f64),
        detail.taker_fee.map(decimal_from_f64),
        info,
        UnixNanos::default(),
        UnixNanos::default(),
    )
    .into_any();

    Ok(Some(LighterInstrumentMeta {
        market_id,
        instrument,
        market_type: LighterMarketType::Spot,
        price_precision,
        size_precision,
    }))
}

fn resolve_symbol_metadata(
    market_type: LighterMarketType,
    symbol: Option<&str>,
    base_asset_id: Option<i64>,
    quote_asset_id: Option<i64>,
    assets_by_id: &AHashMap<i64, String>,
) -> anyhow::Result<(String, String, String)> {
    let mut base_code = base_asset_id.and_then(|id| assets_by_id.get(&id)).cloned();
    let mut quote_code = quote_asset_id.and_then(|id| assets_by_id.get(&id)).cloned();

    let mut raw_symbol = symbol.unwrap_or_default().to_string();
    if raw_symbol.is_empty() {
        if let (Some(base), Some(quote)) = (&base_code, &quote_code) {
            raw_symbol = format!("{base}-{quote}");
        } else {
            anyhow::bail!("Unable to resolve Lighter symbol metadata");
        }
    }

    if base_code.is_none() || quote_code.is_none() {
        let normalized_symbol = raw_symbol.replace('/', "-");
        let mut parts = normalized_symbol.split('-');
        if base_code.is_none() {
            base_code = parts.next().map(ToString::to_string);
        }
        if quote_code.is_none() {
            quote_code = parts.next().map(ToString::to_string);
        }
        if market_type.is_perp() {
            base_code.get_or_insert_with(|| raw_symbol.clone());
            quote_code.get_or_insert_with(|| LIGHTER_SETTLEMENT_CURRENCY.to_string());
        }
    }

    match (base_code, quote_code) {
        (Some(base), Some(quote)) => Ok((raw_symbol, base, quote)),
        _ => anyhow::bail!("Unable to resolve Lighter base/quote metadata"),
    }
}

fn detail_to_params(
    detail: &Value,
    market_id: i64,
    market_type: LighterMarketType,
    raw_symbol: &str,
    price_precision: u8,
    size_precision: u8,
) -> Option<Params> {
    let Value::Object(mut map) = detail.clone() else {
        return None;
    };

    map.insert("market_id".to_string(), Value::from(market_id));
    map.insert(
        "market_type".to_string(),
        Value::from(if market_type.is_perp() {
            "perp"
        } else {
            "spot"
        }),
    );
    map.insert("raw_symbol".to_string(), Value::from(raw_symbol));
    map.insert("price_decimals".to_string(), Value::from(price_precision));
    map.insert("size_decimals".to_string(), Value::from(size_precision));

    serde_json::from_value(Value::Object(map)).ok()
}

#[must_use]
pub fn channel_market_id(channel: &str) -> Option<i64> {
    channel
        .split(['/', ':'])
        .rev()
        .find(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit()))
        .and_then(|part| part.parse::<i64>().ok())
}

#[must_use]
pub fn quote_tick_from_ticker(
    instrument: &InstrumentAny,
    ticker: &WsTickerData,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> Option<QuoteTick> {
    let (Some(bid_price), Some(ask_price)) = (ticker.b.price.as_deref(), ticker.a.price.as_deref())
    else {
        return None;
    };
    let bid_size = ticker.b.size.as_deref().map_or_else(
        || Quantity::zero(instrument.size_precision()),
        |size| qty_from_string(size, instrument.size_precision()),
    );
    let ask_size = ticker.a.size.as_deref().map_or_else(
        || Quantity::zero(instrument.size_precision()),
        |size| qty_from_string(size, instrument.size_precision()),
    );

    Some(QuoteTick::new(
        instrument.id(),
        price_from_string(bid_price, instrument.price_precision()),
        price_from_string(ask_price, instrument.price_precision()),
        bid_size,
        ask_size,
        ts_event,
        ts_init,
    ))
}

#[must_use]
pub fn trade_tick_from_trade(
    instrument: &InstrumentAny,
    trade: &Trade,
    ts_init: UnixNanos,
) -> TradeTick {
    let ts_event = epoch_to_unix_nanos(Some(trade.timestamp));
    let ts_event = if ts_event == UnixNanos::default() {
        ts_init
    } else {
        ts_event
    };
    let aggressor_side = if trade.is_maker_ask {
        AggressorSide::Buyer
    } else {
        AggressorSide::Seller
    };

    TradeTick::new(
        instrument.id(),
        price_from_string(&trade.price, instrument.price_precision()),
        qty_from_string(&trade.size, instrument.size_precision()),
        aggressor_side,
        TradeId::from(trade.trade_id.to_string()),
        ts_event,
        ts_init,
    )
}

#[must_use]
pub fn market_stats_to_updates(
    instrument: &InstrumentAny,
    market: &PerpsMarketStats,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> Vec<LighterMarketStatUpdate> {
    let mut updates = Vec::new();

    if let Some(mark_price) = market.mark_price {
        updates.push(LighterMarketStatUpdate::Mark(MarkPriceUpdate::new(
            instrument.id(),
            price_from_f64(mark_price, instrument.price_precision()),
            ts_event,
            ts_init,
        )));
    }
    if let Some(index_price) = market.index_price {
        updates.push(LighterMarketStatUpdate::Index(IndexPriceUpdate::new(
            instrument.id(),
            price_from_f64(index_price, instrument.price_precision()),
            ts_event,
            ts_init,
        )));
    }

    if let Some(update) = current_update_from_market_stats(instrument, market, ts_event, ts_init) {
        updates.push(LighterMarketStatUpdate::Funding(update));
    }

    updates
}

#[must_use]
pub fn funding_rate_update_from_history(
    instrument: &InstrumentAny,
    funding_rate: &FundingRate,
    ts_init: UnixNanos,
) -> Option<FundingRateUpdate> {
    historical_update_from_funding_rate_endpoint(instrument.id(), funding_rate, ts_init)
}

#[must_use]
pub fn funding_rate_updates_from_history(
    instrument: &InstrumentAny,
    funding_rates: &[FundingRate],
    market_id: i64,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
    limit: Option<usize>,
    ts_init: UnixNanos,
) -> Vec<FundingRateUpdate> {
    let start = start.map(|value| value.as_u64());
    let end = end.map(|value| value.as_u64());
    let mut updates = Vec::new();

    for funding_rate in funding_rates {
        if funding_rate.market_id != Some(market_id) {
            continue;
        }

        let Some(update) = funding_rate_update_from_history(instrument, funding_rate, ts_init)
        else {
            continue;
        };
        let ts_event = update.ts_event.as_u64();

        if let Some(start) = start
            && ts_event < start
        {
            continue;
        }
        if let Some(end) = end
            && ts_event > end
        {
            continue;
        }

        updates.push(update);
    }

    updates.sort_unstable_by_key(|update| update.ts_event);
    if let Some(limit) = limit {
        updates.truncate(limit);
    }

    updates
}

pub fn candles_to_bars(
    instrument: &InstrumentAny,
    bar_type: BarType,
    candles: &[Candle],
) -> anyhow::Result<Vec<Bar>> {
    let mut sorted = candles.to_vec();
    sorted.sort_by_key(|candle| candle.timestamp.unwrap_or_default());

    let bars = sorted
        .into_iter()
        .filter_map(|candle| {
            let (Some(open), Some(high), Some(low), Some(close)) =
                (candle.open, candle.high, candle.low, candle.close)
            else {
                return None;
            };

            let ts_event = candle
                .timestamp
                .map(|value| epoch_to_unix_nanos(Some(value)))
                .unwrap_or_default();

            Some(Bar::new(
                bar_type,
                price_from_f64(open, instrument.price_precision()),
                price_from_f64(high, instrument.price_precision()),
                price_from_f64(low, instrument.price_precision()),
                price_from_f64(close, instrument.price_precision()),
                qty_from_f64(
                    candle.volume.unwrap_or_default(),
                    instrument.size_precision(),
                ),
                ts_event,
                ts_event,
            ))
        })
        .collect();

    Ok(bars)
}

#[must_use]
pub fn order_book_snapshot_deltas(
    instrument: &InstrumentAny,
    bids: &[PriceLevel],
    asks: &[PriceLevel],
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> OrderBookDeltas {
    let total_levels = bids.len() + asks.len();
    let mut deltas = Vec::with_capacity(total_levels + 1);
    let mut clear = OrderBookDelta::clear(instrument.id(), sequence, ts_event, ts_init);
    if total_levels == 0 {
        clear.flags |= RecordFlag::F_LAST as u8;
    }
    deltas.push(clear);

    push_book_side_snapshot_deltas(
        &mut deltas,
        instrument,
        bids,
        OrderSide::Buy,
        sequence,
        ts_event,
        ts_init,
    );
    push_book_side_snapshot_deltas(
        &mut deltas,
        instrument,
        asks,
        OrderSide::Sell,
        sequence,
        ts_event,
        ts_init,
    );

    if total_levels > 0
        && let Some(last) = deltas.last_mut()
    {
        last.flags |= RecordFlag::F_LAST as u8;
    }

    OrderBookDeltas::new(instrument.id(), deltas)
}

#[must_use]
pub fn order_book_delta_updates(
    instrument: &InstrumentAny,
    bids: &[PriceLevel],
    asks: &[PriceLevel],
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> OrderBookDeltas {
    let mut deltas = Vec::with_capacity(bids.len() + asks.len());
    push_book_side_delta_updates(
        &mut deltas,
        instrument,
        bids,
        OrderSide::Buy,
        sequence,
        ts_event,
        ts_init,
    );
    push_book_side_delta_updates(
        &mut deltas,
        instrument,
        asks,
        OrderSide::Sell,
        sequence,
        ts_event,
        ts_init,
    );

    if let Some(last) = deltas.last_mut() {
        last.flags |= RecordFlag::F_LAST as u8;
    }

    OrderBookDeltas::new(instrument.id(), deltas)
}

pub fn populate_order_book(
    book: &mut nautilus_model::orderbook::OrderBook,
    instrument: &InstrumentAny,
    bids: &[PriceLevel],
    asks: &[PriceLevel],
    sequence: u64,
    ts_event: UnixNanos,
) {
    book.clear(sequence, ts_event);

    for level in bids {
        let order = book_order_from_level(instrument, level, OrderSide::Buy);
        book.add(order, RecordFlag::F_MBP as u8, sequence, ts_event);
    }

    for level in asks {
        let order = book_order_from_level(instrument, level, OrderSide::Sell);
        book.add(order, RecordFlag::F_MBP as u8, sequence, ts_event);
    }
}

#[must_use]
pub fn account_balances_from_assets(assets: &[Asset]) -> Vec<AccountBalance> {
    assets
        .iter()
        .map(|asset| {
            let currency = Currency::get_or_create_crypto_with_context(
                asset.symbol.as_str(),
                Some("lighter account asset"),
            );
            let margin_balance = (asset.symbol == LIGHTER_SETTLEMENT_CURRENCY)
                .then(|| asset.margin_balance_f64())
                .flatten();
            let total = margin_balance
                .or_else(|| asset.balance_f64())
                .unwrap_or_default();
            let locked = if margin_balance.is_some() {
                0.0
            } else {
                asset.locked_balance_f64().unwrap_or_default()
            };
            AccountBalance::new(
                Money::new(total, currency),
                Money::new(locked, currency),
                Money::new(total - locked, currency),
            )
        })
        .collect()
}

#[must_use]
pub fn margin_balances_from_positions(
    positions: &[AccountPosition],
    registry: &LighterInstrumentRegistry,
) -> Vec<MarginBalance> {
    positions
        .iter()
        .filter_map(|position| {
            let instrument = registry.instrument_for_market_id(position.market_id)?;
            Some(MarginBalance::new(
                Money::new(
                    position.allocated_margin.parse::<f64>().unwrap_or_default(),
                    instrument.quote_currency(),
                ),
                Money::new(0.0, instrument.quote_currency()),
                Some(instrument.id()),
            ))
        })
        .collect()
}

#[must_use]
pub fn account_position_is_nonzero(position: &AccountPosition) -> bool {
    position
        .position
        .parse::<f64>()
        .is_ok_and(|quantity| quantity != 0.0)
}

pub fn order_report_from_lighter(
    order: &Order,
    account_id: AccountId,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
    resolver: impl Fn(i64) -> Option<ClientOrderId>,
) -> OrderStatusReport {
    let order_status = order_status_from_lighter(&order.status_typed());
    let order_type = order_type_from_lighter(&order.order_type);
    let post_only = order.time_in_force.eq_ignore_ascii_case("post_only");
    let client_order_id = if order.client_order_index != 0 {
        resolver(order.client_order_index)
            .or_else(|| Some(ClientOrderId::from(order.client_order_index.to_string())))
    } else if let Ok(client_order_index) = order.client_order_id.parse::<i64>() {
        resolver(client_order_index)
            .or_else(|| Some(ClientOrderId::from(order.client_order_id.as_str())))
    } else if !order.client_order_id.is_empty() {
        Some(ClientOrderId::from(order.client_order_id.as_str()))
    } else {
        None
    };
    let ts_accepted = epoch_to_unix_nanos(Some(if order.created_at != 0 {
        order.created_at
    } else {
        order.timestamp
    }));
    let ts_last = epoch_to_unix_nanos(Some(if order.updated_at != 0 {
        order.updated_at
    } else if order.transaction_time != 0 {
        order.transaction_time
    } else {
        order.timestamp
    }));

    let mut report = OrderStatusReport::new(
        account_id,
        instrument.id(),
        client_order_id,
        VenueOrderId::from(if order.order_index != 0 {
            order.order_index.to_string()
        } else {
            order.order_id.clone()
        }),
        if order.is_ask {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        },
        order_type,
        time_in_force_from_lighter(&order.time_in_force, post_only),
        order_status,
        qty_from_string(&order.initial_base_amount, instrument.size_precision()),
        qty_from_string(&order.filled_base_amount, instrument.size_precision()),
        ts_accepted,
        ts_last,
        ts_init,
        Some(UUID4::new()),
    )
    .with_post_only(post_only)
    .with_reduce_only(order.reduce_only);

    if let Some(price) = non_zero_price_from_string(&order.price, instrument.price_precision()) {
        report = report.with_price(price);
    }
    if let Some(trigger_price) =
        non_zero_price_from_string(&order.trigger_price, instrument.price_precision())
    {
        report = report
            .with_trigger_price(trigger_price)
            .with_trigger_type(TriggerType::Default);
    }
    if let Some(expire_time) = non_zero_i64_to_unix_nanos(order.order_expiry)
        .filter(|_| report.time_in_force == TimeInForce::Gtd)
    {
        report = report.with_expire_time(expire_time);
    }

    let filled_base = Decimal::from_str_exact(&order.filled_base_amount).unwrap_or(Decimal::ZERO);
    let filled_quote = Decimal::from_str_exact(&order.filled_quote_amount).unwrap_or(Decimal::ZERO);
    if filled_base > Decimal::ZERO && filled_quote > Decimal::ZERO {
        let avg_px = (filled_quote / filled_base).to_f64().unwrap_or_default();
        if avg_px > 0.0
            && let Ok(with_avg) = report.clone().with_avg_px(avg_px)
        {
            report = with_avg;
        }
    }

    if let Some(status_text) = canceled_status_text(&order.status) {
        report = report.with_cancel_reason(status_text);
    }

    report
}

pub fn fill_report_from_lighter_trade(
    trade: &Trade,
    account_index: i64,
    account_id: AccountId,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
    resolver: impl Fn(i64) -> Option<ClientOrderId>,
) -> Option<FillReport> {
    let ask_client_order_id = trade.ask_client_id.and_then(&resolver);
    let bid_client_order_id = trade.bid_client_id.and_then(&resolver);
    let is_ask = if trade.ask_account_id == account_index {
        true
    } else if trade.bid_account_id == account_index {
        false
    } else if ask_client_order_id.is_some() {
        true
    } else if bid_client_order_id.is_some() {
        false
    } else {
        return None;
    };

    let is_maker = if is_ask {
        trade.is_maker_ask
    } else {
        !trade.is_maker_ask
    };
    let fee = if is_maker {
        trade.maker_fee.unwrap_or_default()
    } else {
        trade.taker_fee.unwrap_or_default()
    } as f64
        / LIGHTER_FEE_SCALE as f64;

    let client_order_id = if is_ask {
        ask_client_order_id
    } else {
        bid_client_order_id
    };
    let venue_order_id = if is_ask {
        VenueOrderId::from(trade.ask_id.to_string())
    } else {
        VenueOrderId::from(trade.bid_id.to_string())
    };
    let venue_position_id = None;
    let ts_event = epoch_to_unix_nanos(Some(trade.timestamp));

    Some(FillReport::new(
        account_id,
        instrument.id(),
        venue_order_id,
        TradeId::from(trade.trade_id.to_string()),
        if is_ask {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        },
        qty_from_string(&trade.size, instrument.size_precision()),
        price_from_string(&trade.price, instrument.price_precision()),
        Money::new(fee, instrument.quote_currency()),
        if is_maker {
            LiquiditySide::Maker
        } else {
            LiquiditySide::Taker
        },
        client_order_id,
        venue_position_id,
        ts_event,
        ts_init,
        Some(UUID4::new()),
    ))
}

#[must_use]
pub fn position_report_from_lighter(
    position: &AccountPosition,
    account_id: AccountId,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> PositionStatusReport {
    let quantity = position.position.parse::<f64>().unwrap_or_default().abs();
    let side = if quantity == 0.0 {
        PositionSideSpecified::Flat
    } else if position.sign < 0 {
        PositionSideSpecified::Short
    } else {
        PositionSideSpecified::Long
    };

    PositionStatusReport::new(
        account_id,
        instrument.id(),
        side,
        qty_from_f64(quantity, instrument.size_precision()),
        ts_init,
        ts_init,
        Some(UUID4::new()),
        None,
        non_zero_decimal_from_string(&position.avg_entry_price),
    )
}

#[must_use]
pub fn position_reports_from_detailed_account(
    account: &DetailedAccount,
    account_id: AccountId,
    registry: &LighterInstrumentRegistry,
    ts_init: UnixNanos,
) -> Vec<PositionStatusReport> {
    account
        .positions
        .clone()
        .unwrap_or_default()
        .into_iter()
        .filter(account_position_is_nonzero)
        .filter_map(|position| {
            let instrument = registry.instrument_for_market_id(position.market_id)?;
            Some(position_report_from_lighter(
                &position,
                account_id,
                &instrument,
                ts_init,
            ))
        })
        .collect()
}

#[must_use]
pub fn lighter_client_order_index(client_order_id: &ClientOrderId) -> i64 {
    let mut digest = [0_u8; 8];
    let mut hasher = Blake2bVar::new(8).expect("BLAKE2b supports an 8-byte variable output digest");
    hasher.update(client_order_id.as_str().as_bytes());
    hasher
        .finalize_variable(&mut digest)
        .expect("BLAKE2b output buffer has requested length");

    let mut candidate = u64::from_be_bytes(digest) & LIGHTER_MAX_CLIENT_ORDER_INDEX;
    if candidate == 0 {
        candidate = 1;
    }
    candidate as i64
}

#[must_use]
pub fn decimal_increment_price(precision: u8) -> Price {
    Price::from(decimal_increment_string(precision))
}

#[must_use]
pub fn decimal_increment_qty(precision: u8) -> Quantity {
    Quantity::from(decimal_increment_string(precision))
}

#[must_use]
pub fn price_from_string(value: &str, precision: u8) -> Price {
    value
        .parse::<Decimal>()
        .ok()
        .and_then(|value| Price::from_decimal_dp(value, precision).ok())
        .unwrap_or_else(|| Price::from_raw(0, precision))
}

#[must_use]
pub fn price_from_f64(value: f64, precision: u8) -> Price {
    Price::from(format_fixed(value, precision))
}

#[must_use]
pub fn qty_from_string(value: &str, precision: u8) -> Quantity {
    value
        .parse::<Decimal>()
        .ok()
        .and_then(|value| Quantity::from_decimal_dp(value, precision).ok())
        .unwrap_or_else(|| Quantity::from_raw(0, precision))
}

#[must_use]
pub fn qty_from_f64(value: f64, precision: u8) -> Quantity {
    Quantity::from(format_fixed(value, precision))
}

#[must_use]
pub fn decimal_from_f64(value: f64) -> Decimal {
    Decimal::from_str_exact(&value.to_string()).unwrap_or(Decimal::ZERO)
}

#[must_use]
pub fn to_lighter_price(value: Decimal, precision: u8) -> i64 {
    let factor = Decimal::from(10_u64.pow(u32::from(precision)));
    (value * factor)
        .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
        .to_i64()
        .unwrap_or_default()
}

#[must_use]
pub fn to_lighter_size(value: Decimal, precision: u8) -> i64 {
    let factor = Decimal::from(10_u64.pow(u32::from(precision)));
    (value * factor)
        .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
        .to_i64()
        .unwrap_or_default()
}

pub fn bar_granularity(bar_type: BarType) -> anyhow::Result<String> {
    let spec = bar_type.spec();
    if spec.price_type != PriceType::Last {
        anyhow::bail!("Lighter only exposes LAST bars");
    }
    match spec.aggregation {
        BarAggregation::Minute => Ok(format!("{}m", spec.step)),
        BarAggregation::Hour => Ok(format!("{}h", spec.step)),
        BarAggregation::Day => Ok(format!("{}d", spec.step)),
        BarAggregation::Week => Ok(format!("{}w", spec.step)),
        _ => anyhow::bail!("Unsupported Lighter bar aggregation"),
    }
}

fn push_book_side_snapshot_deltas(
    deltas: &mut Vec<OrderBookDelta>,
    instrument: &InstrumentAny,
    levels: &[PriceLevel],
    side: OrderSide,
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) {
    for level in levels {
        deltas.push(OrderBookDelta::new(
            instrument.id(),
            BookAction::Add,
            book_order_from_level(instrument, level, side),
            RecordFlag::F_SNAPSHOT as u8,
            sequence,
            ts_event,
            ts_init,
        ));
    }
}

fn push_book_side_delta_updates(
    deltas: &mut Vec<OrderBookDelta>,
    instrument: &InstrumentAny,
    levels: &[PriceLevel],
    side: OrderSide,
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) {
    for level in levels {
        let size = level.size.parse::<Decimal>().unwrap_or(Decimal::ZERO);
        let action = if size <= Decimal::ZERO {
            BookAction::Delete
        } else {
            BookAction::Update
        };
        deltas.push(OrderBookDelta::new(
            instrument.id(),
            action,
            book_order_from_level(instrument, level, side),
            RecordFlag::F_MBP as u8,
            sequence,
            ts_event,
            ts_init,
        ));
    }
}

fn book_order_from_level(
    instrument: &InstrumentAny,
    level: &PriceLevel,
    side: OrderSide,
) -> nautilus_model::data::BookOrder {
    nautilus_model::data::BookOrder::new(
        side,
        price_from_string(&level.price, instrument.price_precision()),
        qty_from_string(&level.size, instrument.size_precision()),
        0,
    )
}

fn decimal_increment_string(precision: u8) -> String {
    if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat(precision as usize - 1))
    }
}

fn format_fixed(value: f64, precision: u8) -> String {
    format!("{value:.precision$}", precision = precision as usize)
}

fn order_type_from_lighter(value: &str) -> OrderType {
    match value.to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        "stop-loss" | "stop_loss" => OrderType::StopMarket,
        "stop-loss-limit" | "stop_loss_limit" => OrderType::StopLimit,
        "take-profit" | "take_profit" => OrderType::MarketIfTouched,
        "take-profit-limit" | "take_profit_limit" => OrderType::LimitIfTouched,
        _ => OrderType::Limit,
    }
}

fn time_in_force_from_lighter(value: &str, post_only: bool) -> TimeInForce {
    if post_only {
        return TimeInForce::Gtc;
    }
    match value.to_ascii_lowercase().as_str() {
        "ioc" | "immediate_or_cancel" => TimeInForce::Ioc,
        "gtt" | "gtd" | "good_till_time" => TimeInForce::Gtd,
        _ => TimeInForce::Gtc,
    }
}

fn order_status_from_lighter(value: &LighterOrderStatus) -> OrderStatus {
    match value {
        LighterOrderStatus::InProgress | LighterOrderStatus::Pending => OrderStatus::Submitted,
        LighterOrderStatus::New | LighterOrderStatus::Open => OrderStatus::Accepted,
        LighterOrderStatus::PartiallyFilled => OrderStatus::PartiallyFilled,
        LighterOrderStatus::Filled => OrderStatus::Filled,
        LighterOrderStatus::Rejected => OrderStatus::Rejected,
        LighterOrderStatus::Canceled
        | LighterOrderStatus::CanceledPostOnly
        | LighterOrderStatus::CanceledReduceOnly
        | LighterOrderStatus::CanceledPositionNotAllowed
        | LighterOrderStatus::CanceledMarginNotAllowed
        | LighterOrderStatus::CanceledTooMuchSlippage
        | LighterOrderStatus::CanceledNotEnoughLiquidity
        | LighterOrderStatus::CanceledSelfTrade
        | LighterOrderStatus::CanceledExpired
        | LighterOrderStatus::CanceledOco
        | LighterOrderStatus::CanceledChild
        | LighterOrderStatus::CanceledLiquidation
        | LighterOrderStatus::CanceledInvalidBalance => OrderStatus::Canceled,
        LighterOrderStatus::Expired => OrderStatus::Expired,
        LighterOrderStatus::Unknown(_) => OrderStatus::Submitted,
    }
}

fn canceled_status_text(value: &str) -> Option<String> {
    let raw = value.to_ascii_lowercase();
    if raw.starts_with("canceled") || raw.starts_with("cancelled") {
        Some(value.to_string())
    } else {
        None
    }
}

fn non_zero_price_from_string(value: &str, precision: u8) -> Option<Price> {
    let parsed = value.parse::<f64>().ok()?;
    (parsed != 0.0).then(|| price_from_f64(parsed, precision))
}

fn non_zero_decimal_from_string(value: &str) -> Option<Decimal> {
    let parsed = Decimal::from_str_exact(value).ok()?;
    (parsed != Decimal::ZERO).then_some(parsed)
}

fn non_zero_i64_to_unix_nanos(value: i64) -> Option<UnixNanos> {
    (value != 0).then(|| epoch_to_unix_nanos(Some(value)))
}

#[cfg(test)]
mod tests {
    use ahash::AHashMap;
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        identifiers::AccountId,
        instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
        types::{Currency, Price, Quantity},
    };
    use rust_decimal::Decimal;

    use super::{
        LIGHTER_SETTLEMENT_CURRENCY, LighterMarketMarginMode, LighterMarketStatUpdate,
        LighterMarketType, account_balances_from_assets, channel_market_id,
        fill_report_from_lighter_trade, funding_rate_update_from_history,
        funding_rate_updates_from_history, instrument_meta_from_perp_detail,
        lighter_client_order_index, market_stats_to_updates, order_report_from_lighter,
        quote_tick_from_ticker, resolve_symbol_metadata,
    };
    use crate::models::{
        asset::Asset, funding::FundingRate, market::PerpsMarketStats, order::Order,
        order_book::PerpsOrderBookDetail, trade::Trade, ws::WsTickerData, ws::WsTickerLevel,
    };
    use nautilus_model::identifiers::ClientOrderId;

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str_exact(value).unwrap()
    }

    fn test_market_stats(
        market_id: i64,
        symbol: &str,
        current_funding_rate: f64,
    ) -> PerpsMarketStats {
        PerpsMarketStats {
            market_id: Some(market_id),
            symbol: Some(symbol.to_string()),
            last_trade_price: None,
            mark_price: None,
            index_price: None,
            open_interest: None,
            next_funding_time: None,
            funding_timestamp: None,
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
    fn resolve_symbol_metadata_supports_single_token_perp_symbols() {
        let assets_by_id = AHashMap::default();

        let (raw_symbol, base_code, quote_code) = resolve_symbol_metadata(
            LighterMarketType::Perp,
            Some("ASTER"),
            Some(0),
            Some(0),
            &assets_by_id,
        )
        .unwrap();

        assert_eq!(raw_symbol, "ASTER");
        assert_eq!(base_code, "ASTER");
        assert_eq!(quote_code, LIGHTER_SETTLEMENT_CURRENCY);
    }

    #[test]
    fn market_stats_funding_rate_converts_lighter_percent_to_nautilus_rate() {
        let instrument = InstrumentAny::from(crypto_perpetual_ethusdt());
        let updates = market_stats_to_updates(
            &instrument,
            &test_market_stats(110, "NVDA", 0.0021),
            0.into(),
            0.into(),
        );

        let funding = updates
            .into_iter()
            .find_map(|update| match update {
                LighterMarketStatUpdate::Funding(funding) => Some(funding),
                _ => None,
            })
            .expect("funding update");
        assert_eq!(funding.rate, decimal("0.000021"));
        assert_eq!(funding.interval, Some(60));
        assert_eq!(funding.next_funding_ns, None);
    }

    #[test]
    fn market_stats_funding_rate_preserves_negative_sign() {
        let instrument = InstrumentAny::from(crypto_perpetual_ethusdt());
        let updates = market_stats_to_updates(
            &instrument,
            &test_market_stats(124, "FOGO", -0.0405),
            0.into(),
            0.into(),
        );

        let funding = updates
            .into_iter()
            .find_map(|update| match update {
                LighterMarketStatUpdate::Funding(funding) => Some(funding),
                _ => None,
            })
            .expect("funding update");
        assert_eq!(funding.rate, decimal("-0.000405"));
        assert_eq!(funding.interval, Some(60));
        assert_eq!(funding.next_funding_ns, None);
    }

    #[test]
    fn market_stats_funding_timestamp_derives_next_funding_time() {
        let instrument = InstrumentAny::from(crypto_perpetual_ethusdt());
        let mut market = test_market_stats(110, "NVDA", 0.0021);
        market.funding_timestamp = Some(1_700_000_000_000);

        let updates = market_stats_to_updates(&instrument, &market, 0.into(), 0.into());

        let funding = updates
            .into_iter()
            .find_map(|update| match update {
                LighterMarketStatUpdate::Funding(funding) => Some(funding),
                _ => None,
            })
            .expect("funding update");
        assert_eq!(
            funding.next_funding_ns,
            Some(UnixNanos::from(1_700_003_600_000_000_000))
        );
    }

    #[test]
    fn historical_funding_rate_converts_lighter_percent_to_nautilus_rate() {
        let instrument = InstrumentAny::from(crypto_perpetual_ethusdt());
        let funding = funding_rate_update_from_history(
            &instrument,
            &FundingRate {
                market_id: Some(110),
                exchange: None,
                symbol: None,
                mark_price: None,
                index_price: None,
                funding_rate: Some(0.0021),
                settlement_time: Some(1_700_000_000_000),
            },
            0.into(),
        )
        .expect("funding update");

        assert_eq!(funding.rate, decimal("0.000021"));
        assert_eq!(funding.interval, Some(60));
        assert_eq!(funding.next_funding_ns, None);
    }

    #[test]
    fn historical_funding_rates_apply_market_window_limit_and_endpoint_shape() {
        let instrument = InstrumentAny::from(crypto_perpetual_ethusdt());
        let funding_rates = vec![
            FundingRate {
                market_id: Some(110),
                exchange: Some("lighter".to_string()),
                symbol: Some("NVDA".to_string()),
                mark_price: None,
                index_price: None,
                funding_rate: Some(0.0030),
                settlement_time: Some(1_700_000_003_000),
            },
            FundingRate {
                market_id: Some(110),
                exchange: Some("binance".to_string()),
                symbol: Some("NVDA".to_string()),
                mark_price: None,
                index_price: None,
                funding_rate: Some(0.0020),
                settlement_time: Some(1_700_000_002_000),
            },
            FundingRate {
                market_id: Some(110),
                exchange: Some("lighter".to_string()),
                symbol: Some("NVDA".to_string()),
                mark_price: None,
                index_price: None,
                funding_rate: Some(0.0010),
                settlement_time: Some(1_700_000_001_000),
            },
            FundingRate {
                market_id: Some(111),
                exchange: Some("lighter".to_string()),
                symbol: Some("FOGO".to_string()),
                mark_price: None,
                index_price: None,
                funding_rate: Some(0.0040),
                settlement_time: Some(1_700_000_004_000),
            },
            FundingRate {
                market_id: Some(110),
                exchange: Some("lighter".to_string()),
                symbol: Some("NVDA".to_string()),
                mark_price: None,
                index_price: None,
                funding_rate: Some(0.0050),
                settlement_time: None,
            },
        ];

        let updates = funding_rate_updates_from_history(
            &instrument,
            &funding_rates,
            110,
            Some(UnixNanos::from(1_700_000_001_000_000_000)),
            Some(UnixNanos::from(1_700_000_003_000_000_000)),
            Some(1),
            0.into(),
        );

        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].ts_event,
            UnixNanos::from(1_700_000_001_000_000_000)
        );
        assert_eq!(updates[0].rate, decimal("0.00001"));
    }

    #[test]
    fn quote_tick_uses_exchange_strings_directly() {
        let instrument = InstrumentAny::from(crypto_perpetual_ethusdt());
        let ticker = WsTickerData {
            s: "ETH-USDT".to_string(),
            a: WsTickerLevel {
                price: Some("223.978".to_string()),
                size: Some("94.307".to_string()),
            },
            b: WsTickerLevel {
                price: Some("223.933".to_string()),
                size: Some("2.389".to_string()),
            },
            last_updated_at: None,
        };

        let quote =
            quote_tick_from_ticker(&instrument, &ticker, 1.into(), 1.into()).expect("quote tick");

        assert_eq!(quote.bid_price, Price::from("223.93"));
        assert_eq!(quote.ask_price, Price::from("223.98"));
        assert_eq!(quote.bid_size, Quantity::from("2.38900000"));
        assert_eq!(quote.ask_size, Quantity::from("94.30700000"));
    }

    #[test]
    fn quote_tick_defaults_missing_ticker_sizes_to_zero() {
        let instrument = InstrumentAny::from(crypto_perpetual_ethusdt());
        let ticker = WsTickerData {
            s: "ETH-USDT".to_string(),
            a: WsTickerLevel {
                price: Some("223.978".to_string()),
                size: None,
            },
            b: WsTickerLevel {
                price: Some("223.933".to_string()),
                size: None,
            },
            last_updated_at: None,
        };

        let quote =
            quote_tick_from_ticker(&instrument, &ticker, 1.into(), 1.into()).expect("quote tick");

        assert_eq!(quote.bid_price, Price::from("223.93"));
        assert_eq!(quote.ask_price, Price::from("223.98"));
        assert_eq!(quote.bid_size, Quantity::zero(instrument.size_precision()));
        assert_eq!(quote.ask_size, Quantity::zero(instrument.size_precision()));
    }

    #[test]
    fn channel_market_id_supports_slash_and_colon_channels() {
        assert_eq!(channel_market_id("order_book/2048"), Some(2048));
        assert_eq!(channel_market_id("order_book:2048"), Some(2048));
        assert_eq!(channel_market_id("market_stats:all"), None);
    }

    #[test]
    fn client_order_index_matches_python_blake2b_mapping() {
        assert_eq!(
            lighter_client_order_index(&ClientOrderId::from("O-201")),
            96_188_314_079_481
        );
        assert_eq!(
            lighter_client_order_index(&ClientOrderId::from("O-MAKER-202")),
            274_836_310_172_150
        );
    }

    #[test]
    fn order_report_resolves_numeric_client_order_id_string() {
        let instrument = InstrumentAny::from(crypto_perpetual_ethusdt());
        let client_order_id = ClientOrderId::from("O-20260529-123418-001-001-375");
        let client_order_index = lighter_client_order_index(&client_order_id);
        let order = Order {
            order_index: 12_345,
            client_order_index: 0,
            order_id: "12345".to_string(),
            client_order_id: client_order_index.to_string(),
            market_index: 0,
            owner_account_index: 713_543,
            initial_base_amount: "0.01".to_string(),
            price: "1000".to_string(),
            nonce: 0,
            remaining_base_amount: "0.01".to_string(),
            is_ask: false,
            base_size: 0,
            base_price: 0,
            filled_base_amount: "0".to_string(),
            filled_quote_amount: "0".to_string(),
            side: "bid".to_string(),
            order_type: "limit".to_string(),
            time_in_force: "good-till-time".to_string(),
            reduce_only: false,
            trigger_price: String::new(),
            order_expiry: 0,
            status: "open".to_string(),
            trigger_status: String::new(),
            trigger_time: 0,
            parent_order_index: 0,
            parent_order_id: String::new(),
            to_trigger_order_id_0: String::new(),
            to_trigger_order_id_1: String::new(),
            to_cancel_order_id_0: String::new(),
            block_height: 0,
            timestamp: 1,
            created_at: 1,
            updated_at: 1,
            transaction_time: 1,
        };

        let report = order_report_from_lighter(
            &order,
            AccountId::from("LIGHTER-713543"),
            &instrument,
            1.into(),
            |value| (value == client_order_index).then_some(client_order_id),
        );

        assert_eq!(report.client_order_id, Some(client_order_id));
    }

    #[test]
    fn fill_report_infers_side_from_known_client_id_when_account_ids_are_missing() {
        let instrument = InstrumentAny::from(crypto_perpetual_ethusdt());
        let client_order_id = ClientOrderId::from("O-20260529-123418-001-001-375");
        let client_order_index = lighter_client_order_index(&client_order_id);
        let trade = Trade {
            trade_id: 99,
            tx_hash: String::new(),
            trade_type: "trade".to_string(),
            market_id: 0,
            size: "0.01".to_string(),
            price: "1000".to_string(),
            usd_amount: "10".to_string(),
            ask_id: 44,
            bid_id: 45,
            ask_client_id: Some(client_order_index),
            bid_client_id: Some(123),
            ask_account_id: 0,
            bid_account_id: 0,
            is_maker_ask: false,
            block_height: 0,
            timestamp: 1,
            taker_fee: Some(0),
            taker_position_size_before: None,
            taker_entry_quote_before: None,
            taker_initial_margin_fraction_before: None,
            taker_position_sign_changed: None,
            maker_fee: Some(0),
            maker_position_size_before: None,
            maker_entry_quote_before: None,
            maker_initial_margin_fraction_before: None,
            maker_position_sign_changed: None,
            transaction_time: 1,
            ask_account_pnl: None,
            bid_account_pnl: None,
        };

        let report = fill_report_from_lighter_trade(
            &trade,
            713_543,
            AccountId::from("LIGHTER-713543"),
            &instrument,
            1.into(),
            |value| (value == client_order_index).then_some(client_order_id),
        )
        .expect("fill report");

        assert_eq!(report.client_order_id, Some(client_order_id));
        assert_eq!(report.venue_order_id.to_string(), "44");
    }

    #[test]
    fn instrument_meta_registers_unknown_lighter_crypto_assets() {
        let assets_by_id = AHashMap::from_iter([(1, "JUP".to_string()), (2, "USDC".to_string())]);
        let detail = PerpsOrderBookDetail {
            market_id: Some(42),
            symbol: Some("JUP-USDC".to_string()),
            base_asset_id: Some(1),
            quote_asset_id: Some(2),
            price_decimals: Some(4),
            size_decimals: Some(2),
            ..serde_json::from_value(serde_json::json!({})).unwrap()
        };

        let meta = instrument_meta_from_perp_detail(&detail, &assets_by_id)
            .unwrap()
            .unwrap();

        assert_eq!(
            meta.instrument.base_currency().unwrap().code.as_str(),
            "JUP"
        );
        assert!(Currency::try_from_str("JUP").is_some());
    }

    #[test]
    fn instrument_meta_exposes_lighter_market_margin_mode() {
        let detail = PerpsOrderBookDetail {
            market_id: Some(173),
            symbol: Some("SPACEX".to_string()),
            price_decimals: Some(4),
            size_decimals: Some(2),
            market_config: Some(serde_json::Map::from_iter([(
                "market_margin_mode".to_string(),
                serde_json::Value::from(1),
            )])),
            ..serde_json::from_value(serde_json::json!({})).unwrap()
        };

        let meta = instrument_meta_from_perp_detail(&detail, &AHashMap::default())
            .unwrap()
            .unwrap();

        assert_eq!(
            meta.market_margin_mode(),
            Some(LighterMarketMarginMode::Isolated)
        );
        assert!(!meta.supports_cross_margin());
    }

    #[test]
    fn account_balances_register_unknown_lighter_crypto_assets() {
        let balances = account_balances_from_assets(&[Asset {
            symbol: "JUP".to_string(),
            asset_id: 1,
            balance: Some("1.5".to_string()),
            locked_balance: Some("0.25".to_string()),
            margin_balance: None,
            extra: serde_json::Map::new(),
        }]);

        assert_eq!(balances[0].currency.code.as_str(), "JUP");
        assert!(Currency::try_from_str("JUP").is_some());
    }

    #[test]
    fn account_balances_use_usdc_margin_balance_when_present() {
        let balances = account_balances_from_assets(&[Asset {
            symbol: LIGHTER_SETTLEMENT_CURRENCY.to_string(),
            asset_id: 3,
            balance: Some("49.99".to_string()),
            locked_balance: Some("1.00".to_string()),
            margin_balance: Some("117.43".to_string()),
            extra: serde_json::Map::new(),
        }]);

        assert_eq!(
            balances[0].currency.code.as_str(),
            LIGHTER_SETTLEMENT_CURRENCY
        );
        assert_eq!(balances[0].total.as_f64(), 117.43);
        assert_eq!(balances[0].locked.as_f64(), 0.0);
        assert_eq!(balances[0].free.as_f64(), 117.43);
    }
}
