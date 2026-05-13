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

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use ahash::AHashMap;
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
    identifiers::{
        AccountId, ClientOrderId, InstrumentId, PositionId, TradeId, Venue, VenueOrderId,
    },
    instruments::{CryptoPerpetual, CurrencyPair, Instrument, InstrumentAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use rust_decimal::{Decimal, RoundingStrategy, prelude::ToPrimitive};
use serde_json::Value;

use crate::{
    config::LighterDataClientConfig,
    http::client::LighterHttpClient,
    models::{
        account::{AccountPosition, DetailedAccount},
        asset::Asset,
        candle::Candle,
        funding::FundingRate,
        market::PerpsMarketStats,
        order::{Order, OrderStatus as LighterOrderStatus},
        order_book::{PerpsOrderBookDetail, PriceLevel, SpotOrderBookDetail},
        trade::Trade,
        ws::WsTickerData,
    },
};

pub const LIGHTER: &str = "LIGHTER";
pub const LIGHTER_PERP_SUFFIX: &str = "PERP";
pub const LIGHTER_SPOT_SUFFIX: &str = "SPOT";
pub const LIGHTER_SETTLEMENT_CURRENCY: &str = "USDC";
pub const LIGHTER_FEE_SCALE: i64 = 1_000_000;
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

#[derive(Clone, Debug)]
pub struct LighterInstrumentMeta {
    pub market_id: i64,
    pub instrument: InstrumentAny,
    pub market_type: LighterMarketType,
    pub price_precision: u8,
    pub size_precision: u8,
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

#[derive(Clone, Debug, Default)]
pub struct LighterBookState {
    pub bids: AHashMap<String, String>,
    pub asks: AHashMap<String, String>,
}

impl LighterBookState {
    pub fn set_snapshot(&mut self, bids: &[PriceLevel], asks: &[PriceLevel]) {
        self.bids = bids
            .iter()
            .map(|level| (level.price.clone(), level.size.clone()))
            .collect();
        self.asks = asks
            .iter()
            .map(|level| (level.price.clone(), level.size.clone()))
            .collect();
    }

    pub fn apply_delta(&mut self, bids: &[PriceLevel], asks: &[PriceLevel]) {
        apply_book_side(&mut self.bids, bids);
        apply_book_side(&mut self.asks, asks);
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
    let base_currency = Currency::from(base_code.as_str());
    let quote_currency = Currency::from(quote_code.as_str());

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
    let base_currency = Currency::from(base_code.as_str());
    let quote_currency = Currency::from(quote_code.as_str());

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
pub fn epoch_to_unix_nanos(value: Option<i64>) -> UnixNanos {
    let Some(value) = value else {
        return UnixNanos::default();
    };

    let digits = value.unsigned_abs().to_string().len();
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
pub fn quote_tick_from_ticker(
    instrument: &InstrumentAny,
    ticker: &WsTickerData,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> Option<QuoteTick> {
    let (Some(bid_price), Some(ask_price), Some(bid_size), Some(ask_size)) =
        (ticker.b.price, ticker.a.price, ticker.b.size, ticker.a.size)
    else {
        return None;
    };

    Some(QuoteTick::new(
        instrument.id(),
        price_from_f64(bid_price, instrument.price_precision()),
        price_from_f64(ask_price, instrument.price_precision()),
        qty_from_f64(bid_size, instrument.size_precision()),
        qty_from_f64(ask_size, instrument.size_precision()),
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

    let funding_rate = market.current_funding_rate.or(market.funding_rate);
    if let Some(funding_rate) = funding_rate {
        updates.push(LighterMarketStatUpdate::Funding(FundingRateUpdate::new(
            instrument.id(),
            decimal_from_f64(funding_rate),
            None,
            market
                .next_funding_time
                .map(|value| epoch_to_unix_nanos(Some(value))),
            ts_event,
            ts_init,
        )));
    }

    updates
}

#[must_use]
pub fn funding_rate_update_from_history(
    instrument: &InstrumentAny,
    funding_rate: &FundingRate,
    ts_init: UnixNanos,
) -> Option<FundingRateUpdate> {
    let rate = funding_rate.funding_rate?;
    let ts_event = funding_rate
        .settlement_time
        .map_or(ts_init, |value| epoch_to_unix_nanos(Some(value)));

    Some(FundingRateUpdate::new(
        instrument.id(),
        decimal_from_f64(rate),
        None,
        funding_rate
            .settlement_time
            .map(|value| epoch_to_unix_nanos(Some(value))),
        ts_event,
        ts_init,
    ))
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
    let mut deltas = vec![OrderBookDelta::clear(
        instrument.id(),
        sequence,
        ts_event,
        ts_init,
    )];
    deltas.extend(book_side_snapshot_deltas(
        instrument,
        bids,
        OrderSide::Buy,
        sequence,
        ts_event,
        ts_init,
    ));
    deltas.extend(book_side_snapshot_deltas(
        instrument,
        asks,
        OrderSide::Sell,
        sequence,
        ts_event,
        ts_init,
    ));

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
    let mut deltas = book_side_delta_updates(
        instrument,
        bids,
        OrderSide::Buy,
        sequence,
        ts_event,
        ts_init,
    );
    deltas.extend(book_side_delta_updates(
        instrument,
        asks,
        OrderSide::Sell,
        sequence,
        ts_event,
        ts_init,
    ));

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
        let order = book_order_from_level(
            instrument,
            level,
            OrderSide::Buy,
            level.price.parse::<u64>().unwrap_or_default(),
        );
        book.add(
            order,
            RecordFlag::F_MBP as u8 | RecordFlag::F_LAST as u8,
            sequence,
            ts_event,
        );
    }

    for level in asks {
        let order = book_order_from_level(
            instrument,
            level,
            OrderSide::Sell,
            level.price.parse::<u64>().unwrap_or_default(),
        );
        book.add(
            order,
            RecordFlag::F_MBP as u8 | RecordFlag::F_LAST as u8,
            sequence,
            ts_event,
        );
    }
}

#[must_use]
pub fn account_balances_from_assets(assets: &[Asset]) -> Vec<AccountBalance> {
    assets
        .iter()
        .map(|asset| {
            let currency = Currency::from(asset.symbol.as_str());
            let total = asset.balance_f64().unwrap_or_default();
            let locked = asset.locked_balance_f64().unwrap_or_default();
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

pub fn order_report_from_lighter(
    order: &Order,
    account_id: AccountId,
    instrument: &InstrumentAny,
    resolver: impl Fn(i64) -> Option<ClientOrderId>,
) -> OrderStatusReport {
    let order_status = order_status_from_lighter(&order.status_typed());
    let order_type = order_type_from_lighter(&order.order_type);
    let post_only = order.time_in_force.eq_ignore_ascii_case("post_only");
    let client_order_id = if order.client_order_index != 0 {
        resolver(order.client_order_index)
            .or_else(|| Some(ClientOrderId::from(order.client_order_index.to_string())))
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
        ts_last,
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
    resolver: impl Fn(i64) -> Option<ClientOrderId>,
) -> Option<FillReport> {
    let is_ask = if trade.ask_account_id == account_index {
        true
    } else if trade.bid_account_id == account_index {
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
        trade.ask_client_id.and_then(&resolver)
    } else {
        trade.bid_client_id.and_then(&resolver)
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
        ts_event,
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
        Some(PositionId::from(format!(
            "{}:{}",
            account_id,
            instrument.id()
        ))),
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
    let mut hasher = DefaultHasher::new();
    client_order_id.hash(&mut hasher);
    let mut candidate = hasher.finish() & LIGHTER_MAX_CLIENT_ORDER_INDEX;
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
    let parsed = value.parse::<f64>().unwrap_or_default();
    price_from_f64(parsed, precision)
}

#[must_use]
pub fn price_from_f64(value: f64, precision: u8) -> Price {
    Price::from(format_fixed(value, precision))
}

#[must_use]
pub fn qty_from_string(value: &str, precision: u8) -> Quantity {
    let parsed = value.parse::<f64>().unwrap_or_default();
    qty_from_f64(parsed, precision)
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

pub fn data_http_client(config: &LighterDataClientConfig) -> anyhow::Result<LighterHttpClient> {
    let mut client = LighterHttpClient::new_public(
        crate::config::Config::for_environment(config.environment)
            .with_http_base_url(config.http_url())
            .with_ws_base_url(config.ws_url()),
    )?;
    let _ = &mut client;
    Ok(client)
}

fn apply_book_side(side: &mut AHashMap<String, String>, levels: &[PriceLevel]) {
    for level in levels {
        let size = level.size.parse::<f64>().unwrap_or_default();
        if size <= 0.0 {
            side.remove(&level.price);
        } else {
            side.insert(level.price.clone(), level.size.clone());
        }
    }
}

fn book_side_snapshot_deltas(
    instrument: &InstrumentAny,
    levels: &[PriceLevel],
    side: OrderSide,
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> Vec<OrderBookDelta> {
    levels
        .iter()
        .map(|level| {
            OrderBookDelta::new(
                instrument.id(),
                BookAction::Add,
                book_order_from_level(
                    instrument,
                    level,
                    side,
                    level.price.parse::<u64>().unwrap_or_default(),
                ),
                RecordFlag::F_MBP as u8 | RecordFlag::F_LAST as u8,
                sequence,
                ts_event,
                ts_init,
            )
        })
        .collect()
}

fn book_side_delta_updates(
    instrument: &InstrumentAny,
    levels: &[PriceLevel],
    side: OrderSide,
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> Vec<OrderBookDelta> {
    levels
        .iter()
        .map(|level| {
            let size = level.size.parse::<f64>().unwrap_or_default();
            let action = if size <= 0.0 {
                BookAction::Delete
            } else {
                BookAction::Update
            };
            OrderBookDelta::new(
                instrument.id(),
                action,
                book_order_from_level(
                    instrument,
                    level,
                    side,
                    level.price.parse::<u64>().unwrap_or_default(),
                ),
                RecordFlag::F_MBP as u8 | RecordFlag::F_LAST as u8,
                sequence,
                ts_event,
                ts_init,
            )
        })
        .collect()
}

fn book_order_from_level(
    instrument: &InstrumentAny,
    level: &PriceLevel,
    side: OrderSide,
    order_id: u64,
) -> nautilus_model::data::BookOrder {
    nautilus_model::data::BookOrder::new(
        side,
        price_from_string(&level.price, instrument.price_precision()),
        qty_from_string(&level.size, instrument.size_precision()),
        order_id,
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
        LighterOrderStatus::Unknown(_) => OrderStatus::Accepted,
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

    use super::{
        LIGHTER_SETTLEMENT_CURRENCY, LighterMarketType, channel_market_id, resolve_symbol_metadata,
    };

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
    fn channel_market_id_supports_slash_and_colon_channels() {
        assert_eq!(channel_market_id("order_book/2048"), Some(2048));
        assert_eq!(channel_market_id("order_book:2048"), Some(2048));
        assert_eq!(channel_market_id("market_stats:all"), None);
    }
}
