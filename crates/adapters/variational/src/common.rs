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

//! Shared Nautilus domain parsing and metadata helpers for the Variational adapter.

use std::str::FromStr;

use ahash::AHashMap;
use anyhow::Context;
use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    data::{FundingRateUpdate, IndexPriceUpdate, MarkPriceUpdate, QuoteTick},
    identifiers::{InstrumentId, Symbol, Venue},
    instruments::{CryptoPerpetual, Instrument, InstrumentAny},
    types::{Currency, Price, Quantity, fixed::FIXED_PRECISION},
};
use rust_decimal::Decimal;
use serde_json::{Map, Value};

use crate::{
    config::VariationalQuoteTier,
    http::client::VariationalHttpClient,
    models::{VariationalListing, VariationalQuote, VariationalStats},
    websocket::messages::VariationalWsPriceMessage,
};

pub const VARIATIONAL: &str = "VARIATIONAL";
pub const VARIATIONAL_PERP_SUFFIX: &str = "PERP";
pub const VARIATIONAL_QUOTE_CURRENCY: &str = "USDC";

pub fn venue() -> Venue {
    Venue::from(VARIATIONAL)
}

#[derive(Clone, Debug)]
pub struct VariationalInstrumentMeta {
    pub ticker: String,
    pub instrument: InstrumentAny,
    pub price_precision: u8,
    pub size_precision: u8,
}

#[derive(Clone, Debug, Default)]
pub struct VariationalInstrumentRegistry {
    by_ticker: AHashMap<String, VariationalInstrumentMeta>,
    by_instrument_id: AHashMap<InstrumentId, VariationalInstrumentMeta>,
}

impl VariationalInstrumentRegistry {
    pub fn insert(&mut self, meta: VariationalInstrumentMeta) {
        self.by_instrument_id
            .insert(meta.instrument.id(), meta.clone());
        self.by_ticker.insert(meta.ticker.clone(), meta);
    }

    #[must_use]
    pub fn meta_for_ticker(&self, ticker: &str) -> Option<&VariationalInstrumentMeta> {
        self.by_ticker.get(ticker)
    }

    #[must_use]
    pub fn meta_for_instrument_id(
        &self,
        instrument_id: &InstrumentId,
    ) -> Option<&VariationalInstrumentMeta> {
        self.by_instrument_id.get(instrument_id)
    }

    #[must_use]
    pub fn instruments(&self) -> Vec<InstrumentAny> {
        let mut instruments: Vec<_> = self
            .by_ticker
            .values()
            .map(|meta| meta.instrument.clone())
            .collect();
        instruments.sort_unstable_by_key(|instrument| instrument.id().to_string());
        instruments
    }

    pub fn clear(&mut self) {
        self.by_ticker.clear();
        self.by_instrument_id.clear();
    }
}

pub async fn load_instrument_registry(
    http_client: &VariationalHttpClient,
    default_size_precision: u8,
) -> anyhow::Result<VariationalInstrumentRegistry> {
    let stats = http_client
        .stats()
        .await
        .context("failed to load Variational stats")?;

    let size_precision = default_size_precision.min(FIXED_PRECISION);
    let mut registry = VariationalInstrumentRegistry::default();

    for listing in &stats.listings {
        let meta = instrument_meta_from_listing(listing, size_precision)
            .with_context(|| format!("failed to parse Variational listing {}", listing.ticker))?;
        registry.insert(meta);
    }

    Ok(registry)
}

pub fn instrument_meta_from_listing(
    listing: &VariationalListing,
    size_precision: u8,
) -> anyhow::Result<VariationalInstrumentMeta> {
    let ticker = listing.ticker.trim();
    anyhow::ensure!(!ticker.is_empty(), "listing ticker is empty");

    let price_precision = infer_price_precision(listing);
    let symbol = format!("{ticker}-{VARIATIONAL_QUOTE_CURRENCY}-{VARIATIONAL_PERP_SUFFIX}");
    let instrument_id = InstrumentId::new(symbol.into(), venue());
    let raw_symbol = Symbol::new(ticker);
    let base_currency =
        Currency::get_or_create_crypto_with_context(ticker, Some("variational perp base"));
    let quote_currency = Currency::get_or_create_crypto_with_context(
        VARIATIONAL_QUOTE_CURRENCY,
        Some("variational perp quote"),
    );
    let price_increment = decimal_increment_price(price_precision)?;
    let size_increment = decimal_increment_quantity(size_precision)?;
    let info = listing_to_params(listing, price_precision, size_precision);

    let instrument = CryptoPerpetual::new_checked(
        instrument_id,
        raw_symbol,
        base_currency,
        quote_currency,
        quote_currency,
        false,
        price_precision,
        size_precision,
        price_increment,
        size_increment,
        None,
        Some(size_increment),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(Decimal::ZERO),
        Some(Decimal::ZERO),
        info,
        UnixNanos::default(),
        UnixNanos::default(),
    )
    .with_context(|| format!("failed to build Variational instrument {ticker}"))?
    .into_any();

    Ok(VariationalInstrumentMeta {
        ticker: ticker.to_string(),
        instrument,
        price_precision,
        size_precision,
    })
}

fn listing_to_params(
    listing: &VariationalListing,
    price_precision: u8,
    size_precision: u8,
) -> Option<Params> {
    let Value::Object(mut map) = serde_json::to_value(listing).ok()? else {
        return None;
    };

    map.insert("market_type".to_string(), Value::from("perp"));
    map.insert(
        "raw_symbol".to_string(),
        Value::from(listing.ticker.clone()),
    );
    map.insert("price_precision".to_string(), Value::from(price_precision));
    map.insert("size_precision".to_string(), Value::from(size_precision));
    serde_json::from_value(Value::Object(map)).ok()
}

#[must_use]
pub fn listing_event_time(listing: &VariationalListing, fallback: UnixNanos) -> UnixNanos {
    listing
        .quotes
        .as_ref()
        .and_then(|quotes| quotes.updated_at.as_deref())
        .and_then(|timestamp| UnixNanos::from_str(timestamp).ok())
        .unwrap_or(fallback)
}

#[must_use]
pub fn quote_tick_from_listing(
    meta: &VariationalInstrumentMeta,
    listing: &VariationalListing,
    quote_tier: VariationalQuoteTier,
    ts_init: UnixNanos,
) -> Option<QuoteTick> {
    let quotes = listing.quotes.as_ref()?;
    let quote = quotes.preferred_quote(quote_tier.key())?;
    let ts_event = listing_event_time(listing, ts_init);
    let bid_price = price_from_optional_str(quote.bid.as_deref(), meta.price_precision)?;
    let ask_price = price_from_optional_str(quote.ask.as_deref(), meta.price_precision)?;
    let bid_size = quote_size(quote, true, quote_tier, meta.size_precision);
    let ask_size = quote_size(quote, false, quote_tier, meta.size_precision);

    Some(QuoteTick::new(
        meta.instrument.id(),
        bid_price,
        ask_price,
        bid_size,
        ask_size,
        ts_event,
        ts_init,
    ))
}

#[must_use]
pub fn mark_price_update_from_listing(
    meta: &VariationalInstrumentMeta,
    listing: &VariationalListing,
    ts_init: UnixNanos,
) -> Option<MarkPriceUpdate> {
    let value = price_from_optional_str(listing.mark_price.as_deref(), meta.price_precision)?;
    Some(MarkPriceUpdate::new(
        meta.instrument.id(),
        value,
        listing_event_time(listing, ts_init),
        ts_init,
    ))
}

#[must_use]
pub fn mark_price_update_from_ws(
    meta: &VariationalInstrumentMeta,
    message: &VariationalWsPriceMessage,
    ts_init: UnixNanos,
) -> Option<MarkPriceUpdate> {
    let value = price_from_optional_str(message.pricing.price.as_deref(), meta.price_precision)?;
    Some(MarkPriceUpdate::new(
        meta.instrument.id(),
        value,
        ws_price_event_time(message, ts_init),
        ts_init,
    ))
}

#[must_use]
pub fn index_price_update_from_ws(
    meta: &VariationalInstrumentMeta,
    message: &VariationalWsPriceMessage,
    ts_init: UnixNanos,
) -> Option<IndexPriceUpdate> {
    let value = price_from_optional_str(
        message.pricing.underlying_price.as_deref(),
        meta.price_precision,
    )?;
    Some(IndexPriceUpdate::new(
        meta.instrument.id(),
        value,
        ws_price_event_time(message, ts_init),
        ts_init,
    ))
}

#[must_use]
pub fn funding_rate_update_from_listing(
    meta: &VariationalInstrumentMeta,
    listing: &VariationalListing,
    ts_init: UnixNanos,
) -> Option<FundingRateUpdate> {
    let rate = listing
        .funding_rate
        .as_deref()
        .and_then(|value| Decimal::from_str(value).ok())?;
    let interval = listing
        .funding_interval_s
        .and_then(|seconds| u16::try_from(seconds / 60).ok());

    Some(FundingRateUpdate::new(
        meta.instrument.id(),
        rate,
        interval,
        None,
        listing_event_time(listing, ts_init),
        ts_init,
    ))
}

#[must_use]
pub fn listing_for_instrument<'a>(
    stats: &'a VariationalStats,
    meta: &VariationalInstrumentMeta,
) -> Option<&'a VariationalListing> {
    stats
        .listings
        .iter()
        .find(|listing| listing.ticker == meta.ticker)
}

#[must_use]
pub fn ws_price_event_time(message: &VariationalWsPriceMessage, fallback: UnixNanos) -> UnixNanos {
    message
        .pricing
        .timestamp
        .as_deref()
        .and_then(|timestamp| UnixNanos::from_str(timestamp).ok())
        .unwrap_or(fallback)
}

fn quote_size(
    quote: &VariationalQuote,
    is_bid: bool,
    quote_tier: VariationalQuoteTier,
    size_precision: u8,
) -> Quantity {
    let Some(notional) = quote_tier.notional_usdc() else {
        return Quantity::zero(size_precision);
    };
    let price = if is_bid {
        quote.bid.as_deref()
    } else {
        quote.ask.as_deref()
    };
    let Some(price) = price.and_then(|value| Decimal::from_str(value).ok()) else {
        return Quantity::zero(size_precision);
    };
    if price <= Decimal::ZERO {
        return Quantity::zero(size_precision);
    }

    let quantity = Decimal::from(notional) / price;
    Quantity::from_decimal_dp(quantity, size_precision)
        .unwrap_or_else(|_| Quantity::zero(size_precision))
}

fn price_from_optional_str(value: Option<&str>, precision: u8) -> Option<Price> {
    let decimal = Decimal::from_str(value?).ok()?;
    Price::from_decimal_dp(decimal, precision).ok()
}

fn decimal_increment_price(precision: u8) -> anyhow::Result<Price> {
    Price::from_decimal_dp(Decimal::new(1, u32::from(precision)), precision)
        .map_err(|error| anyhow::anyhow!("{error}"))
}

fn decimal_increment_quantity(precision: u8) -> anyhow::Result<Quantity> {
    Quantity::from_decimal_dp(Decimal::new(1, u32::from(precision)), precision)
        .map_err(|error| anyhow::anyhow!("{error}"))
}

fn infer_price_precision(listing: &VariationalListing) -> u8 {
    let mut precision = 0;

    if let Some(quotes) = &listing.quotes {
        for quote in [
            quotes.base.as_ref(),
            quotes.size_1k.as_ref(),
            quotes.size_100k.as_ref(),
            quotes.size_1m.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            precision = precision.max(decimal_places(quote.bid.as_deref()));
            precision = precision.max(decimal_places(quote.ask.as_deref()));
        }
    }

    if precision == 0 {
        precision = decimal_places(listing.mark_price.as_deref());
    }

    precision.clamp(0, FIXED_PRECISION)
}

fn decimal_places(value: Option<&str>) -> u8 {
    let Some(value) = value else {
        return 0;
    };
    if Decimal::from_str(value).is_err() {
        return 0;
    }

    let normalized = value
        .split(['e', 'E'])
        .next()
        .unwrap_or(value)
        .trim_start_matches(['+', '-']);
    normalized
        .split_once('.')
        .map_or(0, |(_, fractional)| fractional.len() as u8)
}

#[must_use]
pub fn stats_by_ticker(stats: &VariationalStats) -> AHashMap<&str, &VariationalListing> {
    stats
        .listings
        .iter()
        .map(|listing| (listing.ticker.as_str(), listing))
        .collect()
}

#[must_use]
pub fn params_from_map(map: Map<String, Value>) -> Option<Params> {
    serde_json::from_value(Value::Object(map)).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_listing() -> VariationalListing {
        serde_json::from_value(serde_json::json!({
            "ticker": "BTC",
            "name": "Bitcoin",
            "mark_price": "93787.9606019699",
            "volume_24h": "1058107020.462713",
            "open_interest": {
                "long_open_interest": "113883049.01452301878687056000",
                "short_open_interest": "82403040.51901045963533924000"
            },
            "funding_rate": "0.037347",
            "funding_interval_s": 28800,
            "base_spread_bps": "0.4307589134116585963643440000",
            "quotes": {
                "updated_at": "2026-01-06T06:38:52.476166127Z",
                "base": {"bid": "93750.97", "ask": "93755.01"},
                "size_1k": {"bid": "93750.97", "ask": "93755.01"},
                "size_100k": {"bid": "93746.13", "ask": "93759.85"},
                "size_1m": {"bid": "93718.95", "ask": "93779.82"}
            }
        }))
        .unwrap()
    }

    #[test]
    fn builds_crypto_perpetual_from_listing() {
        let listing = sample_listing();
        let meta = instrument_meta_from_listing(&listing, 8).unwrap();

        assert_eq!(
            meta.instrument.id().to_string(),
            "BTC-USDC-PERP.VARIATIONAL"
        );
        assert_eq!(meta.instrument.raw_symbol().to_string(), "BTC");
        assert_eq!(meta.price_precision, 2);
        assert_eq!(meta.size_precision, 8);
    }

    #[test]
    fn parses_current_market_updates() {
        let listing = sample_listing();
        let meta = instrument_meta_from_listing(&listing, 8).unwrap();
        let ts_init = UnixNanos::from(1);

        let quote = quote_tick_from_listing(&meta, &listing, VariationalQuoteTier::Size1k, ts_init)
            .unwrap();
        let mark = mark_price_update_from_listing(&meta, &listing, ts_init).unwrap();
        let funding = funding_rate_update_from_listing(&meta, &listing, ts_init).unwrap();

        assert_eq!(quote.bid_price.to_string(), "93750.97");
        assert_eq!(mark.value.to_string(), "93787.96");
        assert_eq!(funding.rate, Decimal::from_str("0.037347").unwrap());
        assert_eq!(funding.interval, Some(480));
        assert_eq!(quote.ts_event.as_u64(), 1_767_681_532_476_166_127);
    }
}
