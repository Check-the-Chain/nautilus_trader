use serde::{Deserialize, Serialize};

use super::de::{opt_f64_from_string_or_number, opt_i64_from_string_or_number};
use super::order::SimpleOrder;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub market_id: Option<i64>,
    #[serde(default)]
    pub market_type: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub base_asset_id: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub quote_asset_id: Option<i64>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub taker_fee: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub maker_fee: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub liquidation_fee: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub min_base_amount: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub min_quote_amount: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub order_quote_limit: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub supported_size_decimals: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub supported_price_decimals: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub supported_quote_decimals: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBooks {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub order_books: Vec<OrderBook>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookDepth {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub total_asks: Option<i64>,
    #[serde(default)]
    pub asks: Vec<SimpleOrder>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub total_bids: Option<i64>,
    #[serde(default)]
    pub bids: Vec<SimpleOrder>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: String,
    pub size: String,
}

impl PriceLevel {
    pub fn price_f64(&self) -> Option<f64> {
        self.price.parse::<f64>().ok()
    }

    pub fn size_f64(&self) -> Option<f64> {
        self.size.parse::<f64>().ok()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookDetails {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub order_book_details: Vec<PerpsOrderBookDetail>,
    #[serde(default)]
    pub spot_order_book_details: Vec<SpotOrderBookDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerpsOrderBookDetail {
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub market_id: Option<i64>,
    #[serde(default)]
    pub market_type: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub base_asset_id: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub quote_asset_id: Option<i64>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub taker_fee: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub maker_fee: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub liquidation_fee: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub min_base_amount: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub min_quote_amount: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub order_quote_limit: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub supported_size_decimals: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub supported_price_decimals: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub supported_quote_decimals: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub size_decimals: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub price_decimals: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub quote_multiplier: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub default_initial_margin_fraction: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub min_initial_margin_fraction: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub maintenance_margin_fraction: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub closeout_margin_fraction: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub last_trade_price: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub daily_trades_count: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub daily_base_token_volume: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub daily_quote_token_volume: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub daily_price_low: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub daily_price_high: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub daily_price_change: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub open_interest: Option<f64>,
    #[serde(default)]
    pub daily_chart: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default)]
    pub market_config: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotOrderBookDetail {
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub market_id: Option<i64>,
    #[serde(default)]
    pub market_type: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub base_asset_id: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub quote_asset_id: Option<i64>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub taker_fee: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub maker_fee: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub liquidation_fee: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub min_base_amount: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub min_quote_amount: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub order_quote_limit: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub supported_size_decimals: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub supported_price_decimals: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub supported_quote_decimals: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub size_decimals: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub price_decimals: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub last_trade_price: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub daily_trades_count: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub daily_base_token_volume: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub daily_quote_token_volume: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub daily_price_low: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub daily_price_high: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub daily_price_change: Option<f64>,
    #[serde(default)]
    pub daily_chart: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}
