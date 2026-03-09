use serde::{Deserialize, Serialize};

use super::de::{opt_f64_from_string_or_number, opt_i64_from_string_or_number};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketConfig {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerpsMarketStats {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub market_id: Option<i64>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub last_trade_price: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub mark_price: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub index_price: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub open_interest: Option<f64>,
    #[serde(
        default,
        alias = "funding_timestamp",
        deserialize_with = "opt_i64_from_string_or_number"
    )]
    pub next_funding_time: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub current_funding_rate: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub funding_rate: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub funding_countdown: Option<i64>,
    #[serde(
        default,
        alias = "daily_quote_token_volume",
        deserialize_with = "opt_f64_from_string_or_number"
    )]
    pub volume_24h: Option<f64>,
    #[serde(
        default,
        alias = "daily_price_high",
        deserialize_with = "opt_f64_from_string_or_number"
    )]
    pub high_24h: Option<f64>,
    #[serde(
        default,
        alias = "daily_price_low",
        deserialize_with = "opt_f64_from_string_or_number"
    )]
    pub low_24h: Option<f64>,
    #[serde(
        default,
        alias = "daily_price_change",
        deserialize_with = "opt_f64_from_string_or_number"
    )]
    pub change_24h: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotMarketStats {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub market_id: Option<i64>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub last_price: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub volume_24h: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub high_24h: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub low_24h: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub change_24h: Option<f64>,
}
