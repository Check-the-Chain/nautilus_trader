use serde::{Deserialize, Serialize};

use super::de::{opt_f64_from_string_or_number, opt_i64_from_string_or_number};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub open: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub high: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub low: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub close: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub volume: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candles {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub candles: Vec<Candle>,
    #[serde(default)]
    pub cursor: Option<String>,
}
