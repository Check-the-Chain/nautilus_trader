use serde::{Deserialize, Serialize};

use super::de::{opt_f64_from_string_or_number, opt_i64_from_string_or_number};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingRate {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub market_id: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub mark_price: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub index_price: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub funding_rate: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub settlement_time: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingRates {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub funding_rates: Vec<FundingRate>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionFunding {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub timestamp: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub market_id: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub funding_id: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub change: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub rate: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub position_size: Option<f64>,
    #[serde(default)]
    pub position_side: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionFundings {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    #[serde(alias = "fundings")]
    pub position_fundings: Vec<PositionFunding>,
    #[serde(default, alias = "cursor")]
    pub next_cursor: Option<String>,
}
