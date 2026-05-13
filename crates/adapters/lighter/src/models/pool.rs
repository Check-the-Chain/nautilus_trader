use serde::{Deserialize, Serialize};

use super::de::{opt_f64_from_string_or_number, opt_i64_from_string_or_number};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicPoolInfo {
    #[serde(default)]
    #[serde(deserialize_with = "opt_i64_from_string_or_number")]
    pub status: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub operator_fee: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub min_operator_share_rate: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub total_shares: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub operator_shares: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub annual_percentage_yield: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub sharpe_ratio: Option<f64>,
    #[serde(default)]
    pub daily_returns: Vec<DailyReturn>,
    #[serde(default)]
    pub share_prices: Vec<SharePrice>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyReturn {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub timestamp: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub daily_return: Option<f64>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharePrice {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub timestamp: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub share_price: Option<f64>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicPoolMetadata {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub account_index: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub created_at: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub master_account_index: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub public_pool_index: Option<i64>,
    #[serde(default)]
    pub info: Option<PublicPoolInfo>,
    #[serde(default)]
    pub account_share: Option<PublicPoolShare>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicPoolShare {
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub amount: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub transaction_time: Option<i64>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicPoolsMetadata {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub pools: Vec<PublicPoolMetadata>,
}
