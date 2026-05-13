use serde::{Deserialize, Serialize};

use super::de::{opt_f64_from_string_or_number, opt_i64_from_string_or_number};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositHistory {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub deposits: Vec<DepositHistoryItem>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawHistory {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    #[serde(alias = "withdrawals")]
    pub withdraws: Vec<WithdrawHistoryItem>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferHistory {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub transfers: Vec<TransferHistoryItem>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositHistoryItem {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub id: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub asset_id: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub amount: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub timestamp: Option<i64>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub l1_tx_hash: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawHistoryItem {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub id: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub asset_id: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub amount: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub timestamp: Option<i64>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(rename = "type", default)]
    pub withdraw_type: Option<String>,
    #[serde(default)]
    pub l1_tx_hash: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferHistoryItem {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub id: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub asset_id: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub amount: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub fee: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub timestamp: Option<i64>,
    #[serde(rename = "type", default)]
    pub transfer_type: Option<String>,
    #[serde(default)]
    pub from_l1_address: Option<String>,
    #[serde(default)]
    pub to_l1_address: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub from_account_index: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub to_account_index: Option<i64>,
    #[serde(default)]
    pub from_route: Option<String>,
    #[serde(default)]
    pub to_route: Option<String>,
    #[serde(default)]
    pub tx_hash: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}
