use serde::{Deserialize, Serialize};

use super::de::{opt_f64_from_string_or_number, opt_i64_from_string_or_number};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RespSendTx {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub tx_hash: Option<String>,
    #[serde(default)]
    pub predicted_execution_time_ms: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub volume_quota_remaining: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RespSendTxBatch {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub tx_hash: Option<Vec<String>>,
    #[serde(default)]
    pub predicted_execution_time_ms: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub volume_quota_remaining: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedTx {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub hash: Option<String>,
    #[serde(
        rename = "type",
        default,
        deserialize_with = "opt_i64_from_string_or_number"
    )]
    pub tx_type: Option<i64>,
    #[serde(default)]
    pub info: Option<String>,
    #[serde(default)]
    pub event_info: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub status: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub transaction_index: Option<i64>,
    #[serde(default)]
    pub l1_address: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub account_index: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub nonce: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub expire_at: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub block_height: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub queued_at: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub executed_at: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub sequence_index: Option<i64>,
    #[serde(default)]
    pub parent_hash: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub api_key_index: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub transaction_time: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub committed_at: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub verified_at: Option<i64>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}
