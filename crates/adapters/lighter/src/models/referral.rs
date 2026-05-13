use serde::{Deserialize, Serialize};

use super::de::{opt_i64_from_string_or_number, opt_string_from_string_or_number};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferralCode {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub referral_code: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub remaining_usage: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserReferrals {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub cursor: Option<i64>,
    #[serde(default)]
    pub referrals: Vec<UserReferral>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserReferral {
    #[serde(default)]
    pub l1_address: Option<String>,
    #[serde(default)]
    pub referral_code: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub used_at: Option<i64>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferralActionResponse {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub success: Option<bool>,
    #[serde(default, deserialize_with = "opt_string_from_string_or_number")]
    pub referral_code: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub remaining_usage: Option<i64>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}
