use serde::{Deserialize, Serialize};

use super::de::opt_i64_from_string_or_number;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub api_key_index: Option<i64>,
    #[serde(default)]
    pub public_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiToken {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub token: Option<String>,
}
