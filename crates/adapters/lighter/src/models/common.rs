use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(flatten)]
    pub data: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeMessage {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
}
