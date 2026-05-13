use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub asset_id: i64,
    #[serde(default)]
    pub balance: Option<String>,
    #[serde(default)]
    pub locked_balance: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl Asset {
    pub fn balance_f64(&self) -> Option<f64> {
        self.balance.as_deref()?.parse::<f64>().ok()
    }

    pub fn locked_balance_f64(&self) -> Option<f64> {
        self.locked_balance.as_deref()?.parse::<f64>().ok()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetDetails {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    #[serde(alias = "assets")]
    pub asset_details: Vec<Asset>,
}
