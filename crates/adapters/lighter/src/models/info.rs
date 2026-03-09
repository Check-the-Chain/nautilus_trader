use serde::{Deserialize, Deserializer, Serialize};

fn option_i64_from_string_or_number<'de, D: Deserializer<'de>>(
    de: D,
) -> Result<Option<i64>, D::Error> {
    let v = Option::<serde_json::Value>::deserialize(de)?;
    Ok(match v {
        Some(serde_json::Value::Number(n)) => n.as_i64(),
        Some(serde_json::Value::String(s)) => s.parse::<i64>().ok(),
        _ => None,
    })
}

fn option_f64_from_string_or_number<'de, D: Deserializer<'de>>(
    de: D,
) -> Result<Option<f64>, D::Error> {
    let v = Option::<serde_json::Value>::deserialize(de)?;
    Ok(match v {
        Some(serde_json::Value::Number(n)) => n.as_f64(),
        Some(serde_json::Value::String(s)) => s.parse::<f64>().ok(),
        _ => None,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeStats {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default, deserialize_with = "option_i64_from_string_or_number")]
    pub total: Option<i64>,
    #[serde(default)]
    pub order_book_stats: Vec<OrderBookStats>,
    #[serde(default, deserialize_with = "option_f64_from_string_or_number")]
    pub daily_usd_volume: Option<f64>,
    #[serde(default, deserialize_with = "option_i64_from_string_or_number")]
    pub daily_trades_count: Option<i64>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookStats {
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default, deserialize_with = "option_f64_from_string_or_number")]
    pub last_trade_price: Option<f64>,
    #[serde(default, deserialize_with = "option_i64_from_string_or_number")]
    pub daily_trades_count: Option<i64>,
    #[serde(default, deserialize_with = "option_f64_from_string_or_number")]
    pub daily_base_token_volume: Option<f64>,
    #[serde(default, deserialize_with = "option_f64_from_string_or_number")]
    pub daily_quote_token_volume: Option<f64>,
    #[serde(default, deserialize_with = "option_f64_from_string_or_number")]
    pub daily_price_change: Option<f64>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorInfo {
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub is_active: Option<bool>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferFeeInfo {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default, deserialize_with = "option_i64_from_string_or_number")]
    pub transfer_fee_usdc: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawalDelay {
    #[serde(default, deserialize_with = "option_i64_from_string_or_number")]
    pub seconds: Option<i64>,
}
