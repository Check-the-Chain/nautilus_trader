use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub trade_id: i64,
    #[serde(default)]
    pub tx_hash: String,
    #[serde(rename = "type", default)]
    pub trade_type: String,
    pub market_id: i64,
    pub size: String,
    pub price: String,
    #[serde(default)]
    pub usd_amount: String,
    #[serde(default)]
    pub ask_id: i64,
    #[serde(default)]
    pub bid_id: i64,
    #[serde(default)]
    pub ask_client_id: Option<i64>,
    #[serde(default)]
    pub bid_client_id: Option<i64>,
    #[serde(default)]
    pub ask_account_id: i64,
    #[serde(default)]
    pub bid_account_id: i64,
    pub is_maker_ask: bool,
    #[serde(default)]
    pub block_height: i64,
    pub timestamp: i64,
    #[serde(default)]
    pub taker_fee: Option<i64>,
    #[serde(default)]
    pub taker_position_size_before: Option<String>,
    #[serde(default)]
    pub taker_entry_quote_before: Option<String>,
    #[serde(default)]
    pub taker_initial_margin_fraction_before: Option<i64>,
    #[serde(default)]
    pub taker_position_sign_changed: Option<bool>,
    #[serde(default)]
    pub maker_fee: Option<i64>,
    #[serde(default)]
    pub maker_position_size_before: Option<String>,
    #[serde(default)]
    pub maker_entry_quote_before: Option<String>,
    #[serde(default)]
    pub maker_initial_margin_fraction_before: Option<i64>,
    #[serde(default)]
    pub maker_position_sign_changed: Option<bool>,
    #[serde(default)]
    pub transaction_time: i64,
    #[serde(default)]
    pub ask_account_pnl: Option<String>,
    #[serde(default)]
    pub bid_account_pnl: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trades {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub trades: Vec<Trade>,
    #[serde(default)]
    pub cursor: Option<String>,
}
