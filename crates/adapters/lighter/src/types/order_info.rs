use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderInfo {
    #[serde(rename = "MarketIndex")]
    pub market_index: i16,
    #[serde(rename = "ClientOrderIndex")]
    pub client_order_index: i64,
    #[serde(rename = "BaseAmount")]
    pub base_amount: i64,
    #[serde(rename = "Price")]
    pub price: u32,
    #[serde(rename = "IsAsk")]
    pub is_ask: u8,
    #[serde(rename = "Type")]
    pub order_type: u8,
    #[serde(rename = "TimeInForce")]
    pub time_in_force: u8,
    #[serde(rename = "ReduceOnly")]
    pub reduce_only: u8,
    #[serde(rename = "TriggerPrice")]
    pub trigger_price: u32,
    #[serde(rename = "OrderExpiry")]
    pub order_expiry: i64,
}
