use serde::{Deserialize, Serialize};

use super::order_info::OrderInfo;

pub trait TxInfo {
    fn tx_type(&self) -> u8;
    fn tx_hash(&self) -> &str;
    fn validate(&self) -> crate::error::Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2ChangePubKeyTxInfo {
    #[serde(rename = "AccountIndex")]
    pub account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    pub api_key_index: u8,
    #[serde(rename = "PubKey")]
    pub pub_key: Vec<u8>,
    #[serde(rename = "L1Sig", default, skip_serializing_if = "String::is_empty")]
    pub l1_sig: String,
    #[serde(rename = "ExpiredAt")]
    pub expired_at: i64,
    #[serde(rename = "Nonce")]
    pub nonce: i64,
    #[serde(rename = "Sig")]
    pub sig: Vec<u8>,
    #[serde(skip)]
    pub signed_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2CreateSubAccountTxInfo {
    #[serde(rename = "AccountIndex")]
    pub account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    pub api_key_index: u8,
    #[serde(rename = "ExpiredAt")]
    pub expired_at: i64,
    #[serde(rename = "Nonce")]
    pub nonce: i64,
    #[serde(rename = "Sig")]
    pub sig: Vec<u8>,
    #[serde(skip)]
    pub signed_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2CreateOrderTxInfo {
    #[serde(rename = "AccountIndex")]
    pub account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    pub api_key_index: u8,
    #[serde(flatten)]
    pub order_info: OrderInfo,
    #[serde(rename = "ExpiredAt")]
    pub expired_at: i64,
    #[serde(rename = "Nonce")]
    pub nonce: i64,
    #[serde(rename = "Sig")]
    pub sig: Vec<u8>,
    #[serde(skip)]
    pub signed_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2CreateGroupedOrdersTxInfo {
    #[serde(rename = "AccountIndex")]
    pub account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    pub api_key_index: u8,
    #[serde(rename = "GroupingType")]
    pub grouping_type: u8,
    #[serde(rename = "Orders")]
    pub orders: Vec<OrderInfo>,
    #[serde(rename = "ExpiredAt")]
    pub expired_at: i64,
    #[serde(rename = "Nonce")]
    pub nonce: i64,
    #[serde(rename = "Sig")]
    pub sig: Vec<u8>,
    #[serde(skip)]
    pub signed_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2CancelOrderTxInfo {
    #[serde(rename = "AccountIndex")]
    pub account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    pub api_key_index: u8,
    #[serde(rename = "MarketIndex")]
    pub market_index: i16,
    #[serde(rename = "Index")]
    pub index: i64,
    #[serde(rename = "ExpiredAt")]
    pub expired_at: i64,
    #[serde(rename = "Nonce")]
    pub nonce: i64,
    #[serde(rename = "Sig")]
    pub sig: Vec<u8>,
    #[serde(skip)]
    pub signed_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2ModifyOrderTxInfo {
    #[serde(rename = "AccountIndex")]
    pub account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    pub api_key_index: u8,
    #[serde(rename = "MarketIndex")]
    pub market_index: i16,
    #[serde(rename = "Index")]
    pub index: i64,
    #[serde(rename = "BaseAmount")]
    pub base_amount: i64,
    #[serde(rename = "Price")]
    pub price: u32,
    #[serde(rename = "TriggerPrice")]
    pub trigger_price: u32,
    #[serde(rename = "ExpiredAt")]
    pub expired_at: i64,
    #[serde(rename = "Nonce")]
    pub nonce: i64,
    #[serde(rename = "Sig")]
    pub sig: Vec<u8>,
    #[serde(skip)]
    pub signed_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2CancelAllOrdersTxInfo {
    #[serde(rename = "AccountIndex")]
    pub account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    pub api_key_index: u8,
    #[serde(rename = "TimeInForce")]
    pub time_in_force: u8,
    #[serde(rename = "Time")]
    pub time: i64,
    #[serde(rename = "ExpiredAt")]
    pub expired_at: i64,
    #[serde(rename = "Nonce")]
    pub nonce: i64,
    #[serde(rename = "Sig")]
    pub sig: Vec<u8>,
    #[serde(skip)]
    pub signed_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2TransferTxInfo {
    #[serde(rename = "FromAccountIndex")]
    pub from_account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    pub api_key_index: u8,
    #[serde(rename = "ToAccountIndex")]
    pub to_account_index: i64,
    #[serde(rename = "AssetIndex")]
    pub asset_index: i16,
    #[serde(rename = "FromRouteType")]
    pub from_route_type: u8,
    #[serde(rename = "ToRouteType")]
    pub to_route_type: u8,
    #[serde(rename = "Amount")]
    pub amount: i64,
    #[serde(rename = "USDCFee")]
    pub usdc_fee: i64,
    #[serde(rename = "Memo")]
    pub memo: [u8; 32],
    #[serde(rename = "ExpiredAt")]
    pub expired_at: i64,
    #[serde(rename = "Nonce")]
    pub nonce: i64,
    #[serde(rename = "Sig")]
    pub sig: Vec<u8>,
    #[serde(rename = "L1Sig", default, skip_serializing_if = "String::is_empty")]
    pub l1_sig: String,
    #[serde(skip)]
    pub signed_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2WithdrawTxInfo {
    #[serde(rename = "FromAccountIndex")]
    pub from_account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    pub api_key_index: u8,
    #[serde(rename = "AssetIndex")]
    pub asset_index: i16,
    #[serde(rename = "RouteType")]
    pub route_type: u8,
    #[serde(rename = "Amount")]
    pub amount: u64,
    #[serde(rename = "ExpiredAt")]
    pub expired_at: i64,
    #[serde(rename = "Nonce")]
    pub nonce: i64,
    #[serde(rename = "Sig")]
    pub sig: Vec<u8>,
    #[serde(skip)]
    pub signed_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2CreatePublicPoolTxInfo {
    #[serde(rename = "AccountIndex")]
    pub account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    pub api_key_index: u8,
    #[serde(rename = "OperatorFee")]
    pub operator_fee: i64,
    #[serde(rename = "InitialTotalShares")]
    pub initial_total_shares: i64,
    #[serde(rename = "MinOperatorShareRate")]
    pub min_operator_share_rate: u16,
    #[serde(rename = "ExpiredAt")]
    pub expired_at: i64,
    #[serde(rename = "Nonce")]
    pub nonce: i64,
    #[serde(rename = "Sig")]
    pub sig: Vec<u8>,
    #[serde(skip)]
    pub signed_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2UpdatePublicPoolTxInfo {
    #[serde(rename = "AccountIndex")]
    pub account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    pub api_key_index: u8,
    #[serde(rename = "PublicPoolIndex")]
    pub public_pool_index: i64,
    #[serde(rename = "Status")]
    pub status: u8,
    #[serde(rename = "OperatorFee")]
    pub operator_fee: i64,
    #[serde(rename = "MinOperatorShareRate")]
    pub min_operator_share_rate: u16,
    #[serde(rename = "ExpiredAt")]
    pub expired_at: i64,
    #[serde(rename = "Nonce")]
    pub nonce: i64,
    #[serde(rename = "Sig")]
    pub sig: Vec<u8>,
    #[serde(skip)]
    pub signed_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2MintSharesTxInfo {
    #[serde(rename = "AccountIndex")]
    pub account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    pub api_key_index: u8,
    #[serde(rename = "PublicPoolIndex")]
    pub public_pool_index: i64,
    #[serde(rename = "ShareAmount")]
    pub share_amount: i64,
    #[serde(rename = "ExpiredAt")]
    pub expired_at: i64,
    #[serde(rename = "Nonce")]
    pub nonce: i64,
    #[serde(rename = "Sig")]
    pub sig: Vec<u8>,
    #[serde(skip)]
    pub signed_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2BurnSharesTxInfo {
    #[serde(rename = "AccountIndex")]
    pub account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    pub api_key_index: u8,
    #[serde(rename = "PublicPoolIndex")]
    pub public_pool_index: i64,
    #[serde(rename = "ShareAmount")]
    pub share_amount: i64,
    #[serde(rename = "ExpiredAt")]
    pub expired_at: i64,
    #[serde(rename = "Nonce")]
    pub nonce: i64,
    #[serde(rename = "Sig")]
    pub sig: Vec<u8>,
    #[serde(skip)]
    pub signed_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2UpdateLeverageTxInfo {
    #[serde(rename = "AccountIndex")]
    pub account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    pub api_key_index: u8,
    #[serde(rename = "MarketIndex")]
    pub market_index: i16,
    #[serde(rename = "InitialMarginFraction")]
    pub initial_margin_fraction: u16,
    #[serde(rename = "MarginMode")]
    pub margin_mode: u8,
    #[serde(rename = "ExpiredAt")]
    pub expired_at: i64,
    #[serde(rename = "Nonce")]
    pub nonce: i64,
    #[serde(rename = "Sig")]
    pub sig: Vec<u8>,
    #[serde(skip)]
    pub signed_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2UpdateMarginTxInfo {
    #[serde(rename = "AccountIndex")]
    pub account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    pub api_key_index: u8,
    #[serde(rename = "MarketIndex")]
    pub market_index: i16,
    #[serde(rename = "USDCAmount")]
    pub usdc_amount: i64,
    #[serde(rename = "Direction")]
    pub direction: u8,
    #[serde(rename = "ExpiredAt")]
    pub expired_at: i64,
    #[serde(rename = "Nonce")]
    pub nonce: i64,
    #[serde(rename = "Sig")]
    pub sig: Vec<u8>,
    #[serde(skip)]
    pub signed_hash: String,
}
