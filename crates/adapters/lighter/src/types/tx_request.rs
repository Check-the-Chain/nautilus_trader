use super::order_info::OrderInfo;

#[derive(Debug, Clone)]
pub struct ChangePubKeyReq {
    pub pub_key: [u8; 40],
}

#[derive(Debug, Clone)]
pub struct TransferTxReq {
    pub to_account_index: i64,
    pub asset_index: i16,
    pub from_route_type: u8,
    pub to_route_type: u8,
    pub amount: i64,
    pub usdc_fee: i64,
    pub memo: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct WithdrawTxReq {
    pub asset_index: i16,
    pub route_type: u8,
    pub amount: u64,
}

#[derive(Debug, Clone)]
pub struct CreateOrderTxReq {
    pub market_index: i16,
    pub client_order_index: i64,
    pub base_amount: i64,
    pub price: u32,
    pub is_ask: u8,
    pub order_type: u8,
    pub time_in_force: u8,
    pub reduce_only: u8,
    pub trigger_price: u32,
    pub order_expiry: i64,
}

#[derive(Debug, Clone)]
pub struct CreateGroupedOrdersTxReq {
    pub grouping_type: u8,
    pub orders: Vec<CreateOrderTxReq>,
}

#[derive(Debug, Clone)]
pub struct ModifyOrderTxReq {
    pub market_index: i16,
    pub index: i64,
    pub base_amount: i64,
    pub price: u32,
    pub trigger_price: u32,
}

#[derive(Debug, Clone)]
pub struct CancelOrderTxReq {
    pub market_index: i16,
    pub index: i64,
}

#[derive(Debug, Clone)]
pub struct CancelAllOrdersTxReq {
    pub time_in_force: u8,
    pub time: i64,
}

#[derive(Debug, Clone)]
pub struct CreatePublicPoolTxReq {
    pub operator_fee: i64,
    pub initial_total_shares: i64,
    pub min_operator_share_rate: u16,
}

#[derive(Debug, Clone)]
pub struct UpdatePublicPoolTxReq {
    pub public_pool_index: i64,
    pub status: u8,
    pub operator_fee: i64,
    pub min_operator_share_rate: u16,
}

#[derive(Debug, Clone)]
pub struct MintSharesTxReq {
    pub public_pool_index: i64,
    pub share_amount: i64,
}

#[derive(Debug, Clone)]
pub struct BurnSharesTxReq {
    pub public_pool_index: i64,
    pub share_amount: i64,
}

#[derive(Debug, Clone)]
pub struct UpdateLeverageTxReq {
    pub market_index: i16,
    pub initial_margin_fraction: u16,
    pub margin_mode: u8,
}

#[derive(Debug, Clone)]
pub struct UpdateMarginTxReq {
    pub market_index: i16,
    pub usdc_amount: i64,
    pub direction: u8,
}

impl CreateOrderTxReq {
    pub fn to_order_info(&self) -> OrderInfo {
        OrderInfo {
            market_index: self.market_index,
            client_order_index: self.client_order_index,
            base_amount: self.base_amount,
            price: self.price,
            is_ask: self.is_ask,
            order_type: self.order_type,
            time_in_force: self.time_in_force,
            reduce_only: self.reduce_only,
            trigger_price: self.trigger_price,
            order_expiry: self.order_expiry,
        }
    }
}
