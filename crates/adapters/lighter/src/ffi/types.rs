use std::os::raw::c_char;

#[repr(C)]
pub struct ApiKeyResponse {
    pub private_key: *mut c_char,
    pub public_key: *mut c_char,
    pub err: *mut c_char,
}

#[repr(C)]
pub struct SignedTxResponse {
    pub tx_type: u8,
    pub tx_info: *mut c_char,
    pub tx_hash: *mut c_char,
    pub message_to_sign: *mut c_char,
    pub err: *mut c_char,
}

#[repr(C)]
pub struct StrOrErr {
    pub str_val: *mut c_char,
    pub err: *mut c_char,
}

#[repr(C)]
pub struct FfiCreateOrderTxReq {
    pub market_index: u8,
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
