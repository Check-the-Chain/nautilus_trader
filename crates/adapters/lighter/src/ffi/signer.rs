#![allow(clippy::too_many_arguments)]

use std::ffi::{CStr, CString};
use std::os::raw::c_char;

use libloading::Symbol;

use crate::error::{Result, SdkError};
use crate::ffi::loader::get_signer;
use crate::ffi::types::*;

pub struct SignedTx {
    pub tx_type: u8,
    pub tx_info: String,
    pub tx_hash: String,
    pub message_to_sign: Option<String>,
}

pub struct GeneratedApiKey {
    pub private_key: String,
    pub public_key: String,
}

type SignCreateOrderFn = unsafe extern "C" fn(
    i32,
    i64,
    i64,
    i32,
    i32,
    i32,
    i32,
    i32,
    i32,
    i64,
    i64,
    i32,
    i64,
) -> SignedTxResponse;

unsafe fn c_str_to_option(ptr: *mut c_char) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        Some(
            unsafe { CStr::from_ptr(ptr) }
                .to_string_lossy()
                .into_owned(),
        )
    }
}

fn decode_signed_tx(resp: SignedTxResponse) -> Result<SignedTx> {
    unsafe {
        if !resp.err.is_null() {
            let err = CStr::from_ptr(resp.err).to_string_lossy().into_owned();
            return Err(SdkError::Ffi(err));
        }
        Ok(SignedTx {
            tx_type: resp.tx_type,
            tx_info: c_str_to_option(resp.tx_info).unwrap_or_default(),
            tx_hash: c_str_to_option(resp.tx_hash).unwrap_or_default(),
            message_to_sign: c_str_to_option(resp.message_to_sign),
        })
    }
}

pub fn generate_api_key() -> Result<GeneratedApiKey> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<unsafe extern "C" fn() -> ApiKeyResponse> = lib
            .lib()
            .get(b"GenerateAPIKey")
            .map_err(|e| SdkError::Ffi(e.to_string()))?;
        let resp = func();
        if !resp.err.is_null() {
            let err = CStr::from_ptr(resp.err).to_string_lossy().into_owned();
            return Err(SdkError::Ffi(err));
        }
        Ok(GeneratedApiKey {
            private_key: c_str_to_option(resp.private_key).unwrap_or_default(),
            public_key: c_str_to_option(resp.public_key).unwrap_or_default(),
        })
    }
}

pub fn create_client(
    url: &str,
    private_key: &str,
    chain_id: i32,
    api_key_index: i32,
    account_index: i64,
) -> Result<()> {
    let lib = get_signer()?;
    let c_url = CString::new(url).map_err(|e| SdkError::Ffi(e.to_string()))?;
    let c_key = CString::new(private_key).map_err(|e| SdkError::Ffi(e.to_string()))?;
    unsafe {
        let func: Symbol<
            unsafe extern "C" fn(*mut c_char, *mut c_char, i32, i32, i64) -> *mut c_char,
        > = lib
            .lib()
            .get(b"CreateClient")
            .map_err(|e| SdkError::Ffi(e.to_string()))?;
        let err = func(
            c_url.as_ptr() as *mut c_char,
            c_key.as_ptr() as *mut c_char,
            chain_id,
            api_key_index,
            account_index,
        );
        if !err.is_null() {
            let msg = CStr::from_ptr(err).to_string_lossy().into_owned();
            return Err(SdkError::Ffi(msg));
        }
        Ok(())
    }
}

pub fn check_client(api_key_index: i32, account_index: i64) -> Result<()> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<unsafe extern "C" fn(i32, i64) -> *mut c_char> = lib
            .lib()
            .get(b"CheckClient")
            .map_err(|e| SdkError::Ffi(e.to_string()))?;
        let err = func(api_key_index, account_index);
        if !err.is_null() {
            let msg = CStr::from_ptr(err).to_string_lossy().into_owned();
            return Err(SdkError::Ffi(msg));
        }
        Ok(())
    }
}

pub fn sign_change_pub_key(
    pub_key: &str,
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
) -> Result<SignedTx> {
    let lib = get_signer()?;
    let c_pub_key = CString::new(pub_key).map_err(|e| SdkError::Ffi(e.to_string()))?;
    unsafe {
        let func: Symbol<unsafe extern "C" fn(*mut c_char, i64, i32, i64) -> SignedTxResponse> =
            lib.lib()
                .get(b"SignChangePubKey")
                .map_err(|e| SdkError::Ffi(e.to_string()))?;
        decode_signed_tx(func(
            c_pub_key.as_ptr() as *mut c_char,
            nonce,
            api_key_index,
            account_index,
        ))
    }
}

pub fn sign_create_order(
    market_index: i32,
    client_order_index: i64,
    base_amount: i64,
    price: i32,
    is_ask: i32,
    order_type: i32,
    time_in_force: i32,
    reduce_only: i32,
    trigger_price: i32,
    order_expiry: i64,
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
) -> Result<SignedTx> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<SignCreateOrderFn> = lib
            .lib()
            .get(b"SignCreateOrder")
            .map_err(|e| SdkError::Ffi(e.to_string()))?;
        decode_signed_tx(func(
            market_index,
            client_order_index,
            base_amount,
            price,
            is_ask,
            order_type,
            time_in_force,
            reduce_only,
            trigger_price,
            order_expiry,
            nonce,
            api_key_index,
            account_index,
        ))
    }
}

pub fn sign_create_grouped_orders(
    grouping_type: u8,
    orders: &[FfiCreateOrderTxReq],
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
) -> Result<SignedTx> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<
            unsafe extern "C" fn(
                u8,
                *const FfiCreateOrderTxReq,
                i32,
                i64,
                i32,
                i64,
            ) -> SignedTxResponse,
        > = lib
            .lib()
            .get(b"SignCreateGroupedOrders")
            .map_err(|e| SdkError::Ffi(e.to_string()))?;
        decode_signed_tx(func(
            grouping_type,
            orders.as_ptr(),
            orders.len() as i32,
            nonce,
            api_key_index,
            account_index,
        ))
    }
}

pub fn sign_cancel_order(
    market_index: i32,
    order_index: i64,
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
) -> Result<SignedTx> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<unsafe extern "C" fn(i32, i64, i64, i32, i64) -> SignedTxResponse> = lib
            .lib()
            .get(b"SignCancelOrder")
            .map_err(|e| SdkError::Ffi(e.to_string()))?;
        decode_signed_tx(func(
            market_index,
            order_index,
            nonce,
            api_key_index,
            account_index,
        ))
    }
}

pub fn sign_withdraw(
    asset_index: i32,
    route_type: i32,
    amount: u64,
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
) -> Result<SignedTx> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<unsafe extern "C" fn(i32, i32, u64, i64, i32, i64) -> SignedTxResponse> =
            lib.lib()
                .get(b"SignWithdraw")
                .map_err(|e| SdkError::Ffi(e.to_string()))?;
        decode_signed_tx(func(
            asset_index,
            route_type,
            amount,
            nonce,
            api_key_index,
            account_index,
        ))
    }
}

pub fn sign_create_sub_account(
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
) -> Result<SignedTx> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<unsafe extern "C" fn(i64, i32, i64) -> SignedTxResponse> = lib
            .lib()
            .get(b"SignCreateSubAccount")
            .map_err(|e| SdkError::Ffi(e.to_string()))?;
        decode_signed_tx(func(nonce, api_key_index, account_index))
    }
}

pub fn sign_cancel_all_orders(
    time_in_force: i32,
    time: i64,
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
) -> Result<SignedTx> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<unsafe extern "C" fn(i32, i64, i64, i32, i64) -> SignedTxResponse> = lib
            .lib()
            .get(b"SignCancelAllOrders")
            .map_err(|e| SdkError::Ffi(e.to_string()))?;
        decode_signed_tx(func(
            time_in_force,
            time,
            nonce,
            api_key_index,
            account_index,
        ))
    }
}

pub fn sign_modify_order(
    market_index: i32,
    index: i64,
    base_amount: i64,
    price: i64,
    trigger_price: i64,
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
) -> Result<SignedTx> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<
            unsafe extern "C" fn(i32, i64, i64, i64, i64, i64, i32, i64) -> SignedTxResponse,
        > = lib
            .lib()
            .get(b"SignModifyOrder")
            .map_err(|e| SdkError::Ffi(e.to_string()))?;
        decode_signed_tx(func(
            market_index,
            index,
            base_amount,
            price,
            trigger_price,
            nonce,
            api_key_index,
            account_index,
        ))
    }
}

pub fn sign_transfer(
    to_account_index: i64,
    asset_index: i16,
    from_route_type: u8,
    to_route_type: u8,
    amount: i64,
    usdc_fee: i64,
    memo: &str,
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
) -> Result<SignedTx> {
    let lib = get_signer()?;
    let c_memo = CString::new(memo).map_err(|e| SdkError::Ffi(e.to_string()))?;
    unsafe {
        let func: Symbol<
            unsafe extern "C" fn(
                i64,
                i16,
                u8,
                u8,
                i64,
                i64,
                *mut c_char,
                i64,
                i32,
                i64,
            ) -> SignedTxResponse,
        > = lib
            .lib()
            .get(b"SignTransfer")
            .map_err(|e| SdkError::Ffi(e.to_string()))?;
        decode_signed_tx(func(
            to_account_index,
            asset_index,
            from_route_type,
            to_route_type,
            amount,
            usdc_fee,
            c_memo.as_ptr() as *mut c_char,
            nonce,
            api_key_index,
            account_index,
        ))
    }
}

pub fn sign_create_public_pool(
    operator_fee: i64,
    initial_total_shares: i32,
    min_operator_share_rate: i64,
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
) -> Result<SignedTx> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<unsafe extern "C" fn(i64, i32, i64, i64, i32, i64) -> SignedTxResponse> =
            lib.lib()
                .get(b"SignCreatePublicPool")
                .map_err(|e| SdkError::Ffi(e.to_string()))?;
        decode_signed_tx(func(
            operator_fee,
            initial_total_shares,
            min_operator_share_rate,
            nonce,
            api_key_index,
            account_index,
        ))
    }
}

pub fn sign_update_public_pool(
    public_pool_index: i64,
    status: i32,
    operator_fee: i64,
    min_operator_share_rate: i32,
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
) -> Result<SignedTx> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<
            unsafe extern "C" fn(i64, i32, i64, i32, i64, i32, i64) -> SignedTxResponse,
        > = lib
            .lib()
            .get(b"SignUpdatePublicPool")
            .map_err(|e| SdkError::Ffi(e.to_string()))?;
        decode_signed_tx(func(
            public_pool_index,
            status,
            operator_fee,
            min_operator_share_rate,
            nonce,
            api_key_index,
            account_index,
        ))
    }
}

pub fn sign_mint_shares(
    public_pool_index: i64,
    share_amount: i64,
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
) -> Result<SignedTx> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<unsafe extern "C" fn(i64, i64, i64, i32, i64) -> SignedTxResponse> = lib
            .lib()
            .get(b"SignMintShares")
            .map_err(|e| SdkError::Ffi(e.to_string()))?;
        decode_signed_tx(func(
            public_pool_index,
            share_amount,
            nonce,
            api_key_index,
            account_index,
        ))
    }
}

pub fn sign_burn_shares(
    public_pool_index: i64,
    share_amount: i64,
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
) -> Result<SignedTx> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<unsafe extern "C" fn(i64, i64, i64, i32, i64) -> SignedTxResponse> = lib
            .lib()
            .get(b"SignBurnShares")
            .map_err(|e| SdkError::Ffi(e.to_string()))?;
        decode_signed_tx(func(
            public_pool_index,
            share_amount,
            nonce,
            api_key_index,
            account_index,
        ))
    }
}

pub fn sign_update_leverage(
    market_index: i32,
    initial_margin_fraction: i32,
    margin_mode: i32,
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
) -> Result<SignedTx> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<unsafe extern "C" fn(i32, i32, i32, i64, i32, i64) -> SignedTxResponse> =
            lib.lib()
                .get(b"SignUpdateLeverage")
                .map_err(|e| SdkError::Ffi(e.to_string()))?;
        decode_signed_tx(func(
            market_index,
            initial_margin_fraction,
            margin_mode,
            nonce,
            api_key_index,
            account_index,
        ))
    }
}

pub fn sign_update_margin(
    market_index: i32,
    usdc_amount: i64,
    direction: i32,
    nonce: i64,
    api_key_index: i32,
    account_index: i64,
) -> Result<SignedTx> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<unsafe extern "C" fn(i32, i64, i32, i64, i32, i64) -> SignedTxResponse> =
            lib.lib()
                .get(b"SignUpdateMargin")
                .map_err(|e| SdkError::Ffi(e.to_string()))?;
        decode_signed_tx(func(
            market_index,
            usdc_amount,
            direction,
            nonce,
            api_key_index,
            account_index,
        ))
    }
}

pub fn create_auth_token(deadline: i64, api_key_index: i32, account_index: i64) -> Result<String> {
    let lib = get_signer()?;
    unsafe {
        let func: Symbol<unsafe extern "C" fn(i64, i32, i64) -> StrOrErr> = lib
            .lib()
            .get(b"CreateAuthToken")
            .map_err(|e| SdkError::Ffi(e.to_string()))?;
        let resp = func(deadline, api_key_index, account_index);
        if !resp.err.is_null() {
            let err = CStr::from_ptr(resp.err).to_string_lossy().into_owned();
            return Err(SdkError::Ffi(err));
        }
        Ok(c_str_to_option(resp.str_val).unwrap_or_default())
    }
}
