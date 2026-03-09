#![allow(clippy::too_many_arguments)]

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::config::Config;
use crate::error::{Result, SdkError};
use crate::ffi::loader::load_signer;
use crate::ffi::signer;
use crate::ffi::types::FfiCreateOrderTxReq;
use crate::models::transaction::{RespSendTx, RespSendTxBatch};
use crate::nonce::NonceManagerType;
use crate::nonce::api::ApiNonceManager;
use crate::nonce::manager::NonceManager;
use crate::nonce::optimistic::OptimisticNonceManager;
use crate::rest::client::LighterRestClient;

const CODE_OK: i64 = 200;
const CODE_INVALID_NONCE: i64 = 21104;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct LighterSubmitOrderRequest {
    pub market_index: i32,
    pub client_order_index: i64,
    pub base_amount: i64,
    pub price: i32,
    pub is_ask: bool,
    pub order_type: i32,
    pub time_in_force: i32,
    pub reduce_only: bool,
    pub trigger_price: i32,
    pub order_expiry: i64,
    pub api_key_index: Option<u8>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct LighterModifyOrderRequest {
    pub market_index: i32,
    pub order_index: i64,
    pub base_amount: i64,
    pub price: i64,
    pub trigger_price: i64,
    pub api_key_index: Option<u8>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct LighterCancelOrderRequest {
    pub market_index: i32,
    pub order_index: i64,
    pub api_key_index: Option<u8>,
}

pub struct SignerClient {
    pub config: Config,
    pub account_index: i64,
    pub rest: LighterRestClient,
    nonce_manager: Arc<Mutex<Box<dyn NonceManager>>>,
    api_key_dict: HashMap<u8, String>,
}

impl SignerClient {
    pub async fn new(
        config: Config,
        account_index: i64,
        api_private_keys: HashMap<u8, String>,
        nonce_type: NonceManagerType,
    ) -> Result<Self> {
        if api_private_keys.is_empty() {
            return Err(SdkError::NoApiKeys);
        }

        // Trim 0x prefix from keys
        let api_key_dict: HashMap<u8, String> = api_private_keys
            .into_iter()
            .map(|(idx, key)| {
                let trimmed = key.strip_prefix("0x").unwrap_or(&key).to_string();
                (idx, trimmed)
            })
            .collect();

        // Load signer library
        load_signer(config.signer_lib_path.as_deref())?;

        let rest = LighterRestClient::new(&config)?;

        // Create client for each api key
        for (&api_key_index, private_key) in &api_key_dict {
            signer::create_client(
                &config.api_base_url(),
                private_key,
                config.chain_id as i32,
                api_key_index as i32,
                account_index,
            )?;
        }

        // Create nonce manager
        let api_keys: Vec<u8> = api_key_dict.keys().copied().collect();
        let nonce_rest = LighterRestClient::new(&config)?;

        let nonce_manager: Box<dyn NonceManager> = match nonce_type {
            NonceManagerType::Optimistic => {
                Box::new(OptimisticNonceManager::new(account_index, api_keys, nonce_rest).await?)
            }
            NonceManagerType::Api => {
                Box::new(ApiNonceManager::new(account_index, api_keys, nonce_rest))
            }
        };

        Ok(Self {
            config,
            account_index,
            rest,
            nonce_manager: Arc::new(Mutex::new(nonce_manager)),
            api_key_dict,
        })
    }

    pub fn check_client(&self) -> Result<()> {
        for &api_key in self.api_key_dict.keys() {
            signer::check_client(api_key as i32, self.account_index)?;
        }
        Ok(())
    }

    async fn get_api_key_and_nonce(
        &self,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(u8, i64)> {
        if let (Some(key), Some(n)) = (api_key_index, nonce) {
            return Ok((key, n));
        }
        let nm = self.nonce_manager.lock().await;
        nm.next_nonce().await
    }

    async fn reserve_nonces_for_batch(
        &self,
        api_key_index: Option<u8>,
        count: usize,
    ) -> Result<(u8, Vec<i64>)> {
        if count == 0 {
            return Err(SdkError::Other(
                "Cannot reserve nonces for an empty batch".to_string(),
            ));
        }

        let nm = self.nonce_manager.lock().await;
        let (key, first) = match api_key_index {
            Some(key) => nm.next_nonce_for_key(key).await?,
            None => nm.next_nonce().await?,
        };

        let mut nonces = Vec::with_capacity(count);
        nonces.push(first);
        for _ in 1..count {
            let (_, nonce) = nm.next_nonce_for_key(key).await?;
            nonces.push(nonce);
        }

        Ok((key, nonces))
    }

    async fn handle_tx_result(
        &self,
        result: &std::result::Result<RespSendTx, SdkError>,
        api_key_index: u8,
    ) {
        match result {
            Ok(resp) if resp.code == CODE_INVALID_NONCE => {
                let nm = self.nonce_manager.lock().await;
                let _ = nm.hard_refresh_nonce(api_key_index).await;
            }
            Ok(resp) if resp.code != CODE_OK => {
                let nm = self.nonce_manager.lock().await;
                nm.acknowledge_failure(api_key_index).await;
            }
            Err(e) => {
                if matches!(e, SdkError::Json(_)) {
                    // HTTP 200 but response body couldn't be parsed.
                    // The tx was processed — nonce already advanced correctly.
                    return;
                }
                let nm = self.nonce_manager.lock().await;
                let err_str = e.to_string();
                if err_str.contains("invalid nonce") {
                    let _ = nm.hard_refresh_nonce(api_key_index).await;
                } else {
                    nm.acknowledge_failure(api_key_index).await;
                }
            }
            _ => {}
        }
    }

    async fn handle_batch_result(
        &self,
        result: &std::result::Result<RespSendTxBatch, SdkError>,
        api_key_index: u8,
        count: usize,
    ) {
        match result {
            Ok(resp) if resp.code == CODE_INVALID_NONCE => {
                let nm = self.nonce_manager.lock().await;
                let _ = nm.hard_refresh_nonce(api_key_index).await;
            }
            Ok(resp) if resp.code != CODE_OK => {
                self.acknowledge_batch_failure(api_key_index, count).await;
            }
            Err(e) => {
                if matches!(e, SdkError::Json(_)) {
                    // HTTP 200 but response body couldn't be parsed.
                    // The batch was processed — nonce state is already correct.
                    return;
                }

                let err_str = e.to_string();
                if err_str.contains("invalid nonce") {
                    let nm = self.nonce_manager.lock().await;
                    let _ = nm.hard_refresh_nonce(api_key_index).await;
                } else {
                    self.acknowledge_batch_failure(api_key_index, count).await;
                }
            }
            _ => {}
        }
    }

    fn resolve_batch_api_key<I>(&self, api_key_indexes: I) -> Result<Option<u8>>
    where
        I: IntoIterator<Item = Option<u8>>,
    {
        let keys: BTreeSet<u8> = api_key_indexes.into_iter().flatten().collect();
        match keys.len() {
            0 => Ok(None),
            1 => Ok(keys.into_iter().next()),
            _ => Err(SdkError::AmbiguousApiKey),
        }
    }

    pub async fn create_order(
        &self,
        market_index: i32,
        client_order_index: i64,
        base_amount: i64,
        price: i32,
        is_ask: bool,
        order_type: i32,
        time_in_force: i32,
        reduce_only: bool,
        trigger_price: i32,
        order_expiry: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(signer::SignedTx, RespSendTx)> {
        let (key, n) = self.get_api_key_and_nonce(api_key_index, nonce).await?;
        let signed = signer::sign_create_order(
            market_index,
            client_order_index,
            base_amount,
            price,
            is_ask as i32,
            order_type,
            time_in_force,
            reduce_only as i32,
            trigger_price,
            order_expiry,
            n,
            key as i32,
            self.account_index,
        )?;
        let result = self.rest.send_tx(signed.tx_type, &signed.tx_info).await;
        self.handle_tx_result(&result, key).await;
        Ok((signed, result?))
    }

    pub async fn create_grouped_orders(
        &self,
        grouping_type: u8,
        orders: &[FfiCreateOrderTxReq],
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(signer::SignedTx, RespSendTx)> {
        let (key, n) = self.get_api_key_and_nonce(api_key_index, nonce).await?;
        let signed = signer::sign_create_grouped_orders(
            grouping_type,
            orders,
            n,
            key as i32,
            self.account_index,
        )?;
        let result = self.rest.send_tx(signed.tx_type, &signed.tx_info).await;
        self.handle_tx_result(&result, key).await;
        Ok((signed, result?))
    }

    pub async fn cancel_order(
        &self,
        market_index: i32,
        order_index: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(signer::SignedTx, RespSendTx)> {
        let (key, n) = self.get_api_key_and_nonce(api_key_index, nonce).await?;
        let signed = signer::sign_cancel_order(
            market_index,
            order_index,
            n,
            key as i32,
            self.account_index,
        )?;
        let result = self.rest.send_tx(signed.tx_type, &signed.tx_info).await;
        self.handle_tx_result(&result, key).await;
        Ok((signed, result?))
    }

    pub async fn modify_order(
        &self,
        market_index: i32,
        order_index: i64,
        base_amount: i64,
        price: i64,
        trigger_price: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(signer::SignedTx, RespSendTx)> {
        let (key, n) = self.get_api_key_and_nonce(api_key_index, nonce).await?;
        let signed = signer::sign_modify_order(
            market_index,
            order_index,
            base_amount,
            price,
            trigger_price,
            n,
            key as i32,
            self.account_index,
        )?;
        let result = self.rest.send_tx(signed.tx_type, &signed.tx_info).await;
        self.handle_tx_result(&result, key).await;
        Ok((signed, result?))
    }

    pub async fn cancel_all_orders(
        &self,
        time_in_force: i32,
        timestamp_ms: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(signer::SignedTx, RespSendTx)> {
        let (key, n) = self.get_api_key_and_nonce(api_key_index, nonce).await?;
        let signed = signer::sign_cancel_all_orders(
            time_in_force,
            timestamp_ms,
            n,
            key as i32,
            self.account_index,
        )?;
        let result = self.rest.send_tx(signed.tx_type, &signed.tx_info).await;
        self.handle_tx_result(&result, key).await;
        Ok((signed, result?))
    }

    pub async fn withdraw(
        &self,
        asset_index: i32,
        route_type: i32,
        amount: u64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(signer::SignedTx, RespSendTx)> {
        let (key, n) = self.get_api_key_and_nonce(api_key_index, nonce).await?;
        let signed = signer::sign_withdraw(
            asset_index,
            route_type,
            amount,
            n,
            key as i32,
            self.account_index,
        )?;
        let result = self.rest.send_tx(signed.tx_type, &signed.tx_info).await;
        self.handle_tx_result(&result, key).await;
        Ok((signed, result?))
    }

    pub async fn transfer(
        &self,
        to_account_index: i64,
        asset_index: i16,
        from_route_type: u8,
        to_route_type: u8,
        amount: i64,
        usdc_fee: i64,
        memo: &str,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(signer::SignedTx, RespSendTx)> {
        let (key, n) = self.get_api_key_and_nonce(api_key_index, nonce).await?;
        let signed = signer::sign_transfer(
            to_account_index,
            asset_index,
            from_route_type,
            to_route_type,
            amount,
            usdc_fee,
            memo,
            n,
            key as i32,
            self.account_index,
        )?;
        let result = self.rest.send_tx(signed.tx_type, &signed.tx_info).await;
        self.handle_tx_result(&result, key).await;
        Ok((signed, result?))
    }

    pub async fn change_pub_key(
        &self,
        new_pub_key: &str,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(signer::SignedTx, RespSendTx)> {
        let (key, n) = self.get_api_key_and_nonce(api_key_index, nonce).await?;
        let signed = signer::sign_change_pub_key(new_pub_key, n, key as i32, self.account_index)?;
        let result = self.rest.send_tx(signed.tx_type, &signed.tx_info).await;
        self.handle_tx_result(&result, key).await;
        Ok((signed, result?))
    }

    pub async fn create_sub_account(
        &self,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(signer::SignedTx, RespSendTx)> {
        let (key, n) = self.get_api_key_and_nonce(api_key_index, nonce).await?;
        let signed = signer::sign_create_sub_account(n, key as i32, self.account_index)?;
        let result = self.rest.send_tx(signed.tx_type, &signed.tx_info).await;
        self.handle_tx_result(&result, key).await;
        Ok((signed, result?))
    }

    pub async fn create_public_pool(
        &self,
        operator_fee: i64,
        initial_total_shares: i32,
        min_operator_share_rate: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(signer::SignedTx, RespSendTx)> {
        let (key, n) = self.get_api_key_and_nonce(api_key_index, nonce).await?;
        let signed = signer::sign_create_public_pool(
            operator_fee,
            initial_total_shares,
            min_operator_share_rate,
            n,
            key as i32,
            self.account_index,
        )?;
        let result = self.rest.send_tx(signed.tx_type, &signed.tx_info).await;
        self.handle_tx_result(&result, key).await;
        Ok((signed, result?))
    }

    pub async fn update_public_pool(
        &self,
        public_pool_index: i64,
        status: i32,
        operator_fee: i64,
        min_operator_share_rate: i32,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(signer::SignedTx, RespSendTx)> {
        let (key, n) = self.get_api_key_and_nonce(api_key_index, nonce).await?;
        let signed = signer::sign_update_public_pool(
            public_pool_index,
            status,
            operator_fee,
            min_operator_share_rate,
            n,
            key as i32,
            self.account_index,
        )?;
        let result = self.rest.send_tx(signed.tx_type, &signed.tx_info).await;
        self.handle_tx_result(&result, key).await;
        Ok((signed, result?))
    }

    pub async fn mint_shares(
        &self,
        public_pool_index: i64,
        share_amount: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(signer::SignedTx, RespSendTx)> {
        let (key, n) = self.get_api_key_and_nonce(api_key_index, nonce).await?;
        let signed = signer::sign_mint_shares(
            public_pool_index,
            share_amount,
            n,
            key as i32,
            self.account_index,
        )?;
        let result = self.rest.send_tx(signed.tx_type, &signed.tx_info).await;
        self.handle_tx_result(&result, key).await;
        Ok((signed, result?))
    }

    pub async fn burn_shares(
        &self,
        public_pool_index: i64,
        share_amount: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(signer::SignedTx, RespSendTx)> {
        let (key, n) = self.get_api_key_and_nonce(api_key_index, nonce).await?;
        let signed = signer::sign_burn_shares(
            public_pool_index,
            share_amount,
            n,
            key as i32,
            self.account_index,
        )?;
        let result = self.rest.send_tx(signed.tx_type, &signed.tx_info).await;
        self.handle_tx_result(&result, key).await;
        Ok((signed, result?))
    }

    pub async fn update_leverage(
        &self,
        market_index: i32,
        initial_margin_fraction: i32,
        margin_mode: i32,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(signer::SignedTx, RespSendTx)> {
        let (key, n) = self.get_api_key_and_nonce(api_key_index, nonce).await?;
        let signed = signer::sign_update_leverage(
            market_index,
            initial_margin_fraction,
            margin_mode,
            n,
            key as i32,
            self.account_index,
        )?;
        let result = self.rest.send_tx(signed.tx_type, &signed.tx_info).await;
        self.handle_tx_result(&result, key).await;
        Ok((signed, result?))
    }

    pub async fn update_margin(
        &self,
        market_index: i32,
        usdc_amount: i64,
        direction: i32,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<(signer::SignedTx, RespSendTx)> {
        let (key, n) = self.get_api_key_and_nonce(api_key_index, nonce).await?;
        let signed = signer::sign_update_margin(
            market_index,
            usdc_amount,
            direction,
            n,
            key as i32,
            self.account_index,
        )?;
        let result = self.rest.send_tx(signed.tx_type, &signed.tx_info).await;
        self.handle_tx_result(&result, key).await;
        Ok((signed, result?))
    }

    pub fn create_auth_token(&self, deadline: i64, api_key_index: Option<u8>) -> Result<String> {
        let key = api_key_index.unwrap_or_else(|| *self.api_key_dict.keys().next().unwrap());
        signer::create_auth_token(deadline, key as i32, self.account_index)
    }

    pub async fn send_tx(&self, tx_type: u8, tx_info: &str) -> Result<RespSendTx> {
        self.rest.send_tx(tx_type, tx_info).await
    }

    pub async fn send_tx_batch(&self, tx_types: &str, tx_infos: &str) -> Result<RespSendTxBatch> {
        self.rest.send_tx_batch(tx_types, tx_infos).await
    }

    /// Reserve `count` sequential nonces on the same API key for batch operations.
    /// Returns `(api_key_index, nonces)`.
    pub async fn reserve_nonces(&self, count: usize) -> Result<(u8, Vec<i64>)> {
        self.reserve_nonces_for_batch(None, count).await
    }

    /// Sign pre-built transactions and send them as a single batch HTTP call.
    pub async fn sign_and_send_batch(
        &self,
        signed_txs: &[signer::SignedTx],
    ) -> Result<RespSendTxBatch> {
        let tx_types: Vec<u8> = signed_txs.iter().map(|t| t.tx_type).collect();
        let tx_infos: Vec<&str> = signed_txs.iter().map(|t| t.tx_info.as_str()).collect();
        let types_json = serde_json::to_string(&tx_types)?;
        let infos_json = serde_json::to_string(&tx_infos)?;
        self.rest.send_tx_batch(&types_json, &infos_json).await
    }

    /// Roll back `count` nonces for the given API key after a batch failure.
    pub async fn acknowledge_batch_failure(&self, api_key_index: u8, count: usize) {
        let nm = self.nonce_manager.lock().await;
        for _ in 0..count {
            nm.acknowledge_failure(api_key_index).await;
        }
    }

    /// Hard-refresh the nonce for a given API key (e.g. after an invalid-nonce error).
    pub async fn hard_refresh_nonce(&self, api_key_index: u8) -> Result<()> {
        let nm = self.nonce_manager.lock().await;
        nm.hard_refresh_nonce(api_key_index).await
    }

    pub async fn submit_order_batch(
        &self,
        requests: &[LighterSubmitOrderRequest],
    ) -> Result<RespSendTxBatch> {
        if requests.is_empty() {
            return Ok(RespSendTxBatch {
                code: CODE_OK,
                message: None,
                tx_hash: Some(Vec::new()),
                predicted_execution_time_ms: None,
                volume_quota_remaining: None,
            });
        }

        let batch_api_key =
            self.resolve_batch_api_key(requests.iter().map(|request| request.api_key_index))?;
        let (api_key, nonces) = self
            .reserve_nonces_for_batch(batch_api_key, requests.len())
            .await?;

        let mut signed_txs = Vec::with_capacity(requests.len());
        for (request, nonce) in requests.iter().zip(nonces.into_iter()) {
            match signer::sign_create_order(
                request.market_index,
                request.client_order_index,
                request.base_amount,
                request.price,
                request.is_ask as i32,
                request.order_type,
                request.time_in_force,
                request.reduce_only as i32,
                request.trigger_price,
                request.order_expiry,
                nonce,
                api_key as i32,
                self.account_index,
            ) {
                Ok(signed) => signed_txs.push(signed),
                Err(err) => {
                    self.acknowledge_batch_failure(
                        api_key,
                        requests.len().saturating_sub(signed_txs.len()),
                    )
                    .await;
                    return Err(err);
                }
            }
        }

        let result = self.sign_and_send_batch(&signed_txs).await;
        self.handle_batch_result(&result, api_key, requests.len())
            .await;
        result
    }

    pub async fn cancel_order_batch(
        &self,
        requests: &[LighterCancelOrderRequest],
    ) -> Result<RespSendTxBatch> {
        if requests.is_empty() {
            return Ok(RespSendTxBatch {
                code: CODE_OK,
                message: None,
                tx_hash: Some(Vec::new()),
                predicted_execution_time_ms: None,
                volume_quota_remaining: None,
            });
        }

        let batch_api_key =
            self.resolve_batch_api_key(requests.iter().map(|request| request.api_key_index))?;
        let (api_key, nonces) = self
            .reserve_nonces_for_batch(batch_api_key, requests.len())
            .await?;

        let mut signed_txs = Vec::with_capacity(requests.len());
        for (request, nonce) in requests.iter().zip(nonces.into_iter()) {
            match signer::sign_cancel_order(
                request.market_index,
                request.order_index,
                nonce,
                api_key as i32,
                self.account_index,
            ) {
                Ok(signed) => signed_txs.push(signed),
                Err(err) => {
                    self.acknowledge_batch_failure(
                        api_key,
                        requests.len().saturating_sub(signed_txs.len()),
                    )
                    .await;
                    return Err(err);
                }
            }
        }

        let result = self.sign_and_send_batch(&signed_txs).await;
        self.handle_batch_result(&result, api_key, requests.len())
            .await;
        result
    }
}
