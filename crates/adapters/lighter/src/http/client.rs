// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Low-level HTTP client wrappers for the Lighter adapter.

use std::{collections::HashMap, sync::Arc};

use serde::Serialize;
use tokio::sync::Mutex;

use crate::{
    client::{LighterCancelOrderRequest, LighterSubmitOrderRequest, SignerClient},
    config::Config,
    error::{Result, SdkError},
    models::{asset::Asset, order_book::OrderBook},
    nonce::NonceManagerType,
    rest::client::LighterRestClient,
};

#[derive(Debug, Clone)]
struct LighterSignerCredentials {
    account_index: i64,
    api_private_keys: HashMap<u8, String>,
    nonce_mode: NonceManagerType,
}

/// Low-level HTTP and signer client used by the Python adapter layer.
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.lighter", from_py_object)
)]
pub struct LighterHttpClient {
    config: Config,
    rest: LighterRestClient,
    signer_credentials: Option<LighterSignerCredentials>,
    signer: Arc<Mutex<Option<Arc<SignerClient>>>>,
}

impl Clone for LighterHttpClient {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            rest: self.rest.clone(),
            signer_credentials: self.signer_credentials.clone(),
            signer: Arc::clone(&self.signer),
        }
    }
}

impl std::fmt::Debug for LighterHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LighterHttpClient")
            .field("api_base_url", &self.config.api_base_url())
            .field("ws_base_url", &self.config.ws_base_url())
            .field("has_signer_credentials", &self.signer_credentials.is_some())
            .finish()
    }
}

#[derive(Debug, Serialize)]
struct LighterMarketMetadata {
    assets: Vec<Asset>,
    order_books: Vec<OrderBook>,
    details: Vec<serde_json::Value>,
}

impl LighterHttpClient {
    pub fn new_public(config: Config) -> Result<Self> {
        let rest = LighterRestClient::new(&config)?;
        Ok(Self {
            config,
            rest,
            signer_credentials: None,
            signer: Arc::new(Mutex::new(None)),
        })
    }

    pub fn with_signer(
        config: Config,
        account_index: i64,
        api_private_keys: HashMap<u8, String>,
        nonce_mode: NonceManagerType,
    ) -> Result<Self> {
        let rest = LighterRestClient::new(&config)?;
        Ok(Self {
            config,
            rest,
            signer_credentials: Some(LighterSignerCredentials {
                account_index,
                api_private_keys,
                nonce_mode,
            }),
            signer: Arc::new(Mutex::new(None)),
        })
    }

    #[must_use]
    pub fn api_base_url(&self) -> String {
        self.config.api_base_url()
    }

    #[must_use]
    pub fn ws_base_url(&self) -> String {
        self.config.ws_base_url()
    }

    pub async fn load_market_metadata_json(&self) -> Result<String> {
        let order_books = self.rest.get_order_books().await?;
        let assets = self.rest.get_asset_details().await?;

        let details = self.rest.get_all_order_book_details().await?;

        let flattened_details = details
            .order_book_details
            .into_iter()
            .map(|detail| serde_json::to_value(detail).map_err(SdkError::from))
            .chain(
                details
                    .spot_order_book_details
                    .into_iter()
                    .map(|detail| serde_json::to_value(detail).map_err(SdkError::from)),
            )
            .collect::<Result<Vec<_>>>()?;

        serde_json::to_string(&LighterMarketMetadata {
            assets: assets.asset_details,
            order_books: order_books.order_books,
            details: flattened_details,
        })
        .map_err(Into::into)
    }

    async fn ensure_signer(&self) -> Result<Arc<SignerClient>> {
        if let Some(existing) = self.signer.lock().await.as_ref() {
            return Ok(existing.clone());
        }

        let creds = self
            .signer_credentials
            .clone()
            .ok_or_else(|| SdkError::Other("Signer credentials not configured".to_string()))?;

        let signer = Arc::new(
            SignerClient::new(
                self.config.clone(),
                creds.account_index,
                creds.api_private_keys,
                creds.nonce_mode,
            )
            .await?,
        );

        let mut guard = self.signer.lock().await;
        *guard = Some(signer.clone());
        Ok(signer)
    }

    pub async fn create_auth_token(
        &self,
        deadline_secs: i64,
        api_key_index: Option<u8>,
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        signer.create_auth_token(deadline_secs, api_key_index)
    }

    pub async fn submit_order(
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
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let (_, response) = signer
            .create_order(
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
                api_key_index,
                nonce,
            )
            .await?;
        serde_json::to_string(&response).map_err(Into::into)
    }

    pub async fn submit_order_batch(
        &self,
        requests: Vec<LighterSubmitOrderRequest>,
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let response = signer.submit_order_batch(&requests).await?;
        serde_json::to_string(&response).map_err(Into::into)
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
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let (_, response) = signer
            .modify_order(
                market_index,
                order_index,
                base_amount,
                price,
                trigger_price,
                api_key_index,
                nonce,
            )
            .await?;
        serde_json::to_string(&response).map_err(Into::into)
    }

    pub async fn cancel_order(
        &self,
        market_index: i32,
        order_index: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let (_, response) = signer
            .cancel_order(market_index, order_index, api_key_index, nonce)
            .await?;
        serde_json::to_string(&response).map_err(Into::into)
    }

    pub async fn cancel_order_batch(
        &self,
        requests: Vec<LighterCancelOrderRequest>,
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let response = signer.cancel_order_batch(&requests).await?;
        serde_json::to_string(&response).map_err(Into::into)
    }

    pub async fn cancel_all_orders(
        &self,
        time_in_force: i32,
        timestamp_ms: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let (_, response) = signer
            .cancel_all_orders(time_in_force, timestamp_ms, api_key_index, nonce)
            .await?;
        serde_json::to_string(&response).map_err(Into::into)
    }

    pub async fn update_leverage(
        &self,
        market_index: i32,
        initial_margin_fraction: i32,
        margin_mode: i32,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let (_, response) = signer
            .update_leverage(
                market_index,
                initial_margin_fraction,
                margin_mode,
                api_key_index,
                nonce,
            )
            .await?;
        serde_json::to_string(&response).map_err(Into::into)
    }

    pub async fn update_margin(
        &self,
        market_index: i32,
        usdc_amount: i64,
        direction: i32,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let (_, response) = signer
            .update_margin(market_index, usdc_amount, direction, api_key_index, nonce)
            .await?;
        serde_json::to_string(&response).map_err(Into::into)
    }

    pub async fn withdraw(
        &self,
        asset_index: i32,
        route_type: i32,
        amount: u64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let (_, response) = signer
            .withdraw(asset_index, route_type, amount, api_key_index, nonce)
            .await?;
        serde_json::to_string(&response).map_err(Into::into)
    }

    pub async fn change_pub_key(
        &self,
        new_pub_key: &str,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let (_, response) = signer
            .change_pub_key(new_pub_key, api_key_index, nonce)
            .await?;
        serde_json::to_string(&response).map_err(Into::into)
    }

    pub async fn create_sub_account(
        &self,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let (_, response) = signer.create_sub_account(api_key_index, nonce).await?;
        serde_json::to_string(&response).map_err(Into::into)
    }

    pub async fn create_public_pool(
        &self,
        operator_fee: i64,
        initial_total_shares: i32,
        min_operator_share_rate: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let (_, response) = signer
            .create_public_pool(
                operator_fee,
                initial_total_shares,
                min_operator_share_rate,
                api_key_index,
                nonce,
            )
            .await?;
        serde_json::to_string(&response).map_err(Into::into)
    }

    pub async fn update_public_pool(
        &self,
        public_pool_index: i64,
        status: i32,
        operator_fee: i64,
        min_operator_share_rate: i32,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let (_, response) = signer
            .update_public_pool(
                public_pool_index,
                status,
                operator_fee,
                min_operator_share_rate,
                api_key_index,
                nonce,
            )
            .await?;
        serde_json::to_string(&response).map_err(Into::into)
    }

    pub async fn mint_shares(
        &self,
        public_pool_index: i64,
        share_amount: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let (_, response) = signer
            .mint_shares(public_pool_index, share_amount, api_key_index, nonce)
            .await?;
        serde_json::to_string(&response).map_err(Into::into)
    }

    pub async fn burn_shares(
        &self,
        public_pool_index: i64,
        share_amount: i64,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let (_, response) = signer
            .burn_shares(public_pool_index, share_amount, api_key_index, nonce)
            .await?;
        serde_json::to_string(&response).map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn transfer(
        &self,
        to_account_index: i64,
        asset_index: i16,
        from_route_type: u8,
        to_route_type: u8,
        amount: i64,
        usdc_fee: i64,
        memo: String,
        api_key_index: Option<u8>,
        nonce: Option<i64>,
    ) -> Result<String> {
        let signer = self.ensure_signer().await?;
        let (_, response) = signer
            .transfer(
                to_account_index,
                asset_index,
                from_route_type,
                to_route_type,
                amount,
                usdc_fee,
                &memo,
                api_key_index,
                nonce,
            )
            .await?;
        serde_json::to_string(&response).map_err(Into::into)
    }

    pub fn rest(&self) -> &LighterRestClient {
        &self.rest
    }
}
