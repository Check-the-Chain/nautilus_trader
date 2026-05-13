use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::error::Result;
use crate::rest::client::LighterRestClient;

use super::manager::NonceManager;

pub struct OptimisticNonceManager {
    account_index: i64,
    api_keys: Vec<u8>,
    current_key_idx: Mutex<usize>,
    nonces: Mutex<HashMap<u8, i64>>,
    rest_client: LighterRestClient,
}

impl OptimisticNonceManager {
    pub async fn new(
        account_index: i64,
        api_keys: Vec<u8>,
        rest_client: LighterRestClient,
    ) -> Result<Self> {
        let mut nonces = HashMap::new();
        for &key in &api_keys {
            let resp = rest_client.get_next_nonce(account_index, key).await?;
            let nonce = resp.nonce.unwrap_or(0);
            nonces.insert(key, nonce);
        }
        Ok(Self {
            account_index,
            api_keys,
            current_key_idx: Mutex::new(0),
            nonces: Mutex::new(nonces),
            rest_client,
        })
    }
}

#[async_trait]
impl NonceManager for OptimisticNonceManager {
    async fn next_nonce(&self) -> Result<(u8, i64)> {
        let mut idx = self.current_key_idx.lock().await;
        let api_key = self.api_keys[*idx];
        *idx = (*idx + 1) % self.api_keys.len();

        let mut nonces = self.nonces.lock().await;
        let nonce = nonces.entry(api_key).or_insert(0);
        let current = *nonce;
        *nonce += 1;
        Ok((api_key, current))
    }

    async fn next_nonce_for_key(&self, api_key_index: u8) -> Result<(u8, i64)> {
        let mut nonces = self.nonces.lock().await;
        let nonce = nonces.entry(api_key_index).or_insert(0);
        let current = *nonce;
        *nonce += 1;
        Ok((api_key_index, current))
    }

    async fn acknowledge_failure(&self, api_key_index: u8) {
        let mut nonces = self.nonces.lock().await;
        if let Some(nonce) = nonces.get_mut(&api_key_index)
            && *nonce > 0
        {
            *nonce -= 1;
        }
    }

    async fn hard_refresh_nonce(&self, api_key_index: u8) -> Result<()> {
        let resp = self
            .rest_client
            .get_next_nonce(self.account_index, api_key_index)
            .await?;
        let nonce = resp.nonce.unwrap_or(0);
        let mut nonces = self.nonces.lock().await;
        nonces.insert(api_key_index, nonce);
        Ok(())
    }
}
