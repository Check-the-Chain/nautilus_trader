use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::error::Result;
use crate::rest::client::LighterRestClient;

use super::manager::NonceManager;

pub struct ApiNonceManager {
    account_index: i64,
    api_keys: Vec<u8>,
    current_key_idx: Mutex<usize>,
    last_used: Mutex<HashMap<u8, std::time::Instant>>,
    /// Local nonce tracking for batch operations (next_nonce_for_key).
    local_nonces: Mutex<HashMap<u8, i64>>,
    rest_client: LighterRestClient,
}

impl ApiNonceManager {
    pub fn new(account_index: i64, api_keys: Vec<u8>, rest_client: LighterRestClient) -> Self {
        Self {
            account_index,
            api_keys,
            current_key_idx: Mutex::new(0),
            last_used: Mutex::new(HashMap::new()),
            local_nonces: Mutex::new(HashMap::new()),
            rest_client,
        }
    }
}

const MIN_WAIT_MS: u64 = 350;

#[async_trait]
impl NonceManager for ApiNonceManager {
    async fn next_nonce(&self) -> Result<(u8, i64)> {
        let mut idx = self.current_key_idx.lock().await;
        let api_key = self.api_keys[*idx];
        *idx = (*idx + 1) % self.api_keys.len();

        // Ensure minimum wait between same key uses
        {
            let mut last_used = self.last_used.lock().await;
            if let Some(&last) = last_used.get(&api_key) {
                let elapsed = last.elapsed();
                if elapsed < std::time::Duration::from_millis(MIN_WAIT_MS) {
                    let wait = std::time::Duration::from_millis(MIN_WAIT_MS) - elapsed;
                    drop(last_used);
                    tokio::time::sleep(wait).await;
                    last_used = self.last_used.lock().await;
                }
            }
            last_used.insert(api_key, std::time::Instant::now());
        }

        let resp = self
            .rest_client
            .get_next_nonce(self.account_index, api_key)
            .await?;
        let nonce = resp.nonce.unwrap_or(0);
        // Store nonce+1 locally so next_nonce_for_key can continue the sequence.
        let mut local = self.local_nonces.lock().await;
        local.insert(api_key, nonce + 1);
        Ok((api_key, nonce))
    }

    async fn next_nonce_for_key(&self, api_key_index: u8) -> Result<(u8, i64)> {
        let mut local = self.local_nonces.lock().await;
        if let Some(nonce) = local.get_mut(&api_key_index) {
            let current = *nonce;
            *nonce += 1;
            Ok((api_key_index, current))
        } else {
            // No local state — fall back to API fetch.
            drop(local);
            let resp = self
                .rest_client
                .get_next_nonce(self.account_index, api_key_index)
                .await?;
            let nonce = resp.nonce.unwrap_or(0);
            let mut local = self.local_nonces.lock().await;
            local.insert(api_key_index, nonce + 1);
            Ok((api_key_index, nonce))
        }
    }

    async fn acknowledge_failure(&self, _api_key_index: u8) {
        // API nonce manager fetches fresh nonce each time, no rollback needed
    }

    async fn hard_refresh_nonce(&self, _api_key_index: u8) -> Result<()> {
        // No local state to refresh
        Ok(())
    }
}
