use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait NonceManager: Send + Sync {
    async fn next_nonce(&self) -> Result<(u8, i64)>;
    /// Get the next nonce for a specific key without rotating the key selection.
    /// Used for batch transactions where all txs must use the same API key.
    async fn next_nonce_for_key(&self, api_key_index: u8) -> Result<(u8, i64)>;
    async fn acknowledge_failure(&self, api_key_index: u8);
    async fn hard_refresh_nonce(&self, api_key_index: u8) -> Result<()>;
}
