use crate::error::Result;
use crate::models::asset::AssetDetails;
use crate::models::info::*;
use crate::rest::client::LighterRestClient;

impl LighterRestClient {
    pub async fn get_status(&self) -> Result<serde_json::Value> {
        self.get("/").await
    }

    pub async fn get_system_config(&self) -> Result<serde_json::Value> {
        self.get("/api/v1/systemConfig").await
    }

    pub async fn get_exchange_stats(&self) -> Result<ExchangeStats> {
        self.get("/api/v1/exchangeStats").await
    }

    pub async fn get_asset_details(&self) -> Result<AssetDetails> {
        self.get("/api/v1/assetDetails").await
    }

    pub async fn get_layer1_basic_info(&self) -> Result<serde_json::Value> {
        self.get("/api/v1/layer1BasicInfo").await
    }

    pub async fn get_transfer_fee_info(
        &self,
        account_index: i64,
        to_account_index: Option<i64>,
        auth: Option<&str>,
    ) -> Result<TransferFeeInfo> {
        let account_index = account_index.to_string();
        let to_account_index = to_account_index.map(|value| value.to_string());
        let mut query = vec![("account_index", account_index.as_str())];
        if let Some(ref to_account_index) = to_account_index {
            query.push(("to_account_index", to_account_index.as_str()));
        }

        match auth {
            Some(auth) => {
                self.get_with_auth("/api/v1/transferFeeInfo", &query, auth)
                    .await
            }
            None => self.get_with_query("/api/v1/transferFeeInfo", &query).await,
        }
    }

    pub async fn get_withdrawal_delay(&self) -> Result<WithdrawalDelay> {
        self.get("/api/v1/withdrawalDelay").await
    }

    pub async fn get_zk_lighter_info(&self) -> Result<ValidatorInfo> {
        self.get("/info").await
    }
}
