use crate::error::Result;
use crate::models::account::*;
use crate::models::pool::PublicPoolsMetadata;
use crate::rest::client::LighterRestClient;

impl LighterRestClient {
    /// Look up accounts by a given field (`"l1_address"`, `"index"`, etc.).
    pub async fn get_account(&self, by: &str, value: &str) -> Result<Accounts> {
        self.get_with_query("/api/v1/account", &[("by", by), ("value", value)])
            .await
    }

    /// Look up account by numeric account index.
    pub async fn get_account_by_index(&self, account_index: i64) -> Result<Accounts> {
        let account_index = account_index.to_string();
        self.get_account("index", account_index.as_str()).await
    }

    /// Look up accounts with an auth header (returns positions, PnL, etc.).
    pub async fn get_detailed_account(
        &self,
        by: &str,
        value: &str,
        auth: &str,
    ) -> Result<DetailedAccounts> {
        self.get_with_auth("/api/v1/account", &[("by", by), ("value", value)], auth)
            .await
    }

    /// Look up account details by numeric account index.
    pub async fn get_detailed_account_by_index(
        &self,
        account_index: i64,
        auth: &str,
    ) -> Result<DetailedAccounts> {
        let account_index = account_index.to_string();
        self.get_detailed_account("index", account_index.as_str(), auth)
            .await
    }

    pub async fn get_account_api_keys(
        &self,
        account_index: i64,
        auth: &str,
    ) -> Result<AccountApiKeys> {
        let account_index = account_index.to_string();
        self.get_with_auth(
            "/api/v1/apikeys",
            &[("account_index", account_index.as_str())],
            auth,
        )
        .await
    }

    pub async fn get_account_limits(
        &self,
        account_index: i64,
        auth: &str,
    ) -> Result<AccountLimits> {
        let account_index = account_index.to_string();
        self.get_with_auth(
            "/api/v1/accountLimits",
            &[("account_index", account_index.as_str())],
            auth,
        )
        .await
    }

    pub async fn get_account_metadata(
        &self,
        by: &str,
        value: &str,
        auth: &str,
    ) -> Result<AccountMetadatas> {
        self.get_with_auth(
            "/api/v1/accountMetadata",
            &[("by", by), ("value", value)],
            auth,
        )
        .await
    }

    pub async fn get_account_metadata_by_index(
        &self,
        account_index: i64,
        auth: &str,
    ) -> Result<AccountMetadatas> {
        let account_index = account_index.to_string();
        self.get_account_metadata("index", account_index.as_str(), auth)
            .await
    }

    pub async fn get_account_pnl(&self, account_index: i64, auth: &str) -> Result<AccountPnl> {
        let account_index = account_index.to_string();
        self.get_with_auth(
            "/api/v1/pnl",
            &[("account_index", account_index.as_str())],
            auth,
        )
        .await
    }

    pub async fn get_liquidations(
        &self,
        account_index: i64,
        limit: i64,
        market_id: Option<i64>,
        cursor: Option<&str>,
        auth: Option<&str>,
    ) -> Result<Liquidations> {
        let account_index = account_index.to_string();
        let limit = limit.to_string();
        let market_id = market_id.map(|value| value.to_string());

        let mut query = vec![
            ("account_index", account_index.as_str()),
            ("limit", limit.as_str()),
        ];
        if let Some(ref market_id) = market_id {
            query.push(("market_id", market_id.as_str()));
        }
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor));
        }

        match auth {
            Some(auth) => {
                self.get_with_auth("/api/v1/liquidations", &query, auth)
                    .await
            }
            None => self.get_with_query("/api/v1/liquidations", &query).await,
        }
    }

    pub async fn get_sub_accounts(&self, l1_address: &str) -> Result<SubAccounts> {
        self.get_with_query("/api/v1/accountsByL1Address", &[("l1_address", l1_address)])
            .await
    }

    pub async fn get_l1_metadata(
        &self,
        l1_address: &str,
        auth: Option<&str>,
    ) -> Result<serde_json::Value> {
        let query = [("l1_address", l1_address)];
        match auth {
            Some(auth) => self.get_with_auth("/api/v1/l1Metadata", &query, auth).await,
            None => self.get_with_query("/api/v1/l1Metadata", &query).await,
        }
    }

    /// Change the account tier (e.g. "standard" -> "premium").
    pub async fn change_account_tier(
        &self,
        account_index: i64,
        new_tier: &str,
        auth: &str,
    ) -> Result<ChangeAccountTierResponse> {
        let account_index = account_index.to_string();
        self.post_form_with_auth(
            "/api/v1/changeAccountTier",
            &[
                ("account_index", account_index.as_str()),
                ("new_tier", new_tier),
            ],
            auth,
        )
        .await
    }

    pub async fn get_public_pools_metadata(
        &self,
        filter: &str,
        index: i64,
        limit: i64,
        account_index: Option<i64>,
        auth: Option<&str>,
    ) -> Result<PublicPoolsMetadata> {
        let index = index.to_string();
        let limit = limit.to_string();
        let account_index = account_index.map(|value| value.to_string());

        let mut query = vec![
            ("filter", filter),
            ("index", index.as_str()),
            ("limit", limit.as_str()),
        ];
        if let Some(ref account_index) = account_index {
            query.push(("account_index", account_index.as_str()));
        }

        match auth {
            Some(auth) => {
                self.get_with_auth("/api/v1/publicPoolsMetadata", &query, auth)
                    .await
            }
            None => {
                self.get_with_query("/api/v1/publicPoolsMetadata", &query)
                    .await
            }
        }
    }
}
