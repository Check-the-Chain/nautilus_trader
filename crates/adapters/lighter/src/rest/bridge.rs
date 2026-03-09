use crate::error::Result;
use crate::models::bridge::*;
use crate::rest::client::LighterRestClient;

impl LighterRestClient {
    pub async fn create_intent_address(
        &self,
        chain_id: &str,
        from_addr: &str,
        amount: &str,
        is_external_deposit: bool,
    ) -> Result<serde_json::Value> {
        let is_external_deposit = if is_external_deposit { "true" } else { "false" };
        self.post_form(
            "/api/v1/createIntentAddress",
            &[
                ("chain_id", chain_id),
                ("from_addr", from_addr),
                ("amount", amount),
                ("is_external_deposit", is_external_deposit),
            ],
        )
        .await
    }

    pub async fn get_fast_bridge_info(&self) -> Result<serde_json::Value> {
        self.get("/api/v1/fastbridge/info").await
    }

    pub async fn get_deposit_latest(&self, l1_address: &str) -> Result<serde_json::Value> {
        self.get_with_query("/api/v1/deposit/latest", &[("l1_address", l1_address)])
            .await
    }

    pub async fn get_deposit_networks(&self) -> Result<serde_json::Value> {
        self.get("/api/v1/deposit/networks").await
    }

    pub async fn fast_withdraw(
        &self,
        tx_info: &str,
        to_address: &str,
        auth: &str,
    ) -> Result<serde_json::Value> {
        self.post_form_with_auth(
            "/api/v1/fastwithdraw",
            &[("tx_info", tx_info), ("to_address", to_address)],
            auth,
        )
        .await
    }

    pub async fn get_fast_withdraw_info(
        &self,
        account_index: i64,
        auth: &str,
    ) -> Result<serde_json::Value> {
        let account_index = account_index.to_string();
        self.get_with_auth(
            "/api/v1/fastwithdraw/info",
            &[("account_index", account_index.as_str())],
            auth,
        )
        .await
    }

    pub async fn get_deposit_history(
        &self,
        account_index: i64,
        auth: &str,
        cursor: Option<&str>,
    ) -> Result<DepositHistory> {
        let account_index = account_index.to_string();
        let mut query: Vec<(&str, &str)> = vec![("account_index", account_index.as_str())];
        if let Some(c) = cursor {
            query.push(("cursor", c));
        }
        self.get_with_auth("/api/v1/deposit/history", &query, auth)
            .await
    }

    pub async fn get_withdraw_history(
        &self,
        account_index: i64,
        auth: &str,
        cursor: Option<&str>,
    ) -> Result<WithdrawHistory> {
        let account_index = account_index.to_string();
        let mut query: Vec<(&str, &str)> = vec![("account_index", account_index.as_str())];
        if let Some(c) = cursor {
            query.push(("cursor", c));
        }
        self.get_with_auth("/api/v1/withdraw/history", &query, auth)
            .await
    }

    pub async fn get_transfer_history(
        &self,
        account_index: i64,
        auth: &str,
        cursor: Option<&str>,
    ) -> Result<TransferHistory> {
        let account_index = account_index.to_string();
        let mut query: Vec<(&str, &str)> = vec![("account_index", account_index.as_str())];
        if let Some(c) = cursor {
            query.push(("cursor", c));
        }
        self.get_with_auth("/api/v1/transfer/history", &query, auth)
            .await
    }

    pub async fn get_lease_options(&self) -> Result<serde_json::Value> {
        self.get("/api/v1/leaseOptions").await
    }

    pub async fn get_leases(
        &self,
        account_index: i64,
        auth: &str,
        cursor: Option<&str>,
        limit: Option<i64>,
    ) -> Result<serde_json::Value> {
        let account_index = account_index.to_string();
        let limit = limit.map(|value| value.to_string());
        let mut query: Vec<(&str, &str)> = vec![("account_index", account_index.as_str())];
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor));
        }
        if let Some(ref limit) = limit {
            query.push(("limit", limit.as_str()));
        }
        self.get_with_auth("/api/v1/leases", &query, auth).await
    }

    pub async fn lit_lease(
        &self,
        tx_info: &str,
        lease_amount: Option<&str>,
        duration_days: Option<i64>,
        auth: &str,
    ) -> Result<serde_json::Value> {
        let duration_days = duration_days.map(|value| value.to_string());
        let mut form = vec![("tx_info", tx_info)];
        if let Some(lease_amount) = lease_amount {
            form.push(("lease_amount", lease_amount));
        }
        if let Some(ref duration_days) = duration_days {
            form.push(("duration_days", duration_days.as_str()));
        }
        self.post_form_with_auth("/api/v1/litLease", &form, auth)
            .await
    }
}
