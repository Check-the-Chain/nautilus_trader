use crate::error::Result;
use crate::rest::client::LighterRestClient;

impl LighterRestClient {
    pub async fn get_tokens(&self, account_index: i64, auth: &str) -> Result<serde_json::Value> {
        let account_index = account_index.to_string();
        self.get_with_auth(
            "/api/v1/tokens",
            &[("account_index", account_index.as_str())],
            auth,
        )
        .await
    }

    pub async fn create_token(
        &self,
        name: &str,
        account_index: i64,
        expiry: i64,
        sub_account_access: bool,
        scopes: &str,
        auth: &str,
    ) -> Result<serde_json::Value> {
        let account_index = account_index.to_string();
        let expiry = expiry.to_string();
        let sub_account_access = if sub_account_access { "true" } else { "false" };
        self.post_form_with_auth(
            "/api/v1/tokens/create",
            &[
                ("name", name),
                ("account_index", account_index.as_str()),
                ("expiry", expiry.as_str()),
                ("sub_account_access", sub_account_access),
                ("scopes", scopes),
            ],
            auth,
        )
        .await
    }

    pub async fn revoke_token(
        &self,
        token_id: i64,
        account_index: i64,
        auth: &str,
    ) -> Result<serde_json::Value> {
        let token_id = token_id.to_string();
        let account_index = account_index.to_string();
        self.post_form_with_auth(
            "/api/v1/tokens/revoke",
            &[
                ("token_id", token_id.as_str()),
                ("account_index", account_index.as_str()),
            ],
            auth,
        )
        .await
    }
}
