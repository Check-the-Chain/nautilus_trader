use crate::error::Result;
use crate::rest::client::LighterRestClient;

impl LighterRestClient {
    pub async fn ack_notification(
        &self,
        notif_id: &str,
        account_index: i64,
        auth: &str,
    ) -> Result<serde_json::Value> {
        let account_index = account_index.to_string();
        self.post_form_with_auth(
            "/api/v1/notification/ack",
            &[
                ("notif_id", notif_id),
                ("account_index", account_index.as_str()),
            ],
            auth,
        )
        .await
    }
}
