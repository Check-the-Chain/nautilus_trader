use crate::error::Result;
use crate::models::announcement::Announcements;
use crate::rest::client::LighterRestClient;

impl LighterRestClient {
    pub async fn get_announcements(&self) -> Result<Announcements> {
        self.get("/api/v1/announcement").await
    }

    pub async fn get_exchange_metrics(
        &self,
        period: &str,
        kind: &str,
        filter: Option<&str>,
        value: Option<&str>,
    ) -> Result<serde_json::Value> {
        let mut query = vec![("period", period), ("kind", kind)];
        if let Some(filter) = filter {
            query.push(("filter", filter));
        }
        if let Some(value) = value {
            query.push(("value", value));
        }
        self.get_with_query("/api/v1/exchangeMetrics", &query).await
    }

    pub async fn get_execute_stats(&self, period: &str) -> Result<serde_json::Value> {
        self.get_with_query("/api/v1/executeStats", &[("period", period)])
            .await
    }
}
