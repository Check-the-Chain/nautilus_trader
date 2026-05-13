use crate::error::Result;
use crate::models::candle::Candles;
use crate::rest::client::LighterRestClient;

impl LighterRestClient {
    pub async fn get_candles(
        &self,
        market_id: i64,
        granularity: &str,
        cursor: Option<&str>,
    ) -> Result<Candles> {
        let market_id = market_id.to_string();
        let mut query: Vec<(&str, &str)> = vec![
            ("market_id", market_id.as_str()),
            ("granularity", granularity),
        ];
        if let Some(c) = cursor {
            query.push(("cursor", c));
        }
        self.get_with_query("/api/v1/candles", &query).await
    }
}
