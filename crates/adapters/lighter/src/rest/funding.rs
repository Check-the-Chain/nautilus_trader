use crate::error::Result;
use crate::models::funding::*;
use crate::rest::client::LighterRestClient;

impl LighterRestClient {
    pub async fn get_funding_rates(
        &self,
        market_id: i64,
        cursor: Option<&str>,
    ) -> Result<FundingRates> {
        let market_id = market_id.to_string();
        let mut query: Vec<(&str, &str)> = vec![("market_id", market_id.as_str())];
        if let Some(c) = cursor {
            query.push(("cursor", c));
        }
        self.get_with_query("/api/v1/funding-rates", &query).await
    }

    pub async fn get_position_fundings(
        &self,
        account_index: i64,
        auth: &str,
    ) -> Result<PositionFundings> {
        let account_index = account_index.to_string();
        self.get_with_auth(
            "/api/v1/positionFunding",
            &[("account_index", account_index.as_str())],
            auth,
        )
        .await
    }
}
