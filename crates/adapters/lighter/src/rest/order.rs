use crate::error::Result;
use crate::models::order::*;
use crate::models::order_book::*;
use crate::rest::client::LighterRestClient;

impl LighterRestClient {
    pub async fn get_order_books(&self) -> Result<OrderBooks> {
        self.get("/api/v1/orderBooks").await
    }

    pub async fn get_all_order_book_details(&self) -> Result<OrderBookDetails> {
        self.get("/api/v1/orderBookDetails").await
    }

    pub async fn get_order_book_details(&self, market_id: i64) -> Result<OrderBookDetails> {
        let market_id = market_id.to_string();
        self.get_with_query(
            "/api/v1/orderBookDetails",
            &[("market_id", market_id.as_str())],
        )
        .await
    }

    pub async fn get_order_book_orders(
        &self,
        market_id: i64,
        limit: u32,
    ) -> Result<OrderBookDepth> {
        let market_id = market_id.to_string();
        let limit = limit.to_string();
        self.get_with_query(
            "/api/v1/orderBookOrders",
            &[("market_id", market_id.as_str()), ("limit", limit.as_str())],
        )
        .await
    }

    pub async fn get_account_active_orders(
        &self,
        account_index: i64,
        market_id: i64,
        auth: &str,
    ) -> Result<Orders> {
        let account_index = account_index.to_string();
        let market_id = market_id.to_string();
        let mut query: Vec<(&str, &str)> = vec![("account_index", account_index.as_str())];
        if market_id != "255" {
            query.push(("market_id", market_id.as_str()));
        }
        self.get_with_auth("/api/v1/accountActiveOrders", &query, auth)
            .await
    }

    pub async fn get_account_inactive_orders(
        &self,
        account_index: i64,
        market_id: i64,
        auth: &str,
        cursor: Option<&str>,
    ) -> Result<Orders> {
        let account_index = account_index.to_string();
        let market_id = market_id.to_string();
        let limit = "100";
        let mut query: Vec<(&str, &str)> =
            vec![("account_index", account_index.as_str()), ("limit", limit)];
        if market_id != "255" {
            query.push(("market_id", market_id.as_str()));
        }
        if let Some(c) = cursor {
            query.push(("cursor", c));
        }
        self.get_with_auth("/api/v1/accountInactiveOrders", &query, auth)
            .await
    }

    pub async fn get_recent_trades(
        &self,
        market_id: i64,
        limit: u32,
    ) -> Result<crate::models::trade::Trades> {
        let market_id = market_id.to_string();
        let limit = limit.to_string();
        self.get_with_query(
            "/api/v1/recentTrades",
            &[("market_id", market_id.as_str()), ("limit", limit.as_str())],
        )
        .await
    }

    pub async fn get_trades(
        &self,
        market_id: i64,
        cursor: Option<&str>,
    ) -> Result<crate::models::trade::Trades> {
        let market_id = market_id.to_string();
        let mut query: Vec<(&str, &str)> = vec![("market_id", market_id.as_str())];
        if let Some(c) = cursor {
            query.push(("cursor", c));
        }
        self.get_with_query("/api/v1/trades", &query).await
    }

    /// Fetch account-specific trades (includes PnL fields).
    pub async fn get_account_trades(
        &self,
        account_index: i64,
        auth: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<crate::models::trade::Trades> {
        let account_index = account_index.to_string();
        let limit = limit.to_string();
        let mut query: Vec<(&str, &str)> = vec![
            ("account_index", account_index.as_str()),
            ("sort_by", "timestamp"),
            ("sort_dir", "desc"),
            ("limit", limit.as_str()),
        ];
        if let Some(c) = cursor {
            query.push(("cursor", c));
        }
        self.get_with_auth("/api/v1/trades", &query, auth).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_export(
        &self,
        export_type: &str,
        auth: Option<&str>,
        account_index: Option<i64>,
        market_id: Option<i64>,
        start_timestamp: Option<i64>,
        end_timestamp: Option<i64>,
        side: Option<&str>,
        role: Option<&str>,
        trade_type: Option<&str>,
    ) -> Result<serde_json::Value> {
        let account_index = account_index.map(|value| value.to_string());
        let market_id = market_id.map(|value| value.to_string());
        let start_timestamp = start_timestamp.map(|value| value.to_string());
        let end_timestamp = end_timestamp.map(|value| value.to_string());

        let mut query = vec![("type", export_type)];
        if let Some(ref account_index) = account_index {
            query.push(("account_index", account_index.as_str()));
        }
        if let Some(ref market_id) = market_id {
            query.push(("market_id", market_id.as_str()));
        }
        if let Some(ref start_timestamp) = start_timestamp {
            query.push(("start_timestamp", start_timestamp.as_str()));
        }
        if let Some(ref end_timestamp) = end_timestamp {
            query.push(("end_timestamp", end_timestamp.as_str()));
        }
        if let Some(side) = side {
            query.push(("side", side));
        }
        if let Some(role) = role {
            query.push(("role", role));
        }
        if let Some(trade_type) = trade_type {
            query.push(("trade_type", trade_type));
        }

        match auth {
            Some(auth) => self.get_with_auth("/api/v1/export", &query, auth).await,
            None => self.get_with_query("/api/v1/export", &query).await,
        }
    }
}
