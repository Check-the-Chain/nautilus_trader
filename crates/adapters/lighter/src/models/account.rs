use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::asset::Asset;
use super::de::{
    opt_f64_from_string_or_number, opt_i64_from_string_or_number, opt_string_from_string_or_number,
};

// ---------------------------------------------------------------------------
// /api/v1/account  (no auth)
// ---------------------------------------------------------------------------

/// Top-level response from `/api/v1/account?by=...&value=...`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Accounts {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub accounts: Vec<Account>,
}

/// A single account entry inside the `accounts` array.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub account_index: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub account_type: Option<i64>,
    #[serde(default)]
    pub l1_address: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub total_order_count: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub total_isolated_order_count: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub pending_order_count: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub available_balance: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub status: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub collateral: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub cancel_all_time: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub transaction_time: Option<i64>,
    #[serde(default)]
    pub assets: Option<Vec<Asset>>,
}

// ---------------------------------------------------------------------------
// /api/v1/account  (with auth — includes positions)
// ---------------------------------------------------------------------------

/// Top-level response from `/api/v1/account?by=...&value=...` with auth header.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailedAccounts {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub accounts: Vec<DetailedAccount>,
}

/// A single account entry with position details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailedAccount {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub account_index: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub account_type: Option<i64>,
    #[serde(default)]
    pub l1_address: Option<String>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub available_balance: Option<f64>,
    #[serde(default)]
    pub positions: Option<Vec<AccountPosition>>,
    #[serde(default)]
    pub assets: Option<Vec<Asset>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountPosition {
    pub market_id: i64,
    pub symbol: String,
    pub initial_margin_fraction: String,
    pub open_order_count: i64,
    pub pending_order_count: i64,
    pub position_tied_order_count: i64,
    pub sign: i64,
    pub position: String,
    pub avg_entry_price: String,
    pub position_value: String,
    pub unrealized_pnl: String,
    pub realized_pnl: String,
    pub liquidation_price: String,
    #[serde(default)]
    pub total_funding_paid_out: Option<String>,
    pub margin_mode: i64,
    pub allocated_margin: String,
}

impl AccountPosition {
    pub fn position_f64(&self) -> Option<f64> {
        self.position.parse::<f64>().ok()
    }

    pub fn signed_position(&self) -> f64 {
        let qty = self.position_f64().unwrap_or(0.0);
        if qty == 0.0 {
            return 0.0;
        }

        if self.sign < 0 {
            -qty.abs()
        } else if self.sign > 0 {
            qty.abs()
        } else {
            qty
        }
    }

    pub fn side_label(&self) -> &'static str {
        if self.signed_position() < 0.0 {
            "short"
        } else if self.signed_position() > 0.0 {
            "long"
        } else {
            "flat"
        }
    }
}

// ---------------------------------------------------------------------------
// Other account-related responses
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountApiKeys {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub api_keys: Vec<AccountApiKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountApiKey {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub api_key_index: Option<i64>,
    #[serde(default)]
    pub public_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubAccounts {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub l1_address: Option<String>,
    #[serde(default)]
    pub sub_accounts: Vec<Account>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountLimits {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub max_llp_percentage: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub max_llp_amount: Option<f64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub user_tier: Option<i64>,
    #[serde(default)]
    pub can_create_public_pool: Option<bool>,
    #[serde(default, deserialize_with = "opt_string_from_string_or_number")]
    pub user_tier_name: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub current_maker_fee_tick: Option<i64>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub current_taker_fee_tick: Option<i64>,
    #[serde(default, deserialize_with = "opt_string_from_string_or_number")]
    pub leased_lit: Option<String>,
    #[serde(default, deserialize_with = "opt_string_from_string_or_number")]
    pub effective_lit_stakes: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountPnl {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub resolution: Option<String>,
    #[serde(default)]
    pub pnl: Vec<PnlEntry>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnlEntry {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub timestamp: Option<i64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub trade_pnl: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub trade_spot_pnl: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub inflow: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub outflow: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub spot_outflow: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub spot_inflow: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub pool_pnl: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub pool_inflow: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub pool_outflow: Option<f64>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub pool_total_shares: Option<f64>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountMetadatas {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub account_metadatas: Vec<AccountMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountMetadata {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub account_index: Option<i64>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub can_invite: Option<bool>,
    #[serde(default, deserialize_with = "opt_f64_from_string_or_number")]
    pub referral_points_percentage: Option<f64>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeAccountTierResponse {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Liquidations {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub liquidations: Vec<Liquidation>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Liquidation {
    pub id: i64,
    pub market_id: i64,
    #[serde(rename = "type")]
    pub kind: String,
    pub trade: LiquidationTrade,
    pub info: LiquidationInfo,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub executed_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidationTrade {
    pub price: String,
    pub size: String,
    pub taker_fee: String,
    pub maker_fee: String,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub transaction_time: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidationInfo {
    #[serde(default)]
    pub positions: Vec<AccountPosition>,
    pub risk_info_before: RiskInfo,
    pub risk_info_after: RiskInfo,
    #[serde(default)]
    pub mark_prices: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskInfo {
    pub cross_risk_parameters: RiskParameters,
    #[serde(default)]
    pub isolated_risk_parameters: Vec<RiskParameters>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskParameters {
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub market_id: Option<i64>,
    pub collateral: String,
    pub total_account_value: String,
    pub initial_margin_req: String,
    pub maintenance_margin_req: String,
    pub close_out_margin_req: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NextNonce {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default, deserialize_with = "opt_i64_from_string_or_number")]
    pub nonce: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::{AccountLimits, AccountPosition};

    #[test]
    fn parses_account_limits_fee_tier_fields() {
        let raw = serde_json::json!({
            "code": 200,
            "max_llp_percentage": 10,
            "max_llp_amount": "1000.0",
            "user_tier": "2",
            "can_create_public_pool": false,
            "user_tier_name": "premium",
            "current_maker_fee_tick": 40,
            "current_taker_fee_tick": "280",
            "leased_lit": "0",
            "effective_lit_stakes": "1000"
        });

        let limits: AccountLimits = serde_json::from_value(raw).unwrap();
        assert_eq!(limits.user_tier, Some(2));
        assert_eq!(limits.user_tier_name.as_deref(), Some("premium"));
        assert_eq!(limits.current_maker_fee_tick, Some(40));
        assert_eq!(limits.current_taker_fee_tick, Some(280));
        assert_eq!(limits.leased_lit.as_deref(), Some("0"));
        assert_eq!(limits.effective_lit_stakes.as_deref(), Some("1000"));
    }

    #[test]
    fn parses_position_fields() {
        let raw = serde_json::json!({
            "market_id": 93,
            "symbol": "XAG",
            "initial_margin_fraction": "100",
            "open_order_count": 0,
            "pending_order_count": 0,
            "position_tied_order_count": 0,
            "sign": 1,
            "position": "0.53",
            "avg_entry_price": "76.8259",
            "position_value": "40.650311",
            "unrealized_pnl": "-0.067422",
            "realized_pnl": "0.000000",
            "liquidation_price": "70.0",
            "total_funding_paid_out": "0.0",
            "margin_mode": 1,
            "allocated_margin": "40.717733"
        });

        let pos: AccountPosition = serde_json::from_value(raw).unwrap();
        assert_eq!(pos.market_id, 93);
        assert_eq!(pos.symbol, "XAG");
        assert_eq!(pos.position, "0.53");
        assert_eq!(pos.sign, 1);
        assert_eq!(pos.avg_entry_price, "76.8259");
        assert_eq!(pos.position_value, "40.650311");
        assert_eq!(pos.unrealized_pnl, "-0.067422");
        assert_eq!(pos.realized_pnl, "0.000000");
        assert_eq!(pos.allocated_margin, "40.717733");
        assert_eq!(pos.signed_position(), 0.53);
        assert_eq!(pos.side_label(), "long");
    }

    #[test]
    fn parses_short_position_sign() {
        let raw = serde_json::json!({
            "market_id": 93,
            "symbol": "XAG",
            "initial_margin_fraction": "100",
            "open_order_count": 0,
            "pending_order_count": 0,
            "position_tied_order_count": 0,
            "position": "0.40",
            "avg_entry_price": "75.0",
            "position_value": "30.0",
            "unrealized_pnl": "0.2",
            "realized_pnl": "0.0",
            "liquidation_price": "80.0",
            "total_funding_paid_out": "0.0",
            "margin_mode": 1,
            "allocated_margin": "40.0",
            "sign": -1
        });

        let pos: AccountPosition = serde_json::from_value(raw).unwrap();
        assert_eq!(pos.market_id, 93);
        assert_eq!(pos.position, "0.40");
        assert_eq!(pos.avg_entry_price, "75.0");
        assert_eq!(pos.initial_margin_fraction, "100");
        assert_eq!(pos.margin_mode, 1);
        assert_eq!(pos.sign, -1);
        assert_eq!(pos.signed_position(), -0.40);
        assert_eq!(pos.side_label(), "short");
    }

    #[test]
    fn parses_avg_entry_price_field() {
        let raw = serde_json::json!({
            "market_id": 93,
            "symbol": "XAG",
            "initial_margin_fraction": "100",
            "open_order_count": 0,
            "pending_order_count": 0,
            "position_tied_order_count": 0,
            "sign": 1,
            "position": "0.40",
            "avg_entry_price": "75.0",
            "position_value": "30.0",
            "unrealized_pnl": "0.2",
            "realized_pnl": "0.0",
            "liquidation_price": "80.0",
            "total_funding_paid_out": "0.0",
            "margin_mode": 1,
            "allocated_margin": "40.0"
        });

        let pos: AccountPosition = serde_json::from_value(raw).unwrap();
        assert_eq!(pos.avg_entry_price, "75.0");
    }
}
