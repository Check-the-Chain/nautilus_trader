use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderStatus {
    InProgress,
    Pending,
    New,
    Open,
    PartiallyFilled,
    Filled,
    Rejected,
    Canceled,
    CanceledPostOnly,
    CanceledReduceOnly,
    CanceledPositionNotAllowed,
    CanceledMarginNotAllowed,
    CanceledTooMuchSlippage,
    CanceledNotEnoughLiquidity,
    CanceledSelfTrade,
    CanceledExpired,
    CanceledOco,
    CanceledChild,
    CanceledLiquidation,
    CanceledInvalidBalance,
    Expired,
    Unknown(String),
}

impl OrderStatus {
    pub fn parse(raw: &str) -> Self {
        match raw {
            "in-progress" => Self::InProgress,
            "pending" => Self::Pending,
            "new" => Self::New,
            "open" => Self::Open,
            "partially_filled" | "partially-filled" => Self::PartiallyFilled,
            "filled" => Self::Filled,
            "rejected" => Self::Rejected,
            "canceled" | "cancelled" => Self::Canceled,
            "canceled-post-only" => Self::CanceledPostOnly,
            "canceled-reduce-only" => Self::CanceledReduceOnly,
            "canceled-position-not-allowed" => Self::CanceledPositionNotAllowed,
            "canceled-margin-not-allowed" => Self::CanceledMarginNotAllowed,
            "canceled-too-much-slippage" => Self::CanceledTooMuchSlippage,
            "canceled-not-enough-liquidity" => Self::CanceledNotEnoughLiquidity,
            "canceled-self-trade" => Self::CanceledSelfTrade,
            "canceled-expired" => Self::CanceledExpired,
            "canceled-oco" => Self::CanceledOco,
            "canceled-child" => Self::CanceledChild,
            "canceled-liquidation" => Self::CanceledLiquidation,
            "canceled-invalid-balance" => Self::CanceledInvalidBalance,
            "expired" => Self::Expired,
            other => Self::Unknown(other.to_string()),
        }
    }

    pub fn is_open_like(&self) -> bool {
        matches!(self, Self::Open | Self::New | Self::PartiallyFilled)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Filled
                | Self::Rejected
                | Self::Canceled
                | Self::CanceledPostOnly
                | Self::CanceledReduceOnly
                | Self::CanceledPositionNotAllowed
                | Self::CanceledMarginNotAllowed
                | Self::CanceledTooMuchSlippage
                | Self::CanceledNotEnoughLiquidity
                | Self::CanceledSelfTrade
                | Self::CanceledExpired
                | Self::CanceledOco
                | Self::CanceledChild
                | Self::CanceledLiquidation
                | Self::CanceledInvalidBalance
                | Self::Expired
        )
    }
}

impl fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InProgress => write!(f, "in-progress"),
            Self::Pending => write!(f, "pending"),
            Self::New => write!(f, "new"),
            Self::Open => write!(f, "open"),
            Self::PartiallyFilled => write!(f, "partially_filled"),
            Self::Filled => write!(f, "filled"),
            Self::Rejected => write!(f, "rejected"),
            Self::Canceled => write!(f, "canceled"),
            Self::CanceledPostOnly => write!(f, "canceled-post-only"),
            Self::CanceledReduceOnly => write!(f, "canceled-reduce-only"),
            Self::CanceledPositionNotAllowed => {
                write!(f, "canceled-position-not-allowed")
            }
            Self::CanceledMarginNotAllowed => {
                write!(f, "canceled-margin-not-allowed")
            }
            Self::CanceledTooMuchSlippage => {
                write!(f, "canceled-too-much-slippage")
            }
            Self::CanceledNotEnoughLiquidity => {
                write!(f, "canceled-not-enough-liquidity")
            }
            Self::CanceledSelfTrade => write!(f, "canceled-self-trade"),
            Self::CanceledExpired => write!(f, "canceled-expired"),
            Self::CanceledOco => write!(f, "canceled-oco"),
            Self::CanceledChild => write!(f, "canceled-child"),
            Self::CanceledLiquidation => write!(f, "canceled-liquidation"),
            Self::CanceledInvalidBalance => {
                write!(f, "canceled-invalid-balance")
            }
            Self::Expired => write!(f, "expired"),
            Self::Unknown(raw) => write!(f, "{raw}"),
        }
    }
}

/// Order payload used across both REST and WebSocket SDK models.
///
/// Fields that may be absent in WebSocket messages are marked with
/// `#[serde(default)]` so that a missing field does not cause the entire
/// parent message (e.g. `WsAccountAllOrdersUpdate`) to fail deserialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_index: i64,
    #[serde(default)]
    pub client_order_index: i64,
    #[serde(default)]
    pub order_id: String,
    #[serde(default)]
    pub client_order_id: String,
    pub market_index: i64,
    #[serde(default)]
    pub owner_account_index: i64,
    pub initial_base_amount: String,
    pub price: String,
    #[serde(default)]
    pub nonce: i64,
    pub remaining_base_amount: String,
    pub is_ask: bool,
    #[serde(default)]
    pub base_size: i64,
    #[serde(default)]
    pub base_price: i64,
    pub filled_base_amount: String,
    #[serde(default)]
    pub filled_quote_amount: String,
    pub side: String,
    #[serde(rename = "type")]
    pub order_type: String,
    pub time_in_force: String,
    pub reduce_only: bool,
    #[serde(default)]
    pub trigger_price: String,
    #[serde(default)]
    pub order_expiry: i64,
    pub status: String,
    #[serde(default)]
    pub trigger_status: String,
    #[serde(default)]
    pub trigger_time: i64,
    #[serde(default)]
    pub parent_order_index: i64,
    #[serde(default)]
    pub parent_order_id: String,
    #[serde(default)]
    pub to_trigger_order_id_0: String,
    #[serde(default)]
    pub to_trigger_order_id_1: String,
    #[serde(default)]
    pub to_cancel_order_id_0: String,
    #[serde(default)]
    pub block_height: i64,
    pub timestamp: i64,
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
    #[serde(default)]
    pub transaction_time: i64,
}

impl Order {
    pub fn status_typed(&self) -> OrderStatus {
        OrderStatus::parse(self.status.as_str())
    }

    pub fn price_f64(&self) -> Option<f64> {
        self.price.parse::<f64>().ok()
    }

    pub fn remaining_base_amount_f64(&self) -> Option<f64> {
        self.remaining_base_amount.parse::<f64>().ok()
    }

    pub fn filled_base_amount_f64(&self) -> Option<f64> {
        self.filled_base_amount.parse::<f64>().ok()
    }

    pub fn filled_quote_amount_f64(&self) -> Option<f64> {
        self.filled_quote_amount.parse::<f64>().ok()
    }

    pub fn initial_base_amount_f64(&self) -> Option<f64> {
        self.initial_base_amount.parse::<f64>().ok()
    }

    pub fn trigger_price_f64(&self) -> Option<f64> {
        self.trigger_price.parse::<f64>().ok()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Orders {
    pub code: i64,
    #[serde(default)]
    pub message: Option<String>,
    pub orders: Vec<Order>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleOrder {
    pub price: String,
    pub remaining_base_amount: String,
}

impl SimpleOrder {
    pub fn price_f64(&self) -> Option<f64> {
        self.price.parse::<f64>().ok()
    }

    pub fn remaining_base_amount_f64(&self) -> Option<f64> {
        self.remaining_base_amount.parse::<f64>().ok()
    }
}
