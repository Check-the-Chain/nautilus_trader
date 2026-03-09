use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum OrderType {
    Limit = 0,
    Market = 1,
    StopLoss = 2,
    StopLossLimit = 3,
    TakeProfit = 4,
    TakeProfitLimit = 5,
    Twap = 6,
    TwapSub = 7,
    Liquidation = 8,
}

impl OrderType {
    /// Parse the wire-format string returned by the WS/REST API.
    pub fn from_wire_str(s: &str) -> Option<Self> {
        match s {
            "limit" => Some(Self::Limit),
            "market" => Some(Self::Market),
            "stop-loss" => Some(Self::StopLoss),
            "stop-loss-limit" => Some(Self::StopLossLimit),
            "take-profit" => Some(Self::TakeProfit),
            "take-profit-limit" => Some(Self::TakeProfitLimit),
            "twap" => Some(Self::Twap),
            "twap-sub" => Some(Self::TwapSub),
            "liquidation" => Some(Self::Liquidation),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum TimeInForce {
    ImmediateOrCancel = 0,
    GoodTillTime = 1,
    PostOnly = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum GroupingType {
    None = 0,
    OneTriggersTheOther = 1,
    OneCancelsTheOther = 2,
    OneTriggersAOneCancelsTheOther = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum CancelAllTimeInForce {
    Immediate = 0,
    Scheduled = 1,
    AbortScheduled = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum MarginMode {
    Cross = 0,
    Isolated = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum RouteType {
    Perps = 0,
    Spot = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum MarginDirection {
    Remove = 0,
    Add = 1,
}
