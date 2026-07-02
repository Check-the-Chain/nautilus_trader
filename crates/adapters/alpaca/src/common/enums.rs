// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Alpaca venue enums mirrored from REST and WebSocket payloads.

use nautilus_model::enums::{OrderSide, OrderStatus, OrderType, TimeInForce};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumIter, EnumString};

/// Alpaca API environment.
///
/// Defaults to [`AlpacaEnvironment::Paper`] so that accidental live order flow
/// requires an explicit opt-in.
#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[strum(ascii_case_insensitive, serialize_all = "lowercase")]
pub enum AlpacaEnvironment {
    /// Live trading environment.
    Live,
    /// Paper trading environment.
    #[default]
    Paper,
}

/// Alpaca real-time equities market data feed.
#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[strum(ascii_case_insensitive, serialize_all = "snake_case")]
pub enum AlpacaDataFeed {
    /// IEX exchange feed (free tier).
    #[default]
    Iex,
    /// Consolidated SIP feed (paid subscription).
    Sip,
    /// 15-minute delayed SIP feed.
    DelayedSip,
    /// Test feed streaming fake data (available outside market hours).
    Test,
}

/// Alpaca asset class.
#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[strum(ascii_case_insensitive, serialize_all = "snake_case")]
pub enum AlpacaAssetClass {
    /// US equities (stocks and ETFs).
    #[default]
    UsEquity,
    /// Cryptocurrencies.
    Crypto,
    /// US equity options.
    UsOption,
    /// IPO subscriptions.
    Ipo,
}

/// Alpaca asset status.
#[derive(
    Copy,
    Clone,
    Debug,
    Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[strum(ascii_case_insensitive, serialize_all = "lowercase")]
pub enum AlpacaAssetStatus {
    /// Asset is active and available.
    Active,
    /// Asset is inactive.
    Inactive,
}

/// Alpaca order side.
#[derive(
    Copy,
    Clone,
    Debug,
    Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[strum(ascii_case_insensitive, serialize_all = "lowercase")]
pub enum AlpacaOrderSide {
    /// Buy order.
    Buy,
    /// Sell order.
    Sell,
}

impl From<AlpacaOrderSide> for OrderSide {
    fn from(side: AlpacaOrderSide) -> Self {
        match side {
            AlpacaOrderSide::Buy => Self::Buy,
            AlpacaOrderSide::Sell => Self::Sell,
        }
    }
}

impl TryFrom<OrderSide> for AlpacaOrderSide {
    type Error = anyhow::Error;

    fn try_from(side: OrderSide) -> Result<Self, Self::Error> {
        match side {
            OrderSide::Buy => Ok(Self::Buy),
            OrderSide::Sell => Ok(Self::Sell),
            _ => anyhow::bail!("invalid order side for Alpaca: {side}"),
        }
    }
}

/// Alpaca order type.
#[derive(
    Copy,
    Clone,
    Debug,
    Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[strum(ascii_case_insensitive, serialize_all = "snake_case")]
pub enum AlpacaOrderType {
    /// Market order.
    Market,
    /// Limit order.
    Limit,
    /// Stop (market) order.
    Stop,
    /// Stop-limit order.
    StopLimit,
    /// Trailing-stop order.
    TrailingStop,
}

impl From<AlpacaOrderType> for OrderType {
    fn from(order_type: AlpacaOrderType) -> Self {
        match order_type {
            AlpacaOrderType::Market => Self::Market,
            AlpacaOrderType::Limit => Self::Limit,
            AlpacaOrderType::Stop => Self::StopMarket,
            AlpacaOrderType::StopLimit => Self::StopLimit,
            AlpacaOrderType::TrailingStop => Self::TrailingStopMarket,
        }
    }
}

impl TryFrom<OrderType> for AlpacaOrderType {
    type Error = anyhow::Error;

    fn try_from(order_type: OrderType) -> Result<Self, Self::Error> {
        match order_type {
            OrderType::Market => Ok(Self::Market),
            OrderType::Limit => Ok(Self::Limit),
            OrderType::StopMarket => Ok(Self::Stop),
            OrderType::StopLimit => Ok(Self::StopLimit),
            OrderType::TrailingStopMarket => Ok(Self::TrailingStop),
            _ => anyhow::bail!("unsupported order type for Alpaca: {order_type}"),
        }
    }
}

/// Alpaca order time in force.
#[derive(
    Copy,
    Clone,
    Debug,
    Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[strum(ascii_case_insensitive, serialize_all = "lowercase")]
pub enum AlpacaTimeInForce {
    /// Valid for the trading day.
    Day,
    /// Good till canceled.
    Gtc,
    /// Executes in the opening auction only.
    Opg,
    /// Executes in the closing auction only.
    Cls,
    /// Immediate or cancel.
    Ioc,
    /// Fill or kill.
    Fok,
}

impl From<AlpacaTimeInForce> for TimeInForce {
    fn from(tif: AlpacaTimeInForce) -> Self {
        match tif {
            AlpacaTimeInForce::Day => Self::Day,
            AlpacaTimeInForce::Gtc => Self::Gtc,
            AlpacaTimeInForce::Opg | AlpacaTimeInForce::Cls => Self::AtTheOpen,
            AlpacaTimeInForce::Ioc => Self::Ioc,
            AlpacaTimeInForce::Fok => Self::Fok,
        }
    }
}

impl TryFrom<TimeInForce> for AlpacaTimeInForce {
    type Error = anyhow::Error;

    fn try_from(tif: TimeInForce) -> Result<Self, Self::Error> {
        match tif {
            TimeInForce::Day => Ok(Self::Day),
            TimeInForce::Gtc => Ok(Self::Gtc),
            TimeInForce::AtTheOpen => Ok(Self::Opg),
            TimeInForce::AtTheClose => Ok(Self::Cls),
            TimeInForce::Ioc => Ok(Self::Ioc),
            TimeInForce::Fok => Ok(Self::Fok),
            _ => anyhow::bail!("unsupported time in force for Alpaca: {tif}"),
        }
    }
}

/// Alpaca order status.
#[derive(
    Copy,
    Clone,
    Debug,
    Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[strum(ascii_case_insensitive, serialize_all = "snake_case")]
pub enum AlpacaOrderStatus {
    /// Order received and routed.
    New,
    /// Order partially filled.
    PartiallyFilled,
    /// Order completely filled.
    Filled,
    /// Order done executing for the day.
    DoneForDay,
    /// Order canceled.
    Canceled,
    /// Order expired.
    Expired,
    /// Order replaced by an updated order.
    Replaced,
    /// Cancel request pending.
    PendingCancel,
    /// Replace request pending.
    PendingReplace,
    /// Order received but not yet routed.
    Accepted,
    /// Order received and being evaluated.
    PendingNew,
    /// Order received and evaluated for bidding (rarely sent).
    AcceptedForBidding,
    /// Order stopped, trade guaranteed but not yet executed.
    Stopped,
    /// Order rejected.
    Rejected,
    /// Order suspended.
    Suspended,
    /// Order completed for the day, settlement calculations pending.
    Calculated,
    /// Order held pending a trigger condition (e.g. leg of a held order class).
    Held,
}

impl From<AlpacaOrderStatus> for OrderStatus {
    fn from(status: AlpacaOrderStatus) -> Self {
        match status {
            AlpacaOrderStatus::PendingNew => Self::Submitted,
            AlpacaOrderStatus::New
            | AlpacaOrderStatus::Accepted
            | AlpacaOrderStatus::AcceptedForBidding
            | AlpacaOrderStatus::Stopped
            | AlpacaOrderStatus::Replaced
            | AlpacaOrderStatus::DoneForDay
            | AlpacaOrderStatus::Calculated => Self::Accepted,
            AlpacaOrderStatus::PartiallyFilled => Self::PartiallyFilled,
            AlpacaOrderStatus::Filled => Self::Filled,
            AlpacaOrderStatus::Canceled | AlpacaOrderStatus::Suspended => Self::Canceled,
            AlpacaOrderStatus::Expired => Self::Expired,
            AlpacaOrderStatus::PendingCancel => Self::PendingCancel,
            AlpacaOrderStatus::PendingReplace => Self::PendingUpdate,
            AlpacaOrderStatus::Rejected => Self::Rejected,
            AlpacaOrderStatus::Held => Self::Emulated,
        }
    }
}

/// Alpaca order class.
#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[strum(ascii_case_insensitive, serialize_all = "lowercase")]
pub enum AlpacaOrderClass {
    /// Simple single-leg order (serialized as an empty string by some endpoints).
    #[default]
    #[serde(alias = "")]
    Simple,
    /// Bracket order (entry + take-profit + stop-loss).
    Bracket,
    /// One-cancels-other order.
    Oco,
    /// One-triggers-other order.
    Oto,
    /// Multi-leg options order.
    Mleg,
}

/// Alpaca position side.
#[derive(
    Copy,
    Clone,
    Debug,
    Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[strum(ascii_case_insensitive, serialize_all = "lowercase")]
pub enum AlpacaPositionSide {
    /// Long position.
    Long,
    /// Short position.
    Short,
}

/// Alpaca trade-updates stream event type.
#[derive(
    Copy,
    Clone,
    Debug,
    Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[strum(ascii_case_insensitive, serialize_all = "snake_case")]
pub enum AlpacaTradeUpdateEvent {
    /// Order accepted by the venue.
    New,
    /// Order completely filled.
    Fill,
    /// Order partially filled.
    PartialFill,
    /// Order canceled.
    Canceled,
    /// Order expired.
    Expired,
    /// Order done executing for the day.
    DoneForDay,
    /// Order replaced.
    Replaced,
    /// Order rejected.
    Rejected,
    /// Order received and being evaluated.
    PendingNew,
    /// Order stopped.
    Stopped,
    /// Cancel request pending.
    PendingCancel,
    /// Replace request pending.
    PendingReplace,
    /// Settlement calculations pending.
    Calculated,
    /// Order suspended.
    Suspended,
    /// Replace request rejected.
    OrderReplaceRejected,
    /// Cancel request rejected.
    OrderCancelRejected,
    /// Order held.
    Held,
    /// Order accepted (not yet routed).
    Accepted,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(AlpacaAssetClass::UsEquity, "\"us_equity\"")]
    #[case(AlpacaAssetClass::Crypto, "\"crypto\"")]
    #[case(AlpacaAssetClass::UsOption, "\"us_option\"")]
    fn test_asset_class_serde_roundtrip(#[case] value: AlpacaAssetClass, #[case] json: &str) {
        assert_eq!(serde_json::to_string(&value).unwrap(), json);
        assert_eq!(
            serde_json::from_str::<AlpacaAssetClass>(json).unwrap(),
            value
        );
    }

    #[rstest]
    #[case(AlpacaOrderType::Market, "\"market\"")]
    #[case(AlpacaOrderType::StopLimit, "\"stop_limit\"")]
    #[case(AlpacaOrderType::TrailingStop, "\"trailing_stop\"")]
    fn test_order_type_serde_roundtrip(#[case] value: AlpacaOrderType, #[case] json: &str) {
        assert_eq!(serde_json::to_string(&value).unwrap(), json);
        assert_eq!(
            serde_json::from_str::<AlpacaOrderType>(json).unwrap(),
            value
        );
    }

    #[rstest]
    #[case(AlpacaOrderStatus::PartiallyFilled, "\"partially_filled\"")]
    #[case(AlpacaOrderStatus::AcceptedForBidding, "\"accepted_for_bidding\"")]
    #[case(AlpacaOrderStatus::PendingCancel, "\"pending_cancel\"")]
    fn test_order_status_serde_roundtrip(#[case] value: AlpacaOrderStatus, #[case] json: &str) {
        assert_eq!(serde_json::to_string(&value).unwrap(), json);
        assert_eq!(
            serde_json::from_str::<AlpacaOrderStatus>(json).unwrap(),
            value
        );
    }

    #[rstest]
    fn test_order_class_empty_string_is_simple() {
        assert_eq!(
            serde_json::from_str::<AlpacaOrderClass>("\"\"").unwrap(),
            AlpacaOrderClass::Simple
        );
    }

    #[rstest]
    #[case(AlpacaOrderType::Market, OrderType::Market)]
    #[case(AlpacaOrderType::Stop, OrderType::StopMarket)]
    #[case(AlpacaOrderType::TrailingStop, OrderType::TrailingStopMarket)]
    fn test_order_type_to_nautilus(#[case] value: AlpacaOrderType, #[case] expected: OrderType) {
        assert_eq!(OrderType::from(value), expected);
    }

    #[rstest]
    #[case(OrderType::Market, AlpacaOrderType::Market)]
    #[case(OrderType::StopMarket, AlpacaOrderType::Stop)]
    fn test_order_type_from_nautilus(#[case] value: OrderType, #[case] expected: AlpacaOrderType) {
        assert_eq!(AlpacaOrderType::try_from(value).unwrap(), expected);
    }

    #[rstest]
    fn test_order_type_from_nautilus_unsupported() {
        assert!(AlpacaOrderType::try_from(OrderType::MarketToLimit).is_err());
    }

    #[rstest]
    #[case(AlpacaOrderStatus::New, OrderStatus::Accepted)]
    #[case(AlpacaOrderStatus::PendingNew, OrderStatus::Submitted)]
    #[case(AlpacaOrderStatus::Filled, OrderStatus::Filled)]
    #[case(AlpacaOrderStatus::Rejected, OrderStatus::Rejected)]
    fn test_order_status_to_nautilus(
        #[case] value: AlpacaOrderStatus,
        #[case] expected: OrderStatus,
    ) {
        assert_eq!(OrderStatus::from(value), expected);
    }
}
