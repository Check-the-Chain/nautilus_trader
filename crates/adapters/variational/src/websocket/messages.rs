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

//! Variational public WebSocket request and response shapes.

use serde::{Deserialize, Serialize};

use crate::{
    common::VARIATIONAL_QUOTE_CURRENCY, config::VARIATIONAL_WS_PRICE_FUNDING_INTERVAL_SECS,
};

pub const VARIATIONAL_WS_PERPETUAL_FUTURE: &str = "perpetual_future";
pub const VARIATIONAL_WS_PRICE_CHANNEL_PREFIX: &str = "instrument_price:";
pub const VARIATIONAL_WS_HEARTBEAT_TYPE: &str = "heartbeat";

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VariationalWsAction {
    Subscribe,
    Unsubscribe,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct VariationalWsInstrument {
    pub underlying: String,
    pub instrument_type: &'static str,
    pub settlement_asset: &'static str,
    pub funding_interval_s: u64,
}

impl VariationalWsInstrument {
    #[must_use]
    pub fn perpetual(ticker: impl Into<String>, funding_interval_s: u64) -> Self {
        Self {
            underlying: ticker.into(),
            instrument_type: VARIATIONAL_WS_PERPETUAL_FUTURE,
            settlement_asset: VARIATIONAL_QUOTE_CURRENCY,
            funding_interval_s,
        }
    }

    #[must_use]
    pub fn price_channel(&self) -> String {
        format!(
            "P-{}-{}-{}",
            self.underlying, self.settlement_asset, self.funding_interval_s
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct VariationalWsSubscriptionRequest {
    pub action: VariationalWsAction,
    pub instruments: Vec<VariationalWsInstrument>,
}

impl VariationalWsSubscriptionRequest {
    #[must_use]
    pub fn subscribe_tickers(
        tickers: impl IntoIterator<Item = String>,
        funding_interval_s: u64,
    ) -> Self {
        Self {
            action: VariationalWsAction::Subscribe,
            instruments: tickers
                .into_iter()
                .map(|ticker| VariationalWsInstrument::perpetual(ticker, funding_interval_s))
                .collect(),
        }
    }

    #[must_use]
    pub fn unsubscribe_tickers(
        tickers: impl IntoIterator<Item = String>,
        funding_interval_s: u64,
    ) -> Self {
        Self {
            action: VariationalWsAction::Unsubscribe,
            instruments: tickers
                .into_iter()
                .map(|ticker| VariationalWsInstrument::perpetual(ticker, funding_interval_s))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum VariationalWsMessage {
    Heartbeat(VariationalWsHeartbeat),
    Price(Box<VariationalWsPriceMessage>),
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct VariationalWsHeartbeat {
    pub timestamp: String,
    #[serde(rename = "type")]
    pub message_type: String,
}

impl VariationalWsHeartbeat {
    #[must_use]
    pub fn is_heartbeat(&self) -> bool {
        self.message_type == VARIATIONAL_WS_HEARTBEAT_TYPE
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct VariationalWsPriceMessage {
    pub channel: String,
    pub pricing: VariationalWsPricing,
}

impl VariationalWsPriceMessage {
    #[must_use]
    pub fn ticker(&self) -> Option<&str> {
        ticker_from_price_channel(&self.channel)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct VariationalWsPricing {
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub native_price: Option<String>,
    #[serde(default)]
    pub delta: Option<String>,
    #[serde(default)]
    pub gamma: Option<String>,
    #[serde(default)]
    pub theta: Option<String>,
    #[serde(default)]
    pub vega: Option<String>,
    #[serde(default)]
    pub rho: Option<String>,
    #[serde(default)]
    pub iv: Option<String>,
    #[serde(default)]
    pub underlying_price: Option<String>,
    #[serde(default)]
    pub interest_rate: Option<String>,
    #[serde(default)]
    pub timestamp: Option<String>,
}

#[must_use]
pub fn ticker_from_price_channel(channel: &str) -> Option<&str> {
    let topic = channel.strip_prefix(VARIATIONAL_WS_PRICE_CHANNEL_PREFIX)?;
    let (prefix_and_ticker, _funding_interval) = topic.rsplit_once('-')?;
    let (prefix, settlement_asset) = prefix_and_ticker.rsplit_once('-')?;
    let (prefix, ticker) = prefix.rsplit_once('-')?;
    if !prefix.starts_with('P') || settlement_asset != VARIATIONAL_QUOTE_CURRENCY {
        return None;
    }

    Some(ticker)
}

#[must_use]
pub fn default_price_ws_instrument(ticker: impl Into<String>) -> VariationalWsInstrument {
    VariationalWsInstrument::perpetual(ticker, VARIATIONAL_WS_PRICE_FUNDING_INTERVAL_SECS)
}
