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

//! Variational public API response models.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VariationalStats {
    #[serde(default)]
    pub total_volume_24h: Option<String>,
    #[serde(default)]
    pub cumulative_volume: Option<String>,
    #[serde(default)]
    pub tvl: Option<String>,
    #[serde(default)]
    pub open_interest: Option<String>,
    #[serde(default)]
    pub num_markets: Option<u64>,
    #[serde(default)]
    pub loss_refund: Option<VariationalLossRefund>,
    #[serde(default)]
    pub listings: Vec<VariationalListing>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VariationalLossRefund {
    #[serde(default)]
    pub pool_size: Option<String>,
    #[serde(default)]
    pub refunded_24h: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VariationalListing {
    pub ticker: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub mark_price: Option<String>,
    #[serde(default)]
    pub volume_24h: Option<String>,
    #[serde(default)]
    pub open_interest: Option<VariationalOpenInterest>,
    #[serde(default)]
    pub funding_rate: Option<String>,
    #[serde(default)]
    pub funding_interval_s: Option<u64>,
    #[serde(default)]
    pub base_spread_bps: Option<String>,
    #[serde(default)]
    pub quotes: Option<VariationalQuotes>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VariationalOpenInterest {
    #[serde(default)]
    pub long_open_interest: Option<String>,
    #[serde(default)]
    pub short_open_interest: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VariationalQuotes {
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub base: Option<VariationalQuote>,
    #[serde(default)]
    pub size_1k: Option<VariationalQuote>,
    #[serde(default)]
    pub size_100k: Option<VariationalQuote>,
    #[serde(default)]
    pub size_1m: Option<VariationalQuote>,
    #[serde(flatten)]
    pub additional: BTreeMap<String, Value>,
}

impl VariationalQuotes {
    #[must_use]
    pub fn quote_for_key(&self, key: &str) -> Option<&VariationalQuote> {
        match key {
            "base" => self.base.as_ref(),
            "size_1k" => self.size_1k.as_ref(),
            "size_100k" => self.size_100k.as_ref(),
            "size_1m" => self.size_1m.as_ref(),
            _ => None,
        }
    }

    #[must_use]
    pub fn preferred_quote(&self, key: &str) -> Option<&VariationalQuote> {
        self.quote_for_key(key)
            .or(self.base.as_ref())
            .or(self.size_1k.as_ref())
            .or(self.size_100k.as_ref())
            .or(self.size_1m.as_ref())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VariationalQuote {
    #[serde(default)]
    pub bid: Option<String>,
    #[serde(default)]
    pub ask: Option<String>,
}
