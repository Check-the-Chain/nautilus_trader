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

//! Base URL resolution for Alpaca REST and WebSocket endpoints.

use super::enums::{AlpacaDataFeed, AlpacaEnvironment};

const ALPACA_LIVE_TRADING_HTTP_URL: &str = "https://api.alpaca.markets";
const ALPACA_PAPER_TRADING_HTTP_URL: &str = "https://paper-api.alpaca.markets";

const ALPACA_DATA_HTTP_URL: &str = "https://data.alpaca.markets";

const ALPACA_LIVE_TRADE_UPDATES_WS_URL: &str = "wss://api.alpaca.markets/stream";
const ALPACA_PAPER_TRADE_UPDATES_WS_URL: &str = "wss://paper-api.alpaca.markets/stream";

const ALPACA_STOCKS_STREAM_IEX_WS_URL: &str = "wss://stream.data.alpaca.markets/v2/iex";
const ALPACA_STOCKS_STREAM_SIP_WS_URL: &str = "wss://stream.data.alpaca.markets/v2/sip";
const ALPACA_STOCKS_STREAM_DELAYED_SIP_WS_URL: &str =
    "wss://stream.data.alpaca.markets/v2/delayed_sip";
const ALPACA_STOCKS_STREAM_TEST_WS_URL: &str = "wss://stream.data.alpaca.markets/v2/test";

/// Returns the Trading API base URL for the given environment.
#[must_use]
pub const fn alpaca_trading_http_url(environment: AlpacaEnvironment) -> &'static str {
    match environment {
        AlpacaEnvironment::Live => ALPACA_LIVE_TRADING_HTTP_URL,
        AlpacaEnvironment::Paper => ALPACA_PAPER_TRADING_HTTP_URL,
    }
}

/// Returns the Market Data API base URL.
///
/// The market data host is shared between live and paper environments.
#[must_use]
pub const fn alpaca_data_http_url() -> &'static str {
    ALPACA_DATA_HTTP_URL
}

/// Returns the trade-updates WebSocket URL for the given environment.
#[must_use]
pub const fn alpaca_trade_updates_ws_url(environment: AlpacaEnvironment) -> &'static str {
    match environment {
        AlpacaEnvironment::Live => ALPACA_LIVE_TRADE_UPDATES_WS_URL,
        AlpacaEnvironment::Paper => ALPACA_PAPER_TRADE_UPDATES_WS_URL,
    }
}

/// Returns the equities market data stream URL for the given feed.
#[must_use]
pub const fn alpaca_stocks_stream_ws_url(feed: AlpacaDataFeed) -> &'static str {
    match feed {
        AlpacaDataFeed::Iex => ALPACA_STOCKS_STREAM_IEX_WS_URL,
        AlpacaDataFeed::Sip => ALPACA_STOCKS_STREAM_SIP_WS_URL,
        AlpacaDataFeed::DelayedSip => ALPACA_STOCKS_STREAM_DELAYED_SIP_WS_URL,
        AlpacaDataFeed::Test => ALPACA_STOCKS_STREAM_TEST_WS_URL,
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_trading_http_url() {
        assert_eq!(
            alpaca_trading_http_url(AlpacaEnvironment::Live),
            ALPACA_LIVE_TRADING_HTTP_URL,
        );
        assert_eq!(
            alpaca_trading_http_url(AlpacaEnvironment::Paper),
            ALPACA_PAPER_TRADING_HTTP_URL,
        );
    }

    #[rstest]
    fn test_trade_updates_ws_url() {
        assert_eq!(
            alpaca_trade_updates_ws_url(AlpacaEnvironment::Live),
            ALPACA_LIVE_TRADE_UPDATES_WS_URL,
        );
        assert_eq!(
            alpaca_trade_updates_ws_url(AlpacaEnvironment::Paper),
            ALPACA_PAPER_TRADE_UPDATES_WS_URL,
        );
    }

    #[rstest]
    #[case(AlpacaDataFeed::Iex, "wss://stream.data.alpaca.markets/v2/iex")]
    #[case(AlpacaDataFeed::Sip, "wss://stream.data.alpaca.markets/v2/sip")]
    #[case(
        AlpacaDataFeed::DelayedSip,
        "wss://stream.data.alpaca.markets/v2/delayed_sip"
    )]
    #[case(AlpacaDataFeed::Test, "wss://stream.data.alpaca.markets/v2/test")]
    fn test_stocks_stream_ws_url(#[case] feed: AlpacaDataFeed, #[case] expected: &str) {
        assert_eq!(alpaca_stocks_stream_ws_url(feed), expected);
    }
}
