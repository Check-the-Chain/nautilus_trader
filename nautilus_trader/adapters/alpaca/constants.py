# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
from __future__ import annotations

from typing import Final

from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import Venue


ALPACA: Final[str] = "ALPACA"
ALPACA_VENUE: Final[Venue] = Venue(ALPACA)
ALPACA_CLIENT_ID: Final[ClientId] = ClientId(ALPACA)

ALPACA_LIVE_TRADING_BASE_URL: Final[str] = "https://api.alpaca.markets"
ALPACA_PAPER_TRADING_BASE_URL: Final[str] = "https://paper-api.alpaca.markets"
ALPACA_DATA_BASE_URL: Final[str] = "https://data.alpaca.markets"
ALPACA_DATA_WS_BASE_URL: Final[str] = "wss://stream.data.alpaca.markets"

ALPACA_LIVE_TRADING_WS_URL: Final[str] = "wss://api.alpaca.markets/stream"
ALPACA_PAPER_TRADING_WS_URL: Final[str] = "wss://paper-api.alpaca.markets/stream"

ALPACA_DEFAULT_STOCK_FEED: Final[str] = "iex"
ALPACA_DEFAULT_CRYPTO_LOC: Final[str] = "us"
