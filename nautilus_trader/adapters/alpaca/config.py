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

from typing import Literal

from nautilus_trader.common.config import PositiveInt
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig
from nautilus_trader.model.enums import AccountType


class AlpacaInstrumentProviderConfig(InstrumentProviderConfig, frozen=True):
    """
    Configuration for ``AlpacaInstrumentProvider`` instances.
    """

    load_all: bool = True
    asset_classes: frozenset[str] = frozenset({"us_equity", "crypto"})
    option_underlyings: frozenset[str] = frozenset()
    statuses: frozenset[str] = frozenset({"active"})


class AlpacaDataClientConfig(LiveDataClientConfig, frozen=True):
    """
    Configuration for ``AlpacaDataClient`` instances.
    """

    instrument_provider: AlpacaInstrumentProviderConfig = AlpacaInstrumentProviderConfig()
    api_key: str | None = None
    api_secret: str | None = None
    paper: bool = True
    stock_feed: Literal["iex", "sip", "otc"] = "iex"
    crypto_loc: str = "us"
    option_feed: Literal["indicative", "opra"] = "indicative"
    trading_base_url: str | None = None
    data_base_url: str | None = None
    data_ws_base_url: str | None = None
    trading_ws_url: str | None = None
    http_timeout_secs: PositiveInt = 10


class AlpacaExecClientConfig(LiveExecClientConfig, frozen=True):
    """
    Configuration for ``AlpacaExecutionClient`` instances.
    """

    instrument_provider: AlpacaInstrumentProviderConfig = AlpacaInstrumentProviderConfig()
    api_key: str | None = None
    api_secret: str | None = None
    paper: bool = True
    trading_base_url: str | None = None
    data_base_url: str | None = None
    trading_ws_url: str | None = None
    account_type: AccountType = AccountType.MARGIN
    http_timeout_secs: PositiveInt = 10
