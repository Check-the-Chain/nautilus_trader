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

from decimal import Decimal
from typing import Final

from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import Venue


LIGHTER: Final[str] = "LIGHTER"
LIGHTER_VENUE: Final[Venue] = Venue(LIGHTER)
LIGHTER_CLIENT_ID: Final[ClientId] = ClientId(LIGHTER)

LIGHTER_MARKET_TYPE_PERP: Final[str] = "perp"
LIGHTER_MARKET_TYPE_SPOT: Final[str] = "spot"
LIGHTER_PERP_SUFFIX: Final[str] = "PERP"
LIGHTER_SPOT_SUFFIX: Final[str] = "SPOT"
LIGHTER_SETTLEMENT_CURRENCY: Final[str] = "USDC"

LIGHTER_LIMIT_ORDER: Final[int] = 0
LIGHTER_MARKET_ORDER: Final[int] = 1
LIGHTER_STOP_LOSS_ORDER: Final[int] = 2
LIGHTER_STOP_LOSS_LIMIT_ORDER: Final[int] = 3
LIGHTER_TAKE_PROFIT_ORDER: Final[int] = 4
LIGHTER_TAKE_PROFIT_LIMIT_ORDER: Final[int] = 5

LIGHTER_TIF_IOC: Final[int] = 0
LIGHTER_TIF_GTT: Final[int] = 1
LIGHTER_TIF_POST_ONLY: Final[int] = 2

LIGHTER_MARGIN_MODE_CROSS: Final[int] = 0
LIGHTER_MARGIN_MODE_ISOLATED: Final[int] = 1
LIGHTER_UPDATE_MARGIN_REMOVE: Final[int] = 0
LIGHTER_UPDATE_MARGIN_ADD: Final[int] = 1

LIGHTER_FEE_SCALE: Final[Decimal] = Decimal(1000000)
LIGHTER_DEFAULT_MARKET_SLIPPAGE: Final[Decimal] = Decimal("0.005")
LIGHTER_MAX_BATCH_TX_COUNT: Final[int] = 50
LIGHTER_MAX_CLIENT_ORDER_INDEX: Final[int] = (1 << 48) - 1
LIGHTER_DEFAULT_ORDER_EXPIRY_SECS: Final[int] = 30 * 24 * 60 * 60

LIGHTER_DEFAULT_HTTP_URL: Final[str] = nautilus_pyo3.get_lighter_http_base_url(False)
LIGHTER_DEFAULT_WS_URL: Final[str] = nautilus_pyo3.get_lighter_ws_base_url(False)
LIGHTER_TESTNET_HTTP_URL: Final[str] = nautilus_pyo3.get_lighter_http_base_url(True)
LIGHTER_TESTNET_WS_URL: Final[str] = nautilus_pyo3.get_lighter_ws_base_url(True)
