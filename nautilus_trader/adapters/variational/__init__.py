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
"""
Variational Omni integration adapter.

This adapter is read-only while Variational's trading API remains unavailable publicly.
"""

from nautilus_trader.adapters.variational.config import VariationalDataClientConfig
from nautilus_trader.adapters.variational.config import VariationalQuoteTier
from nautilus_trader.adapters.variational.constants import VARIATIONAL
from nautilus_trader.adapters.variational.constants import VARIATIONAL_CLIENT_ID
from nautilus_trader.adapters.variational.constants import VARIATIONAL_VENUE
from nautilus_trader.adapters.variational.factories import VariationalDataClientFactory
from nautilus_trader.core.nautilus_pyo3 import VariationalHttpClient
from nautilus_trader.core.nautilus_pyo3 import get_variational_http_base_url
from nautilus_trader.core.nautilus_pyo3 import get_variational_ws_base_url


__all__ = [
    "VARIATIONAL",
    "VARIATIONAL_CLIENT_ID",
    "VARIATIONAL_VENUE",
    "VariationalDataClientConfig",
    "VariationalDataClientFactory",
    "VariationalHttpClient",
    "VariationalQuoteTier",
    "get_variational_http_base_url",
    "get_variational_ws_base_url",
]
