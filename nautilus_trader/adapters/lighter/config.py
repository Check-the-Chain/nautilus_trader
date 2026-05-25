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

from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig
from nautilus_trader.common.config import PositiveInt
from nautilus_trader.core.nautilus_pyo3 import LighterEnvironment


class LighterDataClientConfig(LiveDataClientConfig, frozen=True):
    """
    Configuration for ``LighterDataClient`` instances.
    """

    base_url_http: str | None = None
    base_url_ws: str | None = None
    proxy_url: str | None = None
    environment: LighterEnvironment | None = None
    testnet: bool = False
    # Deprecated: use proxy_url.
    http_proxy_url: str | None = None
    # Deprecated: use proxy_url.
    ws_proxy_url: str | None = None
    http_timeout_secs: PositiveInt = 30


class LighterExecClientConfig(LiveExecClientConfig, frozen=True):
    """
    Configuration for ``LighterExecutionClient`` instances.
    """

    account_index: int | None = None
    private_key: str | None = None
    api_key_index: int | None = None
    api_private_keys: dict[int, str] | None = None
    signer_lib_path: str | None = None
    base_url_http: str | None = None
    base_url_ws: str | None = None
    proxy_url: str | None = None
    environment: LighterEnvironment | None = None
    testnet: bool = False
    # Deprecated: use proxy_url.
    http_proxy_url: str | None = None
    # Deprecated: use proxy_url.
    ws_proxy_url: str | None = None
    http_timeout_secs: PositiveInt = 30
    ws_timeout_secs: PositiveInt = 30
    nonce_mode: str = "optimistic"
    default_auth_token_ttl_secs: PositiveInt = 300
    cancel_all_gtt_secs: PositiveInt = 300
