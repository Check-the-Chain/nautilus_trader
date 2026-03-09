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

import asyncio
from functools import lru_cache

from nautilus_trader.adapters.lighter.config import LighterDataClientConfig
from nautilus_trader.adapters.lighter.config import LighterExecClientConfig
from nautilus_trader.adapters.lighter.data import LighterDataClient
from nautilus_trader.adapters.lighter.execution import LighterExecutionClient
from nautilus_trader.adapters.lighter.providers import LighterInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory


def _normalize_api_private_keys(
    api_private_keys: dict[int, str] | None,
) -> tuple[tuple[int, str], ...] | None:
    if not api_private_keys:
        return None
    return tuple(sorted((int(k), v) for k, v in api_private_keys.items()))


@lru_cache(8)
def get_cached_lighter_http_client(
    *,
    base_url_http: str | None = None,
    base_url_ws: str | None = None,
    testnet: bool = False,
    proxy_url: str | None = None,
    account_index: int | None = None,
    private_key: str | None = None,
    api_key_index: int | None = None,
    api_private_keys: tuple[tuple[int, str], ...] | None = None,
    nonce_mode: str = "optimistic",
    signer_lib_path: str | None = None,
    timeout_secs: int = 30,
) -> nautilus_pyo3.LighterHttpClient:  # type: ignore[name-defined]
    return nautilus_pyo3.LighterHttpClient(  # type: ignore[attr-defined]
        base_url_http=base_url_http,
        base_url_ws=base_url_ws,
        is_testnet=testnet,
        proxy_url=proxy_url,
        account_index=account_index,
        private_key=private_key,
        api_key_index=api_key_index,
        api_private_keys=dict(api_private_keys) if api_private_keys else None,
        nonce_mode=nonce_mode,
        signer_lib_path=signer_lib_path,
        timeout_secs=timeout_secs,
    )


@lru_cache(4)
def get_cached_lighter_instrument_provider(
    client: nautilus_pyo3.LighterHttpClient,  # type: ignore[name-defined]
    config: InstrumentProviderConfig | None = None,
) -> LighterInstrumentProvider:
    return LighterInstrumentProvider(client=client, config=config)


class LighterLiveDataClientFactory(LiveDataClientFactory):
    """
    Provides a Lighter live data client factory.
    """

    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: LighterDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> LighterDataClient:
        client = get_cached_lighter_http_client(
            base_url_http=config.base_url_http,
            base_url_ws=config.base_url_ws,
            testnet=config.testnet,
            proxy_url=config.http_proxy_url,
            timeout_secs=config.http_timeout_secs,
        )
        provider = get_cached_lighter_instrument_provider(
            client=client,
            config=config.instrument_provider,
        )
        return LighterDataClient(
            loop=loop,
            client=client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )


class LighterLiveExecClientFactory(LiveExecClientFactory):
    """
    Provides a Lighter live execution client factory.
    """

    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: LighterExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> LighterExecutionClient:
        client = get_cached_lighter_http_client(
            base_url_http=config.base_url_http,
            base_url_ws=config.base_url_ws,
            testnet=config.testnet,
            proxy_url=config.http_proxy_url,
            account_index=config.account_index,
            private_key=config.private_key,
            api_key_index=config.api_key_index,
            api_private_keys=_normalize_api_private_keys(config.api_private_keys),
            nonce_mode=config.nonce_mode,
            signer_lib_path=config.signer_lib_path,
            timeout_secs=config.http_timeout_secs,
        )
        provider = get_cached_lighter_instrument_provider(
            client=client,
            config=config.instrument_provider,
        )
        return LighterExecutionClient(
            loop=loop,
            client=client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )
