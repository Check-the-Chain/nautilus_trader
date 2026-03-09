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
from typing import Any

from nautilus_trader.adapters.alpaca.config import AlpacaDataClientConfig
from nautilus_trader.adapters.alpaca.config import AlpacaExecClientConfig
from nautilus_trader.adapters.alpaca.config import AlpacaInstrumentProviderConfig
from nautilus_trader.adapters.alpaca.data import AlpacaDataClient
from nautilus_trader.adapters.alpaca.execution import AlpacaExecutionClient
from nautilus_trader.adapters.alpaca.http import AlpacaHttpClient
from nautilus_trader.adapters.alpaca.providers import AlpacaInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory


@lru_cache(8)
def get_cached_alpaca_http_client(
    *,
    api_key: str | None,
    api_secret: str | None,
    paper: bool,
    trading_base_url: str | None,
    data_base_url: str | None,
    timeout_secs: int,
) -> AlpacaHttpClient:
    return AlpacaHttpClient(
        api_key=api_key,
        api_secret=api_secret,
        paper=paper,
        trading_base_url=trading_base_url,
        data_base_url=data_base_url,
        timeout_secs=timeout_secs,
    )


_ALPACA_INSTRUMENT_PROVIDERS: dict[tuple, AlpacaInstrumentProvider] = {}


def _freeze_cache_value(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple(
            sorted(
                ((key, _freeze_cache_value(item)) for key, item in value.items()),
                key=lambda item: repr(item[0]),
            ),
        )
    if isinstance(value, list | tuple):
        return tuple(_freeze_cache_value(item) for item in value)
    if isinstance(value, set | frozenset):
        return tuple(sorted((_freeze_cache_value(item) for item in value), key=repr))
    return value


def _normalize_load_ids(load_ids) -> tuple[str, ...] | None:
    if not load_ids:
        return None
    return tuple(sorted(str(instrument_id) for instrument_id in load_ids))


def _provider_cache_key(
    client: AlpacaHttpClient,
    config: AlpacaInstrumentProviderConfig,
) -> tuple[Any, ...]:
    return (
        client,
        tuple(sorted(config.asset_classes)),
        tuple(sorted(config.option_underlyings)),
        tuple(sorted(config.statuses)),
        config.load_all,
        _normalize_load_ids(config.load_ids),
        _freeze_cache_value(config.filters),
        config.filter_callable,
        config.log_warnings,
        config.use_gamma_markets,
    )


def get_cached_alpaca_instrument_provider(
    client: AlpacaHttpClient,
    config: AlpacaInstrumentProviderConfig,
) -> AlpacaInstrumentProvider:
    key = _provider_cache_key(client, config)
    provider = _ALPACA_INSTRUMENT_PROVIDERS.get(key)
    if provider is None:
        provider = AlpacaInstrumentProvider(client=client, config=config)
        _ALPACA_INSTRUMENT_PROVIDERS[key] = provider
    return provider


class AlpacaLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: AlpacaDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> AlpacaDataClient:
        client = get_cached_alpaca_http_client(
            api_key=config.api_key,
            api_secret=config.api_secret,
            paper=config.paper,
            trading_base_url=config.trading_base_url,
            data_base_url=config.data_base_url,
            timeout_secs=config.http_timeout_secs,
        )
        provider = get_cached_alpaca_instrument_provider(
            client=client,
            config=config.instrument_provider,
        )
        return AlpacaDataClient(
            loop=loop,
            client=client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )


class AlpacaLiveExecClientFactory(LiveExecClientFactory):
    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: AlpacaExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> AlpacaExecutionClient:
        client = get_cached_alpaca_http_client(
            api_key=config.api_key,
            api_secret=config.api_secret,
            paper=config.paper,
            trading_base_url=config.trading_base_url,
            data_base_url=config.data_base_url,
            timeout_secs=config.http_timeout_secs,
        )
        provider = get_cached_alpaca_instrument_provider(
            client=client,
            config=config.instrument_provider,
        )
        return AlpacaExecutionClient(
            loop=loop,
            client=client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )
