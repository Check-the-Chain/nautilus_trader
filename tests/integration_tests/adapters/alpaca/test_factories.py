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
from unittest.mock import MagicMock

from nautilus_trader.adapters.alpaca.config import AlpacaDataClientConfig
from nautilus_trader.adapters.alpaca.config import AlpacaExecClientConfig
from nautilus_trader.adapters.alpaca.config import AlpacaInstrumentProviderConfig
from nautilus_trader.adapters.alpaca.factories import _ALPACA_INSTRUMENT_PROVIDERS
from nautilus_trader.adapters.alpaca.factories import get_cached_alpaca_http_client
from nautilus_trader.adapters.alpaca.factories import get_cached_alpaca_instrument_provider
from nautilus_trader.adapters.alpaca.http import AlpacaHttpClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.identifiers import InstrumentId


def make_http_client_mock() -> MagicMock:
    client = MagicMock(spec=AlpacaHttpClient)
    client.api_key = "key"
    client.paper = True
    client.trading_base_url = "https://paper-api.alpaca.markets"
    client.data_base_url = "https://data.alpaca.markets"
    return client


class TestAlpacaDataClientConfig:
    def test_default_config(self):
        config = AlpacaDataClientConfig()

        assert config.paper is True
        assert config.stock_feed == "iex"
        assert config.crypto_loc == "us"
        assert config.option_feed == "indicative"
        assert config.http_timeout_secs == 10

    def test_cached_http_client_reuses_identical_configs(self):
        get_cached_alpaca_http_client.cache_clear()

        client1 = get_cached_alpaca_http_client(
            api_key="key",
            api_secret="secret",
            paper=True,
            trading_base_url=None,
            data_base_url=None,
            timeout_secs=10,
        )
        client2 = get_cached_alpaca_http_client(
            api_key="key",
            api_secret="secret",
            paper=True,
            trading_base_url=None,
            data_base_url=None,
            timeout_secs=10,
        )

        assert client1 is client2


class TestAlpacaExecClientConfig:
    def test_default_config(self):
        config = AlpacaExecClientConfig()

        assert config.paper is True
        assert config.account_type == AccountType.MARGIN
        assert config.http_timeout_secs == 10

    def test_custom_credentials(self):
        config = AlpacaExecClientConfig(api_key="key", api_secret="secret", paper=False)

        assert config.api_key == "key"
        assert config.api_secret == "secret"
        assert config.paper is False

    def test_cached_instrument_provider_reuses_identical_configs(self):
        _ALPACA_INSTRUMENT_PROVIDERS.clear()
        client = make_http_client_mock()

        config = AlpacaInstrumentProviderConfig()
        provider1 = get_cached_alpaca_instrument_provider(client=client, config=config)
        provider2 = get_cached_alpaca_instrument_provider(client=client, config=config)

        assert provider1 is provider2

    def test_cached_instrument_provider_separates_distinct_load_ids(self):
        _ALPACA_INSTRUMENT_PROVIDERS.clear()
        client = make_http_client_mock()

        provider1 = get_cached_alpaca_instrument_provider(
            client=client,
            config=AlpacaInstrumentProviderConfig(
                load_ids=frozenset({InstrumentId.from_str("AAPL.ALPACA")}),
            ),
        )
        provider2 = get_cached_alpaca_instrument_provider(
            client=client,
            config=AlpacaInstrumentProviderConfig(
                load_ids=frozenset({InstrumentId.from_str("MSFT.ALPACA")}),
            ),
        )

        assert provider1 is not provider2

    def test_cached_instrument_provider_separates_distinct_clients(self):
        _ALPACA_INSTRUMENT_PROVIDERS.clear()

        provider1 = get_cached_alpaca_instrument_provider(
            client=make_http_client_mock(),
            config=AlpacaInstrumentProviderConfig(),
        )
        provider2 = get_cached_alpaca_instrument_provider(
            client=make_http_client_mock(),
            config=AlpacaInstrumentProviderConfig(),
        )

        assert provider1 is not provider2

    def test_cached_instrument_provider_normalizes_nested_filters(self):
        _ALPACA_INSTRUMENT_PROVIDERS.clear()
        client = make_http_client_mock()
        config = AlpacaInstrumentProviderConfig(
            filters={
                "symbols": ["AAPL", "MSFT"],
                "metadata": {
                    "asset_classes": {"us_equity", "crypto"},
                },
            },
        )

        provider1 = get_cached_alpaca_instrument_provider(client=client, config=config)
        provider2 = get_cached_alpaca_instrument_provider(client=client, config=config)

        assert provider1 is provider2

    def test_cached_instrument_provider_separates_distinct_option_underlyings(self):
        _ALPACA_INSTRUMENT_PROVIDERS.clear()
        client = make_http_client_mock()

        provider1 = get_cached_alpaca_instrument_provider(
            client=client,
            config=AlpacaInstrumentProviderConfig(option_underlyings=frozenset({"AAPL"})),
        )
        provider2 = get_cached_alpaca_instrument_provider(
            client=client,
            config=AlpacaInstrumentProviderConfig(option_underlyings=frozenset({"MSFT"})),
        )

        assert provider1 is not provider2
