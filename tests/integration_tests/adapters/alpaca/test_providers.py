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

import pytest

from nautilus_trader.adapters.alpaca.common import symbol_to_instrument_id
from nautilus_trader.adapters.alpaca.config import AlpacaInstrumentProviderConfig
from nautilus_trader.adapters.alpaca.providers import AlpacaInstrumentProvider
from tests.integration_tests.adapters.alpaca.conftest import make_option_contract_asset


@pytest.mark.asyncio
async def test_load_all_async_loads_equity_and_crypto(mock_http_client):
    mock_http_client.get_assets.side_effect = [
        [
            {
                "symbol": "AAPL",
                "asset_class": "us_equity",
                "status": "active",
                "tradable": True,
                "fractionable": True,
            },
        ],
        [
            {
                "symbol": "BTC/USD",
                "asset_class": "crypto",
                "status": "active",
                "tradable": True,
                "min_order_size": "0.0001",
                "min_trade_increment": "0.0001",
                "price_increment": "0.01",
            },
        ],
    ]
    provider = AlpacaInstrumentProvider(
        client=mock_http_client,
        config=AlpacaInstrumentProviderConfig(),
    )

    await provider.load_all_async()

    assert provider.count == 2
    assert provider.find(symbol_to_instrument_id("AAPL")) is not None
    assert provider.find(symbol_to_instrument_id("BTC/USD")) is not None
    assert provider.instrument_for_symbol("BTCUSD") is not None


@pytest.mark.asyncio
async def test_load_all_async_filters_symbols(mock_http_client):
    mock_http_client.get_assets.side_effect = [
        [
            {
                "symbol": "AAPL",
                "asset_class": "us_equity",
                "status": "active",
                "tradable": True,
            },
            {
                "symbol": "MSFT",
                "asset_class": "us_equity",
                "status": "active",
                "tradable": True,
            },
        ],
    ]
    provider = AlpacaInstrumentProvider(
        client=mock_http_client,
        config=AlpacaInstrumentProviderConfig(asset_classes=frozenset({"us_equity"})),
    )

    await provider.load_all_async(filters={"symbols": ["MSFT"]})

    assert provider.count == 1
    assert provider.list_all()[0].id.symbol.value == "MSFT"


@pytest.mark.asyncio
async def test_load_async_fetches_single_asset(mock_http_client):
    provider = AlpacaInstrumentProvider(
        client=mock_http_client,
        config=AlpacaInstrumentProviderConfig(load_all=False),
    )

    await provider.load_async(symbol_to_instrument_id("BTC/USD"))

    assert provider.find(symbol_to_instrument_id("BTC/USD")) is not None
    assert provider.instrument_for_symbol("BTCUSD") is not None


@pytest.mark.asyncio
async def test_load_async_fetches_single_option_contract(mock_http_client):
    provider = AlpacaInstrumentProvider(
        client=mock_http_client,
        config=AlpacaInstrumentProviderConfig(load_all=False),
    )

    await provider.load_async(symbol_to_instrument_id("AAPL260320C00150000"))

    assert provider.find(symbol_to_instrument_id("AAPL260320C00150000")) is not None
    assert provider.instrument_for_symbol("AAPL260320C00150000") is not None
    mock_http_client.get_option_contract.assert_awaited_once_with("AAPL260320C00150000")


@pytest.mark.asyncio
async def test_load_ids_async_fetches_requested_assets_without_bulk_listing(mock_http_client):
    provider = AlpacaInstrumentProvider(
        client=mock_http_client,
        config=AlpacaInstrumentProviderConfig(load_all=False),
    )

    await provider.load_ids_async(
        [
            symbol_to_instrument_id("AAPL"),
            symbol_to_instrument_id("BTC/USD"),
        ],
    )

    assert provider.find(symbol_to_instrument_id("AAPL")) is not None
    assert provider.find(symbol_to_instrument_id("BTC/USD")) is not None
    mock_http_client.get_assets.assert_not_awaited()
    assert mock_http_client.get_asset.await_count == 2


@pytest.mark.asyncio
async def test_initialize_with_load_ids_uses_per_instrument_fetch(mock_http_client):
    provider = AlpacaInstrumentProvider(
        client=mock_http_client,
        config=AlpacaInstrumentProviderConfig(
            load_all=False,
            load_ids=frozenset(
                {
                    symbol_to_instrument_id("AAPL"),
                    symbol_to_instrument_id("BTC/USD"),
                },
            ),
        ),
    )

    await provider.initialize()

    assert provider.find(symbol_to_instrument_id("AAPL")) is not None
    assert provider.find(symbol_to_instrument_id("BTC/USD")) is not None
    mock_http_client.get_assets.assert_not_awaited()
    assert mock_http_client.get_asset.await_count == 2


@pytest.mark.asyncio
async def test_provider_helper_methods_return_metadata_and_symbols(mock_http_client):
    mock_http_client.get_assets.side_effect = [
        [
            {
                "symbol": "AAPL",
                "asset_class": "us_equity",
                "status": "active",
                "tradable": True,
                "fractionable": True,
            },
        ],
    ]
    provider = AlpacaInstrumentProvider(
        client=mock_http_client,
        config=AlpacaInstrumentProviderConfig(asset_classes=frozenset({"us_equity"})),
    )

    await provider.load_all_async()
    instrument_id = symbol_to_instrument_id("AAPL")

    assert provider.metadata_for_instrument(instrument_id)["symbol"] == "AAPL"
    assert provider.trade_symbol_for_instrument(instrument_id) == "AAPL"
    assert provider.data_symbol_for_instrument(instrument_id) == "AAPL"


@pytest.mark.asyncio
async def test_load_all_async_loads_option_contracts_with_underlying_filter(mock_http_client):
    mock_http_client.get_option_contracts.return_value = {
        "option_contracts": [
            make_option_contract_asset(),
        ],
    }
    provider = AlpacaInstrumentProvider(
        client=mock_http_client,
        config=AlpacaInstrumentProviderConfig(
            asset_classes=frozenset({"option"}),
            option_underlyings=frozenset({"AAPL"}),
        ),
    )

    await provider.load_all_async()

    option_id = symbol_to_instrument_id("AAPL260320C00150000")
    assert provider.find(option_id) is not None
    assert provider.instrument_for_symbol("AAPL260320C00150000") is not None
    mock_http_client.get_option_contracts.assert_awaited_once()
    params = mock_http_client.get_option_contracts.await_args.kwargs
    assert params["underlying_symbols"] == ["AAPL"]
    assert params["status"] == "active"
