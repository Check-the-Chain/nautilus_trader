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

import pytest

from nautilus_trader.adapters.lighter.providers import LighterInstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.instruments import CurrencyPair


class TestLighterInstrumentProvider:
    def test_provider_initialization(self, mock_http_client):
        provider = LighterInstrumentProvider(
            client=mock_http_client,
            config=InstrumentProviderConfig(),
        )

        assert provider is not None

    def test_provider_without_client_raises(self):
        with pytest.raises(TypeError):
            LighterInstrumentProvider(client=None, config=InstrumentProviderConfig())

    @pytest.mark.asyncio
    async def test_load_all_async_loads_perp_and_spot_markets(self, mock_http_client):
        provider = LighterInstrumentProvider(
            client=mock_http_client,
            config=InstrumentProviderConfig(),
        )

        await provider.load_all_async()

        assert provider.count == 2
        perp = provider.instrument_for_market_id(1)
        spot = provider.instrument_for_market_id(2048)
        assert isinstance(perp, CryptoPerpetual)
        assert isinstance(spot, CurrencyPair)
        assert provider.market_id_for_instrument(perp.id) == 1
        assert provider.metadata_for_instrument(perp.id)["market_type"] == "perp"
        assert provider.metadata_for_instrument(spot.id)["market_type"] == "spot"

    @pytest.mark.asyncio
    async def test_load_all_async_respects_filters(self, mock_http_client):
        provider = LighterInstrumentProvider(
            client=mock_http_client,
            config=InstrumentProviderConfig(),
        )

        await provider.load_all_async(filters={"market_type": "perp", "symbols": ["BTC-USDC"]})

        assert provider.count == 1
        only_instrument = next(iter(provider.get_all().values()))
        assert only_instrument.id.symbol.value == "BTC-USDC-PERP"

    @pytest.mark.asyncio
    async def test_load_ids_async_retains_existing_instruments(self, mock_http_client):
        provider = LighterInstrumentProvider(
            client=mock_http_client,
            config=InstrumentProviderConfig(),
        )
        await provider.load_all_async()
        instrument_id = next(iter(provider.get_all()))

        await provider.load_ids_async([instrument_id])

        assert provider.find(instrument_id) is not None

    @pytest.mark.asyncio
    async def test_load_all_async_supports_single_token_perp_symbols(self, mock_http_client):
        mock_http_client.load_market_metadata.return_value = """
        {
            "assets": [
                {"asset_id": 2, "symbol": "USDC"}
            ],
            "details": [
                {
                    "market_id": 83,
                    "symbol": "ASTER",
                    "base_asset_id": 0,
                    "quote_asset_id": 0,
                    "market_type": "perp",
                    "price_decimals": 5,
                    "size_decimals": 1,
                    "default_initial_margin_fraction": 2000,
                    "maintenance_margin_fraction": 1200
                }
            ]
        }
        """

        provider = LighterInstrumentProvider(
            client=mock_http_client,
            config=InstrumentProviderConfig(),
        )

        await provider.load_all_async()

        assert provider.count == 1
        perp = provider.instrument_for_market_id(83)
        assert isinstance(perp, CryptoPerpetual)
        assert perp.id.symbol.value == "ASTER-PERP"
        assert perp.base_currency.code == "ASTER"
        assert perp.quote_currency.code == "USDC"
