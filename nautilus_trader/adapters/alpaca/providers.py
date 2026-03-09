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

from collections.abc import Iterable

from nautilus_trader.adapters.alpaca.common import ALPACA_OPTION_ASSET_CLASSES
from nautilus_trader.adapters.alpaca.common import asset_to_instrument
from nautilus_trader.adapters.alpaca.common import data_symbol_for_instrument
from nautilus_trader.adapters.alpaca.common import data_symbol_from_symbol
from nautilus_trader.adapters.alpaca.common import normalize_symbol
from nautilus_trader.adapters.alpaca.common import trade_symbol_for_instrument
from nautilus_trader.adapters.alpaca.common import trade_symbol_from_symbol
from nautilus_trader.adapters.alpaca.config import AlpacaInstrumentProviderConfig
from nautilus_trader.adapters.alpaca.http import AlpacaHttpClient
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument


class AlpacaInstrumentProvider(InstrumentProvider):
    """
    Load supported Alpaca assets and convert them into Nautilus instruments.
    """

    def __init__(
        self,
        client: AlpacaHttpClient,
        config: AlpacaInstrumentProviderConfig | None = None,
    ) -> None:
        PyCondition.not_none(client, "client")
        super().__init__(config=config or AlpacaInstrumentProviderConfig())
        self._client = client
        self._config = config or AlpacaInstrumentProviderConfig()
        self._metadata_by_instrument_id: dict[InstrumentId, dict] = {}
        self._instrument_by_data_symbol: dict[str, Instrument] = {}
        self._instrument_by_trade_symbol: dict[str, Instrument] = {}

    async def load_all_async(self, filters: dict | None = None) -> None:
        filters = filters or self._filters or {}

        configured_classes = filters.get("asset_classes") or self._config.asset_classes
        configured_statuses = filters.get("statuses") or self._config.statuses
        symbols_filter = {
            symbol.upper()
            for symbol in (filters.get("symbols") or [])
            if isinstance(symbol, str)
        }

        self._instruments.clear()
        self._currencies.clear()
        self._metadata_by_instrument_id.clear()
        self._instrument_by_data_symbol.clear()
        self._instrument_by_trade_symbol.clear()

        for status in configured_statuses:
            for asset_class in configured_classes:
                if asset_class in ALPACA_OPTION_ASSET_CLASSES:
                    await self._load_option_contracts(
                        status=status,
                        filters=filters,
                        symbols_filter=symbols_filter,
                    )
                    continue
                assets = await self._client.get_assets(status=status, asset_class=asset_class)
                self._ingest_assets(assets, symbols_filter=symbols_filter)

    async def load_ids_async(
        self,
        instrument_ids: list[InstrumentId],
        filters: dict | None = None,
    ) -> None:
        del filters  # Explicit instrument IDs are fetched directly.

        for instrument_id in instrument_ids:
            await self.load_async(instrument_id)

    async def load_async(self, instrument_id: InstrumentId, filters: dict | None = None) -> None:
        del filters

        if self.find(instrument_id) is not None:
            return

        symbol = instrument_id.symbol.value
        candidates = [symbol, trade_symbol_from_symbol(symbol), data_symbol_from_symbol(symbol)]
        for candidate in candidates:
            try:
                asset = await self._client.get_asset(candidate)
            except Exception as exc:
                self._log.debug(f"Unable to load Alpaca asset for {candidate}: {exc}")
            else:
                self._ingest_assets([asset], symbols_filter=set())
                if self.find(instrument_id) is not None:
                    return

            try:
                option_contract = await self._client.get_option_contract(candidate)
            except Exception as exc:
                self._log.debug(f"Unable to load Alpaca option contract for {candidate}: {exc}")
                continue

            self._ingest_assets([option_contract], symbols_filter=set())
            if self.find(instrument_id) is not None:
                return

    async def _load_option_contracts(
        self,
        *,
        status: str,
        filters: dict,
        symbols_filter: set[str],
    ) -> None:
        if symbols_filter:
            for symbol in sorted(symbols_filter):
                try:
                    asset = await self._client.get_option_contract(symbol)
                except Exception as exc:
                    self._log.debug(f"Unable to load Alpaca option contract for {symbol}: {exc}")
                    continue
                self._ingest_assets([asset], symbols_filter=symbols_filter)
            return

        option_underlyings = sorted(
            {
                normalize_symbol(str(symbol))
                for symbol in (
                    filters.get("option_underlyings")
                    or filters.get("underlying_symbols")
                    or self._config.option_underlyings
                )
                if symbol
            },
        )
        if not option_underlyings:
            self._log.warning(
                "Loading all Alpaca option contracts without `option_underlyings` may be slow",
            )

        page_token: str | None = None
        while True:
            payload = await self._client.get_option_contracts(
                underlying_symbols=option_underlyings or None,
                status=status,
                expiration_date_gte=filters.get("expiration_date_gte"),
                expiration_date_lte=filters.get("expiration_date_lte"),
                option_type=filters.get("option_type"),
                style=filters.get("style"),
                limit=1000,
                page_token=page_token,
            )
            assets = payload.get("option_contracts") or []
            self._ingest_assets(assets, symbols_filter=symbols_filter)
            page_token = self._next_page_token(payload)
            if page_token is None:
                break

    def _ingest_assets(
        self,
        assets: Iterable[dict],
        *,
        symbols_filter: set[str],
    ) -> None:
        for asset in assets:
            instrument = asset_to_instrument(asset)
            if instrument is None:
                continue

            if symbols_filter:
                candidates = {
                    instrument.id.symbol.value.upper(),
                    data_symbol_for_instrument(instrument).upper(),
                    trade_symbol_for_instrument(instrument).upper(),
                }
                if not candidates.intersection(symbols_filter):
                    continue

            if instrument.id in self._instruments:
                continue

            self.add(instrument)
            self._metadata_by_instrument_id[instrument.id] = asset
            self._instrument_by_data_symbol[data_symbol_for_instrument(instrument)] = instrument
            self._instrument_by_trade_symbol[trade_symbol_for_instrument(instrument)] = instrument

            if hasattr(instrument, "base_currency"):
                self.add_currency(instrument.base_currency)
            self.add_currency(instrument.quote_currency)

    def metadata_for_instrument(self, instrument_id: InstrumentId) -> dict | None:
        return self._metadata_by_instrument_id.get(instrument_id)

    def instrument_for_symbol(self, symbol: str) -> Instrument | None:
        return self._instrument_by_data_symbol.get(
            data_symbol_from_symbol(symbol),
        ) or self._instrument_by_trade_symbol.get(
            trade_symbol_from_symbol(symbol),
        )

    @staticmethod
    def _next_page_token(payload: dict) -> str | None:
        token = payload.get("next_page_token")
        return str(token) if token else None

    def trade_symbol_for_instrument(self, instrument_id: InstrumentId) -> str | None:
        instrument = self.find(instrument_id)
        return trade_symbol_for_instrument(instrument) if instrument is not None else None

    def data_symbol_for_instrument(self, instrument_id: InstrumentId) -> str | None:
        instrument = self.find(instrument_id)
        return data_symbol_for_instrument(instrument) if instrument is not None else None
