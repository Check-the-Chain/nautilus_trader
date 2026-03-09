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

import json
from collections.abc import Iterable
from decimal import Decimal
from typing import Any

from nautilus_trader.adapters.lighter.constants import LIGHTER_MARKET_TYPE_PERP
from nautilus_trader.adapters.lighter.constants import LIGHTER_MARKET_TYPE_SPOT
from nautilus_trader.adapters.lighter.constants import LIGHTER_PERP_SUFFIX
from nautilus_trader.adapters.lighter.constants import LIGHTER_SETTLEMENT_CURRENCY
from nautilus_trader.adapters.lighter.constants import LIGHTER_SPOT_SUFFIX
from nautilus_trader.adapters.lighter.constants import LIGHTER_VENUE
from nautilus_trader.adapters.lighter.parsing import decimal_increment
from nautilus_trader.adapters.lighter.parsing import normalize_market_type
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class LighterInstrumentProvider(InstrumentProvider):
    """
    Load spot and perpetual instruments from Lighter.
    """

    def __init__(
        self,
        client: nautilus_pyo3.LighterHttpClient,  # type: ignore[name-defined]
        config: InstrumentProviderConfig | None = None,
    ) -> None:
        PyCondition.not_none(client, "client")
        super().__init__(config=config or InstrumentProviderConfig())

        self._client = client
        self._instrument_by_market_id: dict[int, Instrument] = {}
        self._metadata_by_market_id: dict[int, dict[str, Any]] = {}
        self._metadata_by_instrument_id: dict[InstrumentId, dict[str, Any]] = {}

    def instrument_for_market_id(self, market_id: int) -> Instrument | None:
        return self._instrument_by_market_id.get(market_id)

    def metadata_for_market_id(self, market_id: int) -> dict[str, Any] | None:
        return self._metadata_by_market_id.get(market_id)

    def metadata_for_instrument(self, instrument_id: InstrumentId) -> dict[str, Any] | None:
        return self._metadata_by_instrument_id.get(instrument_id)

    def instrument_metadata(self, instrument_id: InstrumentId) -> dict[str, Any] | None:
        return self.metadata_for_instrument(instrument_id)

    def market_id_for_instrument(self, instrument_id: InstrumentId) -> int | None:
        metadata = self.metadata_for_instrument(instrument_id)
        if metadata is None:
            return None
        return int(metadata["market_id"])

    def market_ids(self) -> list[int]:
        return sorted(self._instrument_by_market_id)

    async def load_all_async(self, filters: dict | None = None) -> None:
        filters = filters or self._filters
        self._log.info("Loading Lighter instruments...")

        metadata = json.loads(await self._client.load_market_metadata())
        assets_by_id = {
            int(asset["asset_id"]): str(asset["symbol"])
            for asset in metadata.get("assets", [])
            if asset.get("asset_id") is not None and asset.get("symbol")
        }

        self._instruments.clear()
        self._currencies.clear()
        self._instrument_by_market_id.clear()
        self._metadata_by_market_id.clear()
        self._metadata_by_instrument_id.clear()

        for detail in metadata.get("details", []):
            instrument = self._build_instrument(detail, assets_by_id)
            if instrument is None:
                continue
            if not self._accept_instrument(instrument, filters):
                continue

            market_id = int(detail["market_id"])
            self._instrument_by_market_id[market_id] = instrument
            self._metadata_by_market_id[market_id] = detail
            self._metadata_by_instrument_id[instrument.id] = detail
            self.add_currency(instrument.base_currency)
            self.add_currency(instrument.quote_currency)
            self.add(instrument)

    def _build_instrument(
        self,
        detail: dict[str, Any],
        assets_by_id: dict[int, str],
    ) -> Instrument | None:
        market_id = detail.get("market_id")
        if market_id is None:
            return None

        market_type = normalize_market_type(detail)
        raw_symbol_value, base_code, quote_code = self._resolve_symbol_metadata(
            detail,
            assets_by_id,
            market_type,
        )
        if raw_symbol_value is None or base_code is None or quote_code is None:
            return None

        base_currency = Currency.from_str(base_code)
        quote_currency = Currency.from_str(quote_code)
        raw_symbol = Symbol(raw_symbol_value)

        suffix = (
            LIGHTER_PERP_SUFFIX if market_type == LIGHTER_MARKET_TYPE_PERP else LIGHTER_SPOT_SUFFIX
        )
        nautilus_symbol = Symbol(f"{raw_symbol_value}-{suffix}")
        instrument_id = InstrumentId(symbol=nautilus_symbol, venue=LIGHTER_VENUE)

        price_precision = int(
            detail.get("price_decimals") or detail.get("supported_price_decimals") or 0
        )
        size_precision = int(
            detail.get("size_decimals") or detail.get("supported_size_decimals") or 0
        )
        price_increment = Price.from_str(decimal_increment(price_precision))
        size_increment = Quantity.from_str(decimal_increment(size_precision))
        lot_size = Quantity.from_str(decimal_increment(size_precision))

        min_quantity = None
        if detail.get("min_base_amount") is not None:
            min_quantity = Quantity.from_str(str(detail["min_base_amount"]))

        maker_fee = Decimal(str(detail.get("maker_fee") or 0))
        taker_fee = Decimal(str(detail.get("taker_fee") or 0))
        margin_init = None
        margin_maint = None
        if market_type == LIGHTER_MARKET_TYPE_PERP:
            if detail.get("default_initial_margin_fraction") is not None:
                margin_init = Decimal(str(detail["default_initial_margin_fraction"])) / Decimal(
                    10000
                )
            if detail.get("maintenance_margin_fraction") is not None:
                margin_maint = Decimal(str(detail["maintenance_margin_fraction"])) / Decimal(10000)

        ts_event = 0
        ts_init = 0
        info = {
            **detail,
            "market_id": int(market_id),
            "market_type": market_type,
            "raw_symbol": raw_symbol_value,
            "nautilus_symbol": nautilus_symbol.value,
            "price_decimals": price_precision,
            "size_decimals": size_precision,
        }

        if market_type == LIGHTER_MARKET_TYPE_PERP:
            return CryptoPerpetual(
                instrument_id=instrument_id,
                raw_symbol=raw_symbol,
                base_currency=base_currency,
                quote_currency=quote_currency,
                settlement_currency=quote_currency,
                is_inverse=False,
                price_precision=price_precision,
                size_precision=size_precision,
                price_increment=price_increment,
                size_increment=size_increment,
                lot_size=lot_size,
                min_quantity=min_quantity,
                margin_init=margin_init,
                margin_maint=margin_maint,
                maker_fee=maker_fee,
                taker_fee=taker_fee,
                ts_event=ts_event,
                ts_init=ts_init,
                info=info,
            )

        return CurrencyPair(
            instrument_id=instrument_id,
            raw_symbol=raw_symbol,
            base_currency=base_currency,
            quote_currency=quote_currency,
            price_precision=price_precision,
            size_precision=size_precision,
            price_increment=price_increment,
            size_increment=size_increment,
            lot_size=lot_size,
            min_quantity=min_quantity,
            maker_fee=maker_fee,
            taker_fee=taker_fee,
            margin_init=Decimal(0),
            margin_maint=Decimal(0),
            ts_event=ts_event,
            ts_init=ts_init,
            info=info,
        )

    def _resolve_symbol_metadata(
        self,
        detail: dict[str, Any],
        assets_by_id: dict[int, str],
        market_type: str,
    ) -> tuple[str | None, str | None, str | None]:
        base_asset_id = detail.get("base_asset_id")
        quote_asset_id = detail.get("quote_asset_id")
        base_code = assets_by_id.get(int(base_asset_id)) if base_asset_id is not None else None
        quote_code = assets_by_id.get(int(quote_asset_id)) if quote_asset_id is not None else None

        raw_symbol_value = str(detail.get("symbol") or "")
        if not raw_symbol_value:
            if base_code and quote_code:
                raw_symbol_value = f"{base_code}-{quote_code}"
            else:
                return None, None, None

        if base_code is None or quote_code is None:
            symbol_parts = raw_symbol_value.replace("/", "-").split("-")
            if len(symbol_parts) >= 2:
                base_code = base_code or symbol_parts[0]
                quote_code = quote_code or symbol_parts[1]
            elif market_type == LIGHTER_MARKET_TYPE_PERP:
                base_code = base_code or raw_symbol_value
                quote_code = quote_code or LIGHTER_SETTLEMENT_CURRENCY

        return raw_symbol_value, base_code, quote_code

    def _accept_instrument(
        self,
        instrument: Instrument,
        filters: dict | None,
    ) -> bool:
        if not filters:
            return True

        def _normalize(value: Any, *, to_lower: bool = False) -> set[str]:
            if value is None:
                return set()
            if isinstance(value, str):
                values: Iterable[str] = [value]
            else:
                values = value
            result = {
                (item.lower() if to_lower else item.upper())
                for item in values
                if isinstance(item, str)
            }
            return result

        market_type = (
            LIGHTER_MARKET_TYPE_PERP
            if isinstance(instrument, CryptoPerpetual)
            else LIGHTER_MARKET_TYPE_SPOT
        )
        kinds = _normalize(filters.get("market_types") or filters.get("kinds"), to_lower=True)
        if kinds and market_type not in kinds:
            return False

        bases = _normalize(filters.get("bases"))
        if bases and instrument.base_currency.code.upper() not in bases:
            return False

        quotes = _normalize(filters.get("quotes"))
        if quotes and instrument.quote_currency.code.upper() not in quotes:
            return False

        symbols = _normalize(filters.get("symbols"))
        return not (
            symbols
            and instrument.id.symbol.value.upper() not in symbols
            and instrument.raw_symbol.value.upper() not in symbols
        )
