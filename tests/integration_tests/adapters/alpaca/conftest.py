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
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.alpaca.common import asset_to_instrument
from nautilus_trader.adapters.alpaca.common import data_symbol_from_symbol
from nautilus_trader.adapters.alpaca.common import trade_symbol_from_symbol
from nautilus_trader.adapters.alpaca.constants import ALPACA_VENUE
from nautilus_trader.adapters.alpaca.http import AlpacaHttpClient
from nautilus_trader.adapters.alpaca.providers import AlpacaInstrumentProvider
from nautilus_trader.adapters.alpaca.websocket import AlpacaWebSocketClient
from nautilus_trader.model.currencies import BTC
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.events import AccountState
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


@pytest.fixture
def venue() -> Venue:
    return ALPACA_VENUE


@pytest.fixture
def account_id(venue) -> AccountId:
    return AccountId(f"{venue.value}-paper-001")


@pytest.fixture
def equity_instrument() -> Equity:
    return Equity(
        instrument_id=InstrumentId(Symbol("AAPL"), ALPACA_VENUE),
        raw_symbol=Symbol("AAPL"),
        currency=USD,
        price_precision=2,
        price_increment=Price.from_str("0.01"),
        lot_size=Quantity.from_int(1),
        max_quantity=None,
        min_quantity=Quantity.from_int(1),
        ts_event=0,
        ts_init=0,
        info={
            "asset_class": "us_equity",
            "fractionable": True,
            "data_symbol": "AAPL",
            "trade_symbol": "AAPL",
        },
    )


@pytest.fixture
def crypto_instrument() -> CurrencyPair:
    return CurrencyPair(
        instrument_id=InstrumentId(Symbol("BTC/USD"), ALPACA_VENUE),
        raw_symbol=Symbol("BTC/USD"),
        base_currency=BTC,
        quote_currency=USD,
        price_precision=2,
        size_precision=4,
        price_increment=Price.from_str("0.01"),
        size_increment=Quantity.from_str("0.0001"),
        lot_size=None,
        max_quantity=None,
        min_quantity=Quantity.from_str("0.0001"),
        max_notional=None,
        min_notional=None,
        max_price=None,
        min_price=None,
        margin_init=Decimal(0),
        margin_maint=Decimal(0),
        maker_fee=Decimal(0),
        taker_fee=Decimal(0),
        ts_event=0,
        ts_init=0,
        info={
            "asset_class": "crypto",
            "data_symbol": "BTC/USD",
            "trade_symbol": "BTCUSD",
        },
    )


def make_option_contract_asset(
    *,
    symbol: str = "AAPL260320C00150000",
    underlying_symbol: str = "AAPL",
    option_type: str = "call",
    strike_price: str = "150",
    expiration_date: str = "2026-03-20",
) -> dict:
    return {
        "symbol": symbol,
        "status": "active",
        "tradable": True,
        "expiration_date": expiration_date,
        "root_symbol": underlying_symbol,
        "underlying_symbol": underlying_symbol,
        "type": option_type,
        "style": "american",
        "strike_price": strike_price,
        "multiplier": "100",
        "size": "100",
        "asset_class": "option",
    }


@pytest.fixture
def option_instrument():
    instrument = asset_to_instrument(make_option_contract_asset())
    assert instrument is not None
    return instrument


@pytest.fixture
def instrument(equity_instrument):
    return equity_instrument


@pytest.fixture
def account_state(account_id) -> AccountState:
    return AccountState(
        account_id=account_id,
        account_type=AccountType.MARGIN,
        base_currency=USD,
        reported=True,
        balances=[
            AccountBalance(
                total=Money(10_000, USD),
                locked=Money(0, USD),
                free=Money(10_000, USD),
            ),
        ],
        margins=[],
        info={},
        event_id=TestIdStubs.uuid(),
        ts_event=0,
        ts_init=0,
    )


@pytest.fixture
def data_client():
    return None


@pytest.fixture
def exec_client():
    return None


def make_alpaca_order(
    *,
    symbol: str = "AAPL",
    venue_order_id: str = "order-001",
    client_order_id: str = "O-001",
    status: str = "new",
    type_: str = "limit",
    side: str = "buy",
    qty: str = "10",
    filled_qty: str = "0",
    limit_price: str | None = "150.00",
    stop_price: str | None = None,
    submitted_at: str = "2026-03-09T10:00:00Z",
    created_at: str | None = None,
    updated_at: str | None = "2026-03-09T10:00:01Z",
) -> dict:
    return {
        "id": venue_order_id,
        "client_order_id": client_order_id,
        "symbol": symbol,
        "status": status,
        "type": type_,
        "side": side,
        "qty": qty,
        "filled_qty": filled_qty,
        "filled_avg_price": None,
        "limit_price": limit_price,
        "stop_price": stop_price,
        "trail_price": None,
        "trail_percent": None,
        "time_in_force": "day",
        "order_class": "",
        "submitted_at": submitted_at,
        "created_at": created_at or submitted_at,
        "updated_at": updated_at or submitted_at,
        "expires_at": None,
        "filled_at": None,
        "canceled_at": None,
        "expired_at": None,
    }


def make_fill_activity(
    *,
    symbol: str = "AAPL",
    activity_id: str = "fill-001",
    order_id: str = "order-001",
    side: str = "buy",
    qty: str = "2",
    price: str = "150.25",
) -> dict:
    return {
        "id": activity_id,
        "activity_type": "FILL",
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "price": price,
        "order_id": order_id,
        "transaction_time": "2026-03-09T10:00:02Z",
    }


def make_trade_update(
    *,
    event: str = "new",
    symbol: str = "AAPL",
    venue_order_id: str = "order-001",
    client_order_id: str = "O-001",
    type_: str = "limit",
    qty: str = "2",
    price: str = "150.25",
    execution_id: str = "exec-001",
    reason: str | None = None,
) -> dict:
    data = {
        "event": event,
        "timestamp": "2026-03-09T10:00:02Z",
        "order": make_alpaca_order(
            symbol=symbol,
            venue_order_id=venue_order_id,
            client_order_id=client_order_id,
            type_=type_,
        ),
    }
    if event in {"partial_fill", "fill"}:
        data["qty"] = qty
        data["price"] = price
        data["execution_id"] = execution_id
    if reason is not None:
        data["reason"] = reason

    return {"stream": "trade_updates", "data": data}


@pytest.fixture
def mock_http_client():
    mock = MagicMock(spec=AlpacaHttpClient)
    mock.api_key = "key"
    mock.api_secret = "secret"
    mock.auth_headers = {
        "APCA-API-KEY-ID": "key",
        "APCA-API-SECRET-KEY": "secret",
    }

    async def get_asset(symbol_or_asset_id: str) -> dict:
        symbol = str(symbol_or_asset_id).upper().replace("/", "")
        if symbol == "AAPL":
            return {
                "symbol": "AAPL",
                "asset_class": "us_equity",
                "status": "active",
                "tradable": True,
                "fractionable": True,
            }
        if symbol == "BTCUSD":
            return {
                "symbol": "BTC/USD",
                "asset_class": "crypto",
                "status": "active",
                "tradable": True,
                "min_order_size": "0.0001",
                "min_trade_increment": "0.0001",
                "price_increment": "0.01",
            }
        raise RuntimeError(f"Unknown test asset {symbol_or_asset_id}")

    async def get_option_contract(symbol_or_contract_id: str) -> dict:
        symbol = str(symbol_or_contract_id).upper()
        if symbol == "AAPL260320C00150000":
            return make_option_contract_asset()
        raise RuntimeError(f"Unknown test option contract {symbol_or_contract_id}")

    mock.get_assets = AsyncMock(return_value=[])
    mock.get_asset = AsyncMock(side_effect=get_asset)
    mock.get_option_contracts = AsyncMock(return_value={"option_contracts": []})
    mock.get_option_contract = AsyncMock(side_effect=get_option_contract)
    mock.get_account = AsyncMock(
        return_value={
            "account_number": "paper-001",
            "status": "ACTIVE",
            "cash": "10000",
            "equity": "10000",
            "buying_power": "10000",
            "multiplier": "2",
        },
    )
    mock.get_positions = AsyncMock(return_value=[])
    mock.list_orders = AsyncMock(return_value=[])
    mock.get_order = AsyncMock(return_value=make_alpaca_order())
    mock.get_order_by_client_order_id = AsyncMock(return_value=make_alpaca_order())
    mock.submit_order = AsyncMock(return_value=make_alpaca_order())
    mock.replace_order = AsyncMock(return_value=make_alpaca_order(venue_order_id="order-002"))
    mock.cancel_order = AsyncMock()
    mock.cancel_all_orders = AsyncMock(return_value=[])
    mock.get_activities = AsyncMock(return_value=[])
    mock.get_stock_quotes = AsyncMock(return_value={"quotes": {}})
    mock.get_stock_trades = AsyncMock(return_value={"trades": {}})
    mock.get_stock_bars = AsyncMock(return_value={"bars": {}})
    mock.get_option_quotes = AsyncMock(return_value={"quotes": {}})
    mock.get_option_trades = AsyncMock(return_value={"trades": {}})
    mock.get_option_bars = AsyncMock(return_value={"bars": {}})
    mock.get_crypto_quotes = AsyncMock(return_value={"quotes": {}})
    mock.get_crypto_trades = AsyncMock(return_value={"trades": {}})
    mock.get_crypto_bars = AsyncMock(return_value={"bars": {}})
    return mock


def create_ws_mock() -> MagicMock:
    mock = MagicMock(spec=AlpacaWebSocketClient)
    mock.url = "wss://example"
    mock.is_closed = MagicMock(return_value=False)
    mock.connect = AsyncMock()
    mock.send_json = AsyncMock()
    mock.close = AsyncMock()
    return mock


@pytest.fixture
def mock_instrument_provider(equity_instrument, crypto_instrument):
    provider = MagicMock(spec=AlpacaInstrumentProvider)
    provider.initialize = AsyncMock()
    provider.load_async = AsyncMock()
    provider.list_all = MagicMock(return_value=[equity_instrument, crypto_instrument])
    provider.get_all = MagicMock(
        return_value={
            equity_instrument.id: equity_instrument,
            crypto_instrument.id: crypto_instrument,
        },
    )
    provider.currencies = MagicMock(return_value={})
    instrument_mapping = {
        equity_instrument.id: equity_instrument,
        crypto_instrument.id: crypto_instrument,
    }
    symbol_mapping = {
        data_symbol_from_symbol(equity_instrument.id.symbol.value): equity_instrument,
        trade_symbol_from_symbol(equity_instrument.id.symbol.value): equity_instrument,
        data_symbol_from_symbol(crypto_instrument.id.symbol.value): crypto_instrument,
        trade_symbol_from_symbol(crypto_instrument.id.symbol.value): crypto_instrument,
    }
    metadata_mapping = {
        equity_instrument.id: {"symbol": "AAPL", "asset_class": "us_equity"},
        crypto_instrument.id: {"symbol": "BTC/USD", "asset_class": "crypto"},
    }
    provider.find = MagicMock(side_effect=lambda instrument_id: instrument_mapping.get(instrument_id))
    provider.instrument_for_symbol = MagicMock(
        side_effect=lambda symbol: symbol_mapping.get(str(symbol).upper()),
    )
    provider.metadata_for_instrument = MagicMock(
        side_effect=lambda instrument_id: metadata_mapping.get(instrument_id),
    )
    provider.trade_symbol_for_instrument = MagicMock(
        side_effect=lambda instrument_id: (
            "AAPL"
            if instrument_id == equity_instrument.id
            else "BTCUSD"
            if instrument_id == crypto_instrument.id
            else None
        ),
    )
    provider.data_symbol_for_instrument = MagicMock(
        side_effect=lambda instrument_id: (
            "AAPL"
            if instrument_id == equity_instrument.id
            else "BTC/USD"
            if instrument_id == crypto_instrument.id
            else None
        ),
    )
    return provider
