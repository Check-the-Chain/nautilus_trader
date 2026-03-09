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

from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from nautilus_trader.adapters.alpaca.common import asset_to_instrument
from nautilus_trader.adapters.alpaca.config import AlpacaDataClientConfig
from nautilus_trader.adapters.alpaca.constants import ALPACA_VENUE
from nautilus_trader.adapters.alpaca.data import AlpacaDataClient
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from tests.integration_tests.adapters.alpaca.conftest import create_ws_mock


@pytest.fixture
def data_client_builder(
    event_loop,
    mock_http_client,
    msgbus,
    cache,
    live_clock,
    mock_instrument_provider,
):
    def builder(monkeypatch):
        stock_ws = create_ws_mock()
        crypto_ws = create_ws_mock()
        option_ws = create_ws_mock()

        monkeypatch.setattr(
            "nautilus_trader.adapters.alpaca.data.AlpacaWebSocketClient",
            lambda *args, **kwargs: (
                option_ws
                if "/v1beta1/" in kwargs.get("url", args[0] if args else "")
                else crypto_ws
                if "/v1beta3/" in kwargs.get("url", args[0] if args else "")
                else stock_ws
            ),
        )

        mock_http_client.reset_mock()
        mock_instrument_provider.initialize.reset_mock()

        client = AlpacaDataClient(
            loop=event_loop,
            client=mock_http_client,
            msgbus=msgbus,
            cache=cache,
            clock=live_clock,
            instrument_provider=mock_instrument_provider,
            config=AlpacaDataClientConfig(api_key="key", api_secret="secret"),
            name=None,
        )
        return client, stock_ws, crypto_ws, option_ws, mock_http_client, mock_instrument_provider

    return builder


@pytest.mark.asyncio
async def test_connect_initializes_provider(data_client_builder, monkeypatch):
    client, _, _, _, _, instrument_provider = data_client_builder(monkeypatch)

    await client._connect()

    instrument_provider.initialize.assert_awaited_once()


@pytest.mark.asyncio
async def test_disconnect_closes_open_websockets(data_client_builder, monkeypatch, equity_instrument, crypto_instrument):
    client, stock_ws, crypto_ws, _, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    await client._subscribe_quote_ticks(SimpleNamespace(instrument_id=equity_instrument.id))
    await client._subscribe_trade_ticks(SimpleNamespace(instrument_id=crypto_instrument.id))
    await client._disconnect()

    stock_ws.close.assert_awaited_once()
    crypto_ws.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_subscribe_stock_quotes_is_idempotent(data_client_builder, monkeypatch, equity_instrument):
    client, stock_ws, _, _, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    await client._subscribe_quote_ticks(SimpleNamespace(instrument_id=equity_instrument.id))
    first_await_count = stock_ws.send_json.await_count
    await client._subscribe_quote_ticks(SimpleNamespace(instrument_id=equity_instrument.id))

    stock_ws.connect.assert_awaited_once()
    assert first_await_count == 2
    assert stock_ws.send_json.await_count == first_await_count


@pytest.mark.asyncio
async def test_subscribe_stock_quotes_loads_missing_instrument(
    data_client_builder,
    monkeypatch,
):
    client, stock_ws, _, _, _, instrument_provider = data_client_builder(monkeypatch)
    msft_instrument = asset_to_instrument(
        {
            "symbol": "MSFT",
            "asset_class": "us_equity",
            "status": "active",
            "tradable": True,
        },
    )
    assert msft_instrument is not None
    instruments = {}
    instrument_provider.find.side_effect = lambda instrument_id: instruments.get(instrument_id)

    async def load_async(instrument_id):
        if instrument_id == msft_instrument.id:
            instruments[instrument_id] = msft_instrument

    instrument_provider.load_async.side_effect = load_async

    await client._subscribe_quote_ticks(SimpleNamespace(instrument_id=msft_instrument.id))

    instrument_provider.load_async.assert_awaited_once_with(msft_instrument.id)
    stock_ws.send_json.assert_any_await({"action": "subscribe", "quotes": ["MSFT"]})


@pytest.mark.asyncio
async def test_subscribe_crypto_trades(data_client_builder, monkeypatch, crypto_instrument):
    client, _, crypto_ws, _, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    await client._subscribe_trade_ticks(SimpleNamespace(instrument_id=crypto_instrument.id))

    crypto_ws.connect.assert_awaited_once()
    crypto_ws.send_json.assert_any_await({"action": "subscribe", "trades": ["BTC/USD"]})


@pytest.mark.asyncio
async def test_unsubscribe_crypto_trades(data_client_builder, monkeypatch, crypto_instrument):
    client, _, crypto_ws, _, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    await client._subscribe_trade_ticks(SimpleNamespace(instrument_id=crypto_instrument.id))
    await client._unsubscribe_trade_ticks(SimpleNamespace(instrument_id=crypto_instrument.id))

    crypto_ws.send_json.assert_any_await({"action": "unsubscribe", "trades": ["BTC/USD"]})


@pytest.mark.asyncio
async def test_subscribe_option_quotes_uses_option_websocket(
    data_client_builder,
    monkeypatch,
    option_instrument,
):
    client, _, _, option_ws, _, instrument_provider = data_client_builder(monkeypatch)
    instruments = {option_instrument.id: option_instrument}
    instrument_provider.find.side_effect = lambda instrument_id: instruments.get(instrument_id)

    await client._connect()
    await client._subscribe_quote_ticks(SimpleNamespace(instrument_id=option_instrument.id))

    option_ws.connect.assert_awaited_once()
    option_ws.send_json.assert_any_await(
        {"action": "subscribe", "quotes": [option_instrument.id.symbol.value]},
    )


@pytest.mark.asyncio
async def test_recreated_stock_websocket_replays_subscriptions(data_client_builder, monkeypatch, equity_instrument):
    client, stock_ws, _, _, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    await client._subscribe_quote_ticks(SimpleNamespace(instrument_id=equity_instrument.id))
    stock_ws.is_closed.return_value = True

    replacement_ws = create_ws_mock()
    monkeypatch.setattr(
        "nautilus_trader.adapters.alpaca.data.AlpacaWebSocketClient",
        lambda *args, **kwargs: replacement_ws,
    )

    await client._ensure_stock_ws_connected()

    replacement_ws.send_json.assert_any_await({"action": "subscribe", "quotes": ["AAPL"]})


@pytest.mark.asyncio
async def test_stock_disconnect_callback_reconnects_and_replays_subscriptions(
    data_client_builder,
    monkeypatch,
    equity_instrument,
):
    client, stock_ws, _, _, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    await client._subscribe_quote_ticks(SimpleNamespace(instrument_id=equity_instrument.id))

    replacement_ws = create_ws_mock()
    monkeypatch.setattr(
        "nautilus_trader.adapters.alpaca.data.AlpacaWebSocketClient",
        lambda *args, **kwargs: replacement_ws,
    )

    await client._handle_stock_ws_disconnect(RuntimeError("boom"))
    assert client._stock_reconnect_task is not None
    await client._stock_reconnect_task

    replacement_ws.connect.assert_awaited_once()
    replacement_ws.send_json.assert_any_await({"action": "subscribe", "quotes": ["AAPL"]})
    stock_ws.close.assert_not_awaited()


def test_handle_stock_quote_message_routes_data(data_client_builder, monkeypatch):
    client, _, _, _, _, _ = data_client_builder(monkeypatch)
    handle_data = MagicMock()
    monkeypatch.setattr(client, "_handle_data", handle_data)

    client._handle_stock_msg(
        {
            "T": "q",
            "S": "AAPL",
            "bp": 150.0,
            "ap": 150.1,
            "bs": 10,
            "as": 12,
            "t": "2026-03-09T10:00:00Z",
        },
    )

    assert isinstance(handle_data.call_args.args[0], QuoteTick)


def test_handle_crypto_trade_message_routes_data(data_client_builder, monkeypatch):
    client, _, _, _, _, _ = data_client_builder(monkeypatch)
    handle_data = MagicMock()
    monkeypatch.setattr(client, "_handle_data", handle_data)

    client._handle_crypto_msg(
        {
            "T": "t",
            "S": "BTCUSD",
            "p": 100000.0,
            "s": 0.1,
            "i": "trade-1",
            "t": "2026-03-09T10:00:00Z",
        },
    )

    assert isinstance(handle_data.call_args.args[0], TradeTick)


def test_handle_option_quote_message_routes_data(
    data_client_builder,
    monkeypatch,
    option_instrument,
):
    client, _, _, _, _, instrument_provider = data_client_builder(monkeypatch)
    instrument_provider.instrument_for_symbol.side_effect = (
        lambda symbol: option_instrument if symbol == option_instrument.id.symbol.value else None
    )
    handle_data = MagicMock()
    monkeypatch.setattr(client, "_handle_data", handle_data)

    client._handle_option_msg(
        {
            "T": "q",
            "S": option_instrument.id.symbol.value,
            "bp": 5.0,
            "ap": 5.1,
            "bs": 10,
            "as": 12,
            "t": "2026-03-09T10:00:00Z",
        },
    )

    assert isinstance(handle_data.call_args.args[0], QuoteTick)


def test_handle_bar_message_uses_registered_bar_type(data_client_builder, monkeypatch):
    client, _, _, _, _, _ = data_client_builder(monkeypatch)
    handle_data = MagicMock()
    monkeypatch.setattr(client, "_handle_data", handle_data)
    bar_type = BarType.from_str("AAPL.ALPACA-1-MINUTE-LAST-EXTERNAL")
    client._bar_types["AAPL"] = bar_type

    client._handle_stock_msg(
        {
            "T": "b",
            "S": "AAPL",
            "o": 150.0,
            "h": 151.0,
            "l": 149.5,
            "c": 150.5,
            "v": 10,
            "t": "2026-03-09T10:00:00Z",
        },
    )

    bar = handle_data.call_args.args[0]
    assert isinstance(bar, Bar)
    assert bar.bar_type == bar_type


def test_handle_unknown_symbol_message_is_ignored(data_client_builder, monkeypatch):
    client, _, _, _, _, instrument_provider = data_client_builder(monkeypatch)
    instrument_provider.instrument_for_symbol.return_value = None
    handle_data = MagicMock()
    monkeypatch.setattr(client, "_handle_data", handle_data)

    client._handle_stock_msg({"T": "q", "S": "UNKNOWN"})

    handle_data.assert_not_called()


@pytest.mark.asyncio
async def test_request_quote_ticks_paginates(data_client_builder, monkeypatch):
    client, _, _, _, http_client, _ = data_client_builder(monkeypatch)
    http_client.get_stock_quotes.side_effect = [
        {
            "quotes": {
                "AAPL": [
                    {
                        "S": "AAPL",
                        "bp": 150.0,
                        "ap": 150.1,
                        "bs": 10,
                        "as": 12,
                        "t": "2026-03-09T10:00:00Z",
                    },
                ],
            },
            "next_page_token": "page-2",
        },
        {
            "quotes": {
                "AAPL": [
                    {
                        "S": "AAPL",
                        "bp": 150.2,
                        "ap": 150.3,
                        "bs": 8,
                        "as": 9,
                        "t": "2026-03-09T10:00:01Z",
                    },
                ],
            },
        },
    ]
    handle_quotes = MagicMock()
    monkeypatch.setattr(client, "_handle_quote_ticks", handle_quotes)

    request = SimpleNamespace(
        instrument_id=InstrumentId.from_str("AAPL.ALPACA"),
        start=pd.Timestamp("2026-03-09T10:00:00Z"),
        end=pd.Timestamp("2026-03-09T10:01:00Z"),
        limit=2,
        id=TestIdStubs.uuid(),
        params={},
    )

    await client._request_quote_ticks(request)

    assert http_client.get_stock_quotes.await_count == 2
    assert len(handle_quotes.call_args.args[1]) == 2


@pytest.mark.asyncio
async def test_request_quote_ticks_loads_missing_instrument(
    data_client_builder,
    monkeypatch,
):
    client, _, _, _, http_client, instrument_provider = data_client_builder(monkeypatch)
    msft_instrument = asset_to_instrument(
        {
            "symbol": "MSFT",
            "asset_class": "us_equity",
            "status": "active",
            "tradable": True,
        },
    )
    assert msft_instrument is not None
    instruments = {}
    instrument_provider.find.side_effect = lambda instrument_id: instruments.get(instrument_id)

    async def load_async(instrument_id):
        if instrument_id == msft_instrument.id:
            instruments[instrument_id] = msft_instrument

    instrument_provider.load_async.side_effect = load_async
    http_client.get_stock_quotes.return_value = {
        "quotes": {
            "MSFT": [
                {
                    "S": "MSFT",
                    "bp": 150.0,
                    "ap": 150.1,
                    "bs": 10,
                    "as": 12,
                    "t": "2026-03-09T10:00:00Z",
                },
            ],
        },
    }
    handle_quotes = MagicMock()
    monkeypatch.setattr(client, "_handle_quote_ticks", handle_quotes)

    request = SimpleNamespace(
        instrument_id=msft_instrument.id,
        start=pd.Timestamp("2026-03-09T10:00:00Z"),
        end=pd.Timestamp("2026-03-09T10:01:00Z"),
        limit=10,
        id=TestIdStubs.uuid(),
        params={},
    )

    await client._request_quote_ticks(request)

    instrument_provider.load_async.assert_awaited_once_with(msft_instrument.id)
    handle_quotes.assert_called_once()


@pytest.mark.asyncio
async def test_request_crypto_trade_ticks_uses_crypto_path(data_client_builder, monkeypatch, crypto_instrument):
    client, _, _, _, http_client, _ = data_client_builder(monkeypatch)
    http_client.get_crypto_trades.return_value = {
        "trades": {
            "BTC/USD": [
                {
                    "S": "BTC/USD",
                    "p": 100000.0,
                    "s": 0.1,
                    "i": "trade-1",
                    "t": "2026-03-09T10:00:00Z",
                },
            ],
        },
    }
    handle_trades = MagicMock()
    monkeypatch.setattr(client, "_handle_trade_ticks", handle_trades)

    request = SimpleNamespace(
        instrument_id=crypto_instrument.id,
        start=pd.Timestamp("2026-03-09T10:00:00Z"),
        end=pd.Timestamp("2026-03-09T10:01:00Z"),
        limit=10,
        id=TestIdStubs.uuid(),
        params={},
    )

    await client._request_trade_ticks(request)

    http_client.get_crypto_trades.assert_awaited_once()
    assert len(handle_trades.call_args.args[1]) == 1


@pytest.mark.asyncio
async def test_request_option_quote_ticks_uses_option_path(
    data_client_builder,
    monkeypatch,
    option_instrument,
):
    client, _, _, _, http_client, instrument_provider = data_client_builder(monkeypatch)
    instruments = {option_instrument.id: option_instrument}
    instrument_provider.find.side_effect = lambda instrument_id: instruments.get(instrument_id)
    http_client.get_option_quotes.return_value = {
        "quotes": {
            option_instrument.id.symbol.value: [
                {
                    "S": option_instrument.id.symbol.value,
                    "bp": 5.0,
                    "ap": 5.1,
                    "bs": 10,
                    "as": 11,
                    "t": "2026-03-09T10:00:00Z",
                },
            ],
        },
    }
    handle_quotes = MagicMock()
    monkeypatch.setattr(client, "_handle_quote_ticks", handle_quotes)

    request = SimpleNamespace(
        instrument_id=option_instrument.id,
        start=pd.Timestamp("2026-03-09T10:00:00Z"),
        end=pd.Timestamp("2026-03-09T10:01:00Z"),
        limit=10,
        id=TestIdStubs.uuid(),
        params={},
    )

    await client._request_quote_ticks(request)

    http_client.get_option_quotes.assert_awaited_once()
    assert http_client.get_option_quotes.await_args.kwargs["feed"] == "indicative"
    assert len(handle_quotes.call_args.args[1]) == 1


@pytest.mark.asyncio
async def test_request_stock_bars(data_client_builder, monkeypatch, mock_http_client):
    client, _, _, _, _, _ = data_client_builder(monkeypatch)
    mock_http_client.get_stock_bars.return_value = {
        "bars": {
            "AAPL": [
                {
                    "S": "AAPL",
                    "o": 150.0,
                    "h": 151.0,
                    "l": 149.5,
                    "c": 150.5,
                    "v": 10,
                    "t": "2026-03-09T10:00:00Z",
                },
            ],
        },
    }
    handle_bars = MagicMock()
    monkeypatch.setattr(client, "_handle_bars", handle_bars)

    request = SimpleNamespace(
        bar_type=BarType.from_str("AAPL.ALPACA-1-MINUTE-LAST-EXTERNAL"),
        start=pd.Timestamp("2026-03-09T10:00:00Z"),
        end=pd.Timestamp("2026-03-09T10:01:00Z"),
        limit=10,
        id=TestIdStubs.uuid(),
        params={},
    )

    await client._request_bars(request)

    mock_http_client.get_stock_bars.assert_awaited_once()
    handle_bars.assert_called_once()


@pytest.mark.asyncio
async def test_request_option_bars_uses_option_path(
    data_client_builder,
    monkeypatch,
    option_instrument,
):
    client, _, _, _, http_client, instrument_provider = data_client_builder(monkeypatch)
    instruments = {option_instrument.id: option_instrument}
    instrument_provider.find.side_effect = lambda instrument_id: instruments.get(instrument_id)
    http_client.get_option_bars.return_value = {
        "bars": {
            option_instrument.id.symbol.value: [
                {
                    "S": option_instrument.id.symbol.value,
                    "o": 5.0,
                    "h": 5.2,
                    "l": 4.9,
                    "c": 5.1,
                    "v": 10,
                    "t": "2026-03-09T10:00:00Z",
                },
            ],
        },
    }
    handle_bars = MagicMock()
    monkeypatch.setattr(client, "_handle_bars", handle_bars)

    request = SimpleNamespace(
        bar_type=BarType.from_str(f"{option_instrument.id.value}-1-MINUTE-LAST-EXTERNAL"),
        start=pd.Timestamp("2026-03-09T10:00:00Z"),
        end=pd.Timestamp("2026-03-09T10:01:00Z"),
        limit=10,
        id=TestIdStubs.uuid(),
        params={},
    )

    await client._request_bars(request)

    http_client.get_option_bars.assert_awaited_once()
    assert http_client.get_option_bars.await_args.kwargs["feed"] == "indicative"
    handle_bars.assert_called_once()


@pytest.mark.asyncio
async def test_subscribe_option_bars_is_rejected(
    data_client_builder,
    monkeypatch,
    option_instrument,
):
    client, _, _, option_ws, _, instrument_provider = data_client_builder(monkeypatch)
    instruments = {option_instrument.id: option_instrument}
    instrument_provider.find.side_effect = lambda instrument_id: instruments.get(instrument_id)
    bar_type = BarType.from_str(f"{option_instrument.id.value}-1-MINUTE-LAST-EXTERNAL")

    await client._subscribe_bars(
        SimpleNamespace(
            bar_type=bar_type,
        ),
    )

    option_ws.send_json.assert_not_awaited()
    assert option_instrument.id.symbol.value not in client._bar_types


@pytest.mark.asyncio
async def test_request_instrument_loads_missing_instrument(
    data_client_builder,
    monkeypatch,
    equity_instrument,
    mock_instrument_provider,
):
    client, _, _, _, _, _ = data_client_builder(monkeypatch)
    mock_instrument_provider.find.side_effect = [None, equity_instrument]
    handle_instrument = MagicMock()
    monkeypatch.setattr(client, "_handle_instrument", handle_instrument)

    request = SimpleNamespace(
        instrument_id=equity_instrument.id,
        start=None,
        end=None,
        id=TestIdStubs.uuid(),
        params={},
    )

    await client._request_instrument(request)

    mock_instrument_provider.load_async.assert_awaited_once_with(equity_instrument.id)
    handle_instrument.assert_called_once()


@pytest.mark.asyncio
async def test_request_instruments_handles_venue_wide_query(data_client_builder, monkeypatch):
    client, _, _, _, _, _ = data_client_builder(monkeypatch)
    handle_instruments = MagicMock()
    monkeypatch.setattr(client, "_handle_instruments", handle_instruments)

    request = SimpleNamespace(
        venue=ALPACA_VENUE,
        start=None,
        end=None,
        id=TestIdStubs.uuid(),
        params={},
    )

    await client._request_instruments(request)

    handle_instruments.assert_called_once()
    assert len(handle_instruments.call_args.args[1]) == 2
