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
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.lighter.config import LighterDataClientConfig
from nautilus_trader.adapters.lighter.data import LighterDataClient
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import FundingRateUpdate
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import BookType
from tests.integration_tests.adapters.lighter.conftest import _create_ws_mock


@pytest.fixture
def data_client_builder(
    event_loop,
    mock_http_client,
    msgbus,
    cache,
    live_clock,
    mock_instrument_provider,
):
    def builder(monkeypatch, *, config_kwargs: dict | None = None):
        ws_client = _create_ws_mock()
        ws_iter = iter([ws_client])

        monkeypatch.setattr(
            "nautilus_trader.adapters.lighter.data.nautilus_pyo3.LighterWebSocketClient",
            lambda *args, **kwargs: next(ws_iter),
        )

        mock_http_client.reset_mock()
        mock_instrument_provider.initialize.reset_mock()

        config = LighterDataClientConfig(testnet=False, **(config_kwargs or {}))
        client = LighterDataClient(
            loop=event_loop,
            client=mock_http_client,
            msgbus=msgbus,
            cache=cache,
            clock=live_clock,
            instrument_provider=mock_instrument_provider,
            config=config,
            name=None,
        )

        return client, ws_client, mock_http_client, mock_instrument_provider

    return builder


@pytest.mark.asyncio
async def test_connect_and_disconnect_manage_resources(data_client_builder, monkeypatch):
    client, ws_client, _, instrument_provider = data_client_builder(monkeypatch)

    await client._connect()

    try:
        instrument_provider.initialize.assert_awaited_once()
        ws_client.connect.assert_awaited_once()
    finally:
        await client._disconnect()

    ws_client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_subscribe_order_book_deltas(data_client_builder, monkeypatch, instrument):
    client, ws_client, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    try:
        await client._subscribe_order_book_deltas(
            SimpleNamespace(instrument_id=instrument.id, book_type=BookType.L2_MBP),
        )
        ws_client.subscribe_book.assert_awaited_once_with(1)
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_subscribe_quotes_and_trades(data_client_builder, monkeypatch, instrument):
    client, ws_client, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    try:
        await client._subscribe_quote_ticks(SimpleNamespace(instrument_id=instrument.id))
        await client._subscribe_trade_ticks(SimpleNamespace(instrument_id=instrument.id))

        ws_client.subscribe_quotes.assert_awaited_once_with(1)
        ws_client.subscribe_trades.assert_awaited_once_with(1)
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_subscribe_mark_prices(data_client_builder, monkeypatch, instrument):
    client, ws_client, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    try:
        await client._subscribe_mark_prices(SimpleNamespace(instrument_id=instrument.id))

        assert client._market_stats_refcount == 1
        ws_client.subscribe_market_stats.assert_awaited_once()
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_subscribe_index_prices(data_client_builder, monkeypatch, instrument):
    client, ws_client, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    try:
        await client._subscribe_index_prices(SimpleNamespace(instrument_id=instrument.id))

        assert client._market_stats_refcount == 1
        ws_client.subscribe_market_stats.assert_awaited_once()
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_subscribe_funding_rates(data_client_builder, monkeypatch, instrument):
    client, ws_client, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    try:
        await client._subscribe_funding_rates(SimpleNamespace(instrument_id=instrument.id))

        assert client._market_stats_refcount == 1
        ws_client.subscribe_market_stats.assert_awaited_once()
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_subscribe_market_stats_refcounts_shared_channel(
    data_client_builder,
    monkeypatch,
    instrument,
):
    client, ws_client, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    try:
        await client._subscribe_mark_prices(SimpleNamespace(instrument_id=instrument.id))
        await client._subscribe_index_prices(SimpleNamespace(instrument_id=instrument.id))
        await client._subscribe_funding_rates(SimpleNamespace(instrument_id=instrument.id))

        assert client._market_stats_refcount == 3
        ws_client.subscribe_market_stats.assert_awaited_once()

        await client._unsubscribe_mark_prices(SimpleNamespace(instrument_id=instrument.id))
        await client._unsubscribe_index_prices(SimpleNamespace(instrument_id=instrument.id))
        await client._unsubscribe_funding_rates(SimpleNamespace(instrument_id=instrument.id))

        ws_client.unsubscribe_market_stats.assert_awaited_once()
        assert client._market_stats_refcount == 0
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_unsubscribe_order_book_deltas(data_client_builder, monkeypatch, instrument):
    client, ws_client, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    try:
        await client._unsubscribe_order_book_deltas(SimpleNamespace(instrument_id=instrument.id))

        ws_client.unsubscribe_book.assert_awaited_once_with(1)
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_unsubscribe_quote_ticks(data_client_builder, monkeypatch, instrument):
    client, ws_client, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    try:
        await client._unsubscribe_quote_ticks(SimpleNamespace(instrument_id=instrument.id))

        ws_client.unsubscribe_quotes.assert_awaited_once_with(1)
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_unsubscribe_trade_ticks(data_client_builder, monkeypatch, instrument):
    client, ws_client, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    try:
        await client._unsubscribe_trade_ticks(SimpleNamespace(instrument_id=instrument.id))

        ws_client.unsubscribe_trades.assert_awaited_once_with(1)
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_unsubscribe_mark_prices(data_client_builder, monkeypatch, instrument):
    client, ws_client, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    try:
        await client._subscribe_mark_prices(SimpleNamespace(instrument_id=instrument.id))
        await client._unsubscribe_mark_prices(SimpleNamespace(instrument_id=instrument.id))

        assert client._market_stats_refcount == 0
        ws_client.unsubscribe_market_stats.assert_awaited_once()
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_unsubscribe_index_prices(data_client_builder, monkeypatch, instrument):
    client, ws_client, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    try:
        await client._subscribe_index_prices(SimpleNamespace(instrument_id=instrument.id))
        await client._unsubscribe_index_prices(SimpleNamespace(instrument_id=instrument.id))

        assert client._market_stats_refcount == 0
        ws_client.unsubscribe_market_stats.assert_awaited_once()
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_unsubscribe_funding_rates(data_client_builder, monkeypatch, instrument):
    client, ws_client, _, _ = data_client_builder(monkeypatch)

    await client._connect()
    try:
        await client._subscribe_funding_rates(SimpleNamespace(instrument_id=instrument.id))
        await client._unsubscribe_funding_rates(SimpleNamespace(instrument_id=instrument.id))

        assert client._market_stats_refcount == 0
        ws_client.unsubscribe_market_stats.assert_awaited_once()
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_request_trade_ticks_filters_time_window(
    data_client_builder,
    monkeypatch,
    instrument,
):
    client, _, http_client, _ = data_client_builder(monkeypatch)
    client._handle_data_response = MagicMock()

    request = SimpleNamespace(
        instrument_id=instrument.id,
        limit=200,
        start=datetime.fromtimestamp(1704067230, tz=UTC),
        end=datetime.fromtimestamp(1704067300, tz=UTC),
        data_type=TradeTick,
        id="req-1",
        params=None,
    )

    await client._request_trade_ticks(request)

    http_client.request_recent_trades.assert_awaited_once_with(1, limit=200)
    trades = client._handle_data_response.call_args.kwargs["data"]
    assert len(trades) == 1
    assert trades[0].trade_id.value == "trade-2"


@pytest.mark.asyncio
async def test_request_bars_filters_time_window(data_client_builder, monkeypatch):
    client, _, http_client, _ = data_client_builder(monkeypatch)
    client._handle_bars = MagicMock()

    bar_type = BarType.from_str("BTC-USDC-PERP.LIGHTER-1-MINUTE-LAST-EXTERNAL")
    request = SimpleNamespace(
        bar_type=bar_type,
        start=datetime.fromtimestamp(1704067230, tz=UTC),
        end=datetime.fromtimestamp(1704067400, tz=UTC),
        id="req-bars",
        params=None,
    )

    await client._request_bars(request)

    http_client.request_candles.assert_awaited_once_with(1, "1m")
    bars = client._handle_bars.call_args.args[1]
    assert len(bars) == 1
    assert str(bars[0].close) == "100025.00"


@pytest.mark.asyncio
async def test_request_funding_rates_filters_time_window(
    data_client_builder,
    monkeypatch,
    instrument,
):
    client, _, http_client, _ = data_client_builder(monkeypatch)
    client._handle_data_response = MagicMock()

    request = SimpleNamespace(
        instrument_id=instrument.id,
        start=datetime.fromtimestamp(1704067500, tz=UTC),
        end=datetime.fromtimestamp(1704067900, tz=UTC),
        data_type=FundingRateUpdate,
        id="req-funding",
        params=None,
    )

    await client._request_funding_rates(request)

    http_client.request_funding_rates.assert_awaited_once_with(1)
    updates = client._handle_data_response.call_args.kwargs["data"]
    assert len(updates) == 3
    assert updates[-1].rate == Decimal("0.0002")


@pytest.mark.asyncio
async def test_handle_msg_routes_colon_delimited_channels(
    data_client_builder,
    monkeypatch,
    instrument,
):
    client, _, _, _ = data_client_builder(monkeypatch)
    client._handle_data = MagicMock()

    await client._connect()
    try:
        client._handle_msg(
            json.dumps(
                {
                    "type": "update/ticker",
                    "channel": "ticker:1",
                    "ticker": {
                        "s": "BTC/USDC",
                        "a": {"price": "100001.00", "size": "0.2000"},
                        "b": {"price": "100000.00", "size": "0.1000"},
                    },
                },
            ),
        )

        client._handle_msg(
            json.dumps(
                {
                    "type": "update/trade",
                    "channel": "trade:1",
                    "trades": [
                        {
                            "trade_id": 12345,
                            "market_id": 1,
                            "size": "0.1000",
                            "price": "100005.00",
                            "is_maker_ask": False,
                            "timestamp": 1704067260000,
                        },
                    ],
                },
            ),
        )
    finally:
        await client._disconnect()

    handled = [call.args[0].__class__.__name__ for call in client._handle_data.call_args_list]
    assert "QuoteTick" in handled
    assert "TradeTick" in handled
