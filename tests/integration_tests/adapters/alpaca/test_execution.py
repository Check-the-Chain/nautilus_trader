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
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.alpaca.config import AlpacaExecClientConfig
from nautilus_trader.adapters.alpaca.execution import AlpacaExecutionClient
from nautilus_trader.execution.messages import BatchCancelOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import QueryAccount
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.model.enums import ContingencyType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import TrailingOffsetType
from nautilus_trader.model.enums import TriggerType
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import OrderListId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import LimitOrder
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.model.orders import OrderList
from nautilus_trader.model.orders import StopMarketOrder
from nautilus_trader.model.orders import TrailingStopMarketOrder
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from tests.integration_tests.adapters.alpaca.conftest import create_ws_mock
from tests.integration_tests.adapters.alpaca.conftest import make_alpaca_order
from tests.integration_tests.adapters.alpaca.conftest import make_fill_activity
from tests.integration_tests.adapters.alpaca.conftest import make_trade_update


@pytest.fixture
def exec_client_builder(
    event_loop,
    mock_http_client,
    msgbus,
    cache,
    live_clock,
    mock_instrument_provider,
):
    def builder(monkeypatch):
        ws_client = create_ws_mock()
        monkeypatch.setattr(
            "nautilus_trader.adapters.alpaca.execution.AlpacaWebSocketClient",
            lambda *args, **kwargs: ws_client,
        )
        monkeypatch.setattr(
            "nautilus_trader.adapters.alpaca.execution.AlpacaExecutionClient._await_account_registered",
            AsyncMock(),
        )

        mock_http_client.reset_mock()
        mock_instrument_provider.initialize.reset_mock()

        client = AlpacaExecutionClient(
            loop=event_loop,
            client=mock_http_client,
            msgbus=msgbus,
            cache=cache,
            clock=live_clock,
            instrument_provider=mock_instrument_provider,
            config=AlpacaExecClientConfig(api_key="key", api_secret="secret"),
            name=None,
        )
        return client, ws_client, mock_http_client, mock_instrument_provider

    return builder


def _make_limit_order(
    instrument_id,
    *,
    client_order_id: str,
    strategy_id,
    order_side: OrderSide = OrderSide.BUY,
    reduce_only: bool = False,
    quote_quantity: bool = False,
    time_in_force: TimeInForce = TimeInForce.GTC,
    contingency_type: ContingencyType | None = None,
    order_list_id: OrderListId | None = None,
    linked_order_ids: list[ClientOrderId] | None = None,
    parent_order_id: ClientOrderId | None = None,
) -> LimitOrder:
    kwargs: dict[str, object] = {}
    if contingency_type is not None:
        kwargs["contingency_type"] = contingency_type
    if order_list_id is not None:
        kwargs["order_list_id"] = order_list_id
    if linked_order_ids is not None:
        kwargs["linked_order_ids"] = linked_order_ids
    if parent_order_id is not None:
        kwargs["parent_order_id"] = parent_order_id

    return LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy_id,
        instrument_id=instrument_id,
        client_order_id=ClientOrderId(client_order_id),
        order_side=order_side,
        quantity=Quantity.from_int(10),
        price=Price.from_str("150.00"),
        time_in_force=time_in_force,
        reduce_only=reduce_only,
        quote_quantity=quote_quantity,
        init_id=TestIdStubs.uuid(),
        ts_init=0,
        **kwargs,
    )


def _make_market_order(
    instrument_id,
    *,
    client_order_id: str,
    strategy_id,
    order_side: OrderSide = OrderSide.BUY,
    reduce_only: bool = False,
    quote_quantity: bool = False,
    time_in_force: TimeInForce = TimeInForce.GTC,
    contingency_type: ContingencyType | None = None,
    order_list_id: OrderListId | None = None,
    linked_order_ids: list[ClientOrderId] | None = None,
    parent_order_id: ClientOrderId | None = None,
) -> MarketOrder:
    kwargs: dict[str, object] = {}
    if contingency_type is not None:
        kwargs["contingency_type"] = contingency_type
    if order_list_id is not None:
        kwargs["order_list_id"] = order_list_id
    if linked_order_ids is not None:
        kwargs["linked_order_ids"] = linked_order_ids
    if parent_order_id is not None:
        kwargs["parent_order_id"] = parent_order_id

    return MarketOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy_id,
        instrument_id=instrument_id,
        client_order_id=ClientOrderId(client_order_id),
        order_side=order_side,
        quantity=Quantity.from_int(10),
        time_in_force=time_in_force,
        reduce_only=reduce_only,
        quote_quantity=quote_quantity,
        init_id=TestIdStubs.uuid(),
        ts_init=0,
        **kwargs,
    )


def _make_stop_market_order(
    instrument_id,
    *,
    client_order_id: str,
    strategy_id,
    order_side: OrderSide,
    reduce_only: bool = False,
    contingency_type: ContingencyType | None = None,
    order_list_id: OrderListId | None = None,
    linked_order_ids: list[ClientOrderId] | None = None,
    parent_order_id: ClientOrderId | None = None,
) -> StopMarketOrder:
    kwargs: dict[str, object] = {}
    if contingency_type is not None:
        kwargs["contingency_type"] = contingency_type
    if order_list_id is not None:
        kwargs["order_list_id"] = order_list_id
    if linked_order_ids is not None:
        kwargs["linked_order_ids"] = linked_order_ids
    if parent_order_id is not None:
        kwargs["parent_order_id"] = parent_order_id

    return StopMarketOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy_id,
        instrument_id=instrument_id,
        client_order_id=ClientOrderId(client_order_id),
        order_side=order_side,
        quantity=Quantity.from_int(10),
        trigger_price=Price.from_str("145.00"),
        trigger_type=TriggerType.DEFAULT,
        reduce_only=reduce_only,
        init_id=TestIdStubs.uuid(),
        ts_init=0,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_connect_updates_account_and_listens(exec_client_builder, monkeypatch):
    client, ws_client, _, instrument_provider = exec_client_builder(monkeypatch)

    await client._connect()

    try:
        assert client.account_id.value == "ALPACA-paper-001"
        instrument_provider.initialize.assert_awaited_once()
        ws_client.connect.assert_awaited_once()
        ws_client.send_json.assert_any_await(
            {"action": "listen", "data": {"streams": ["trade_updates"]}},
        )
    finally:
        await client._disconnect()

    ws_client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_execution_disconnect_callback_reconnects_and_relists(exec_client_builder, monkeypatch):
    client, _, _, _ = exec_client_builder(monkeypatch)
    await client._connect()

    replacement_ws = create_ws_mock()
    monkeypatch.setattr(
        "nautilus_trader.adapters.alpaca.execution.AlpacaWebSocketClient",
        lambda *args, **kwargs: replacement_ws,
    )

    await client._handle_ws_disconnect(RuntimeError("boom"))
    assert client._reconnect_task is not None
    await client._reconnect_task

    replacement_ws.connect.assert_awaited_once()
    replacement_ws.send_json.assert_any_await(
        {"action": "listen", "data": {"streams": ["trade_updates"]}},
    )


@pytest.mark.asyncio
async def test_generate_order_status_report_by_client_order_id(
    exec_client_builder,
    monkeypatch,
):
    client, _, _, _ = exec_client_builder(monkeypatch)
    await client._connect()

    command = GenerateOrderStatusReport(
        instrument_id=None,
        client_order_id=ClientOrderId("O-001"),
        venue_order_id=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    report = await client.generate_order_status_report(command)

    assert report is not None
    assert report.client_order_id.value == "O-001"


@pytest.mark.asyncio
async def test_generate_order_status_report_loads_missing_instrument_by_symbol(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
):
    client, _, _, instrument_provider = exec_client_builder(monkeypatch)
    await client._connect()
    instrument_provider.instrument_for_symbol = MagicMock(return_value=None)

    command = GenerateOrderStatusReport(
        instrument_id=None,
        client_order_id=ClientOrderId("O-001"),
        venue_order_id=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    report = await client.generate_order_status_report(command)

    assert report is not None
    instrument_provider.load_async.assert_awaited_once_with(equity_instrument.id)


@pytest.mark.asyncio
async def test_generate_order_status_report_applies_cached_order_metadata(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
    cache,
):
    client, _, _, _ = exec_client_builder(monkeypatch)
    await client._connect()

    order = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-001",
        strategy_id=strategy.id,
        contingency_type=ContingencyType.OCO,
        order_list_id=OrderListId("OL-001"),
        linked_order_ids=[ClientOrderId("O-002")],
        parent_order_id=ClientOrderId("O-PARENT"),
    )
    cache.add_order(order, None)

    command = GenerateOrderStatusReport(
        instrument_id=None,
        client_order_id=order.client_order_id,
        venue_order_id=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    report = await client.generate_order_status_report(command)

    assert report is not None
    assert report.order_list_id == order.order_list_id
    assert report.linked_order_ids == list(order.linked_order_ids)
    assert report.parent_order_id == order.parent_order_id
    assert report.contingency_type == order.contingency_type


@pytest.mark.asyncio
async def test_generate_order_status_reports(exec_client_builder, monkeypatch, mock_http_client):
    client, _, _, _ = exec_client_builder(monkeypatch)
    await client._connect()
    mock_http_client.list_orders.return_value = [make_alpaca_order()]

    command = GenerateOrderStatusReports(
        instrument_id=None,
        start=None,
        end=None,
        open_only=False,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reports = await client.generate_order_status_reports(command)

    assert len(reports) == 1
    assert reports[0].venue_order_id.value == "order-001"
    assert mock_http_client.list_orders.await_args.kwargs["nested"] is True


@pytest.mark.asyncio
async def test_generate_order_status_reports_apply_cached_order_metadata(
    exec_client_builder,
    monkeypatch,
    strategy,
    cache,
    equity_instrument,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()

    order = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-001",
        strategy_id=strategy.id,
        contingency_type=ContingencyType.OCO,
        order_list_id=OrderListId("OL-002"),
        linked_order_ids=[ClientOrderId("O-003")],
        parent_order_id=ClientOrderId("O-PARENT-2"),
    )
    cache.add_order(order, None)
    http_client.list_orders.return_value = [make_alpaca_order()]

    command = GenerateOrderStatusReports(
        instrument_id=None,
        start=None,
        end=None,
        open_only=False,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reports = await client.generate_order_status_reports(command)

    assert len(reports) == 1
    assert reports[0].order_list_id == order.order_list_id
    assert reports[0].linked_order_ids == list(order.linked_order_ids)
    assert reports[0].parent_order_id == order.parent_order_id
    assert reports[0].contingency_type == order.contingency_type


@pytest.mark.asyncio
async def test_generate_order_status_reports_expand_nested_bracket_legs(
    exec_client_builder,
    monkeypatch,
    strategy,
    cache,
    equity_instrument,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()

    order_list_id = OrderListId("OL-REPORT-BRACKET")
    entry = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-REPORT-ENTRY",
        strategy_id=strategy.id,
        contingency_type=ContingencyType.OTO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-REPORT-SL"), ClientOrderId("O-REPORT-TP")],
    )
    stop_loss = _make_stop_market_order(
        equity_instrument.id,
        client_order_id="O-REPORT-SL",
        strategy_id=strategy.id,
        order_side=OrderSide.SELL,
        reduce_only=True,
        contingency_type=ContingencyType.OUO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-REPORT-TP")],
        parent_order_id=entry.client_order_id,
    )
    take_profit = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-REPORT-TP",
        strategy_id=strategy.id,
        order_side=OrderSide.SELL,
        reduce_only=True,
        contingency_type=ContingencyType.OUO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-REPORT-SL")],
        parent_order_id=entry.client_order_id,
    )
    cache.add_order(entry, None)
    cache.add_order(stop_loss, None)
    cache.add_order(take_profit, None)
    http_client.list_orders.return_value = [
        {
            **make_alpaca_order(
                client_order_id=entry.client_order_id.value,
                venue_order_id="order-parent",
                type_="limit",
                side="buy",
            ),
            "order_class": "bracket",
            "legs": [
                make_alpaca_order(
                    client_order_id="venue-stop",
                    venue_order_id="order-stop",
                    type_="stop",
                    side="sell",
                    limit_price=None,
                    stop_price="145.00",
                ),
                make_alpaca_order(
                    client_order_id="venue-tp",
                    venue_order_id="order-tp",
                    type_="limit",
                    side="sell",
                ),
            ],
        },
    ]

    command = GenerateOrderStatusReports(
        instrument_id=None,
        start=None,
        end=None,
        open_only=False,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reports = await client.generate_order_status_reports(command)

    assert len(reports) == 3
    reports_by_client_order_id = {report.client_order_id: report for report in reports}
    assert set(reports_by_client_order_id) == {
        entry.client_order_id,
        stop_loss.client_order_id,
        take_profit.client_order_id,
    }
    assert reports_by_client_order_id[entry.client_order_id].order_list_id == order_list_id
    assert reports_by_client_order_id[stop_loss.client_order_id].parent_order_id == entry.client_order_id
    assert reports_by_client_order_id[take_profit.client_order_id].parent_order_id == entry.client_order_id


@pytest.mark.asyncio
async def test_generate_order_status_reports_paginate_using_until_cursor(
    exec_client_builder,
    monkeypatch,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    first_page = [
        make_alpaca_order(
            venue_order_id=f"order-{i:03d}",
            client_order_id=f"O-{i:03d}",
            submitted_at=f"2026-03-09T10:00:{i % 60:02d}Z",
            updated_at=f"2026-03-09T10:01:{i % 60:02d}Z",
        )
        for i in range(500)
    ]
    second_page = [
        make_alpaca_order(
            venue_order_id="order-500",
            client_order_id="O-500",
            submitted_at="2026-03-09T09:59:59Z",
            updated_at="2026-03-09T10:00:59Z",
        ),
    ]
    http_client.list_orders.side_effect = [first_page, second_page]

    command = GenerateOrderStatusReports(
        instrument_id=None,
        start=None,
        end=None,
        open_only=False,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reports = await client.generate_order_status_reports(command)

    assert len(reports) == 501
    assert http_client.list_orders.await_args_list[0].kwargs["direction"] == "desc"
    assert http_client.list_orders.await_args_list[0].kwargs["nested"] is True
    assert http_client.list_orders.await_args_list[1].kwargs["until"] == first_page[-1]["submitted_at"]


@pytest.mark.asyncio
async def test_generate_order_status_reports_handles_failure(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    http_client.list_orders.side_effect = RuntimeError("boom")

    command = GenerateOrderStatusReports(
        instrument_id=None,
        start=None,
        end=None,
        open_only=False,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reports = await client.generate_order_status_reports(command)

    assert reports == []


@pytest.mark.asyncio
async def test_generate_fill_reports_filters_results(exec_client_builder, monkeypatch, equity_instrument, cache):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    cache.add_venue_order_id(ClientOrderId("O-001"), VenueOrderId("order-001"))
    http_client.get_activities.return_value = [make_fill_activity(symbol="AAPL")]

    command = GenerateFillReports(
        instrument_id=equity_instrument.id,
        venue_order_id=VenueOrderId("order-001"),
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reports = await client.generate_fill_reports(command)

    assert len(reports) == 1
    assert reports[0].client_order_id == ClientOrderId("O-001")


@pytest.mark.asyncio
async def test_generate_fill_reports_loads_missing_instrument_by_symbol(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
):
    client, _, http_client, instrument_provider = exec_client_builder(monkeypatch)
    await client._connect()
    instrument_provider.instrument_for_symbol = MagicMock(return_value=None)
    http_client.get_activities.return_value = [make_fill_activity(symbol="AAPL")]

    command = GenerateFillReports(
        instrument_id=equity_instrument.id,
        venue_order_id=None,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reports = await client.generate_fill_reports(command)

    assert len(reports) == 1
    instrument_provider.load_async.assert_awaited_once_with(equity_instrument.id)


@pytest.mark.asyncio
async def test_generate_fill_reports_paginate_by_last_activity_id(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    first_page = [
        make_fill_activity(
            activity_id=f"fill-{i:03d}",
            order_id=f"order-{i:03d}",
        )
        for i in range(100)
    ]
    second_page = [
        make_fill_activity(
            activity_id="fill-100",
            order_id="order-100",
        ),
    ]
    http_client.get_activities.side_effect = [first_page, second_page]

    command = GenerateFillReports(
        instrument_id=None,
        venue_order_id=None,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reports = await client.generate_fill_reports(command)

    assert len(reports) == 101
    assert http_client.get_activities.await_args_list[0].kwargs["direction"] == "desc"
    assert http_client.get_activities.await_args_list[1].kwargs["page_token"] == first_page[-1]["id"]


@pytest.mark.asyncio
async def test_generate_fill_reports_handles_failure(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    http_client.get_activities.side_effect = RuntimeError("boom")

    command = GenerateFillReports(
        instrument_id=None,
        venue_order_id=None,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reports = await client.generate_fill_reports(command)

    assert reports == []


@pytest.mark.asyncio
async def test_generate_position_status_reports_returns_flat_for_missing_instrument_position(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
):
    client, _, _, _ = exec_client_builder(monkeypatch)
    await client._connect()
    client._client.get_positions.return_value = []

    command = GeneratePositionStatusReports(
        instrument_id=equity_instrument.id,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reports = await client.generate_position_status_reports(command)

    assert len(reports) == 1
    assert reports[0].position_side == PositionSide.FLAT


@pytest.mark.asyncio
async def test_generate_position_status_reports_loads_missing_instrument_by_symbol(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
):
    client, _, http_client, instrument_provider = exec_client_builder(monkeypatch)
    await client._connect()
    http_client.get_positions.return_value = [
        {
            "symbol": "AAPL",
            "qty": "2",
            "side": "long",
            "asset_id": "asset-001",
            "avg_entry_price": "150.00",
            "updated_at": "2026-03-09T10:00:02Z",
        },
    ]
    instrument_provider.instrument_for_symbol = MagicMock(return_value=None)

    command = GeneratePositionStatusReports(
        instrument_id=equity_instrument.id,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reports = await client.generate_position_status_reports(command)

    assert len(reports) == 1
    instrument_provider.load_async.assert_awaited_once_with(equity_instrument.id)


@pytest.mark.asyncio
async def test_generate_position_status_reports_handles_failure(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    http_client.get_positions.side_effect = RuntimeError("boom")

    command = GeneratePositionStatusReports(
        instrument_id=None,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reports = await client.generate_position_status_reports(command)

    assert reports == []


@pytest.mark.asyncio
async def test_submit_limit_order(exec_client_builder, monkeypatch, equity_instrument, strategy):
    client, _, _, _ = exec_client_builder(monkeypatch)
    await client._connect()
    client.generate_order_accepted = MagicMock()

    order = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-001",
        strategy_id=strategy.id,
    )
    command = SubmitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        order=order,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._submit_order(command)

    client._client.submit_order.assert_awaited_once()
    payload = client._client.submit_order.call_args.args[0]
    assert payload["symbol"] == "AAPL"
    assert payload["type"] == "limit"
    assert payload["qty"] == "10"
    client.generate_order_accepted.assert_called_once()


@pytest.mark.asyncio
async def test_submit_limit_order_timeout_leaves_submitted(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    client.generate_order_submitted = MagicMock()
    client.generate_order_rejected = MagicMock()
    http_client.submit_order.side_effect = TimeoutError("request timed out")

    order = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-TIMEOUT-001",
        strategy_id=strategy.id,
    )
    command = SubmitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        order=order,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._submit_order(command)

    client.generate_order_submitted.assert_called_once()
    client.generate_order_rejected.assert_not_called()


@pytest.mark.asyncio
async def test_submit_limit_order_http_504_leaves_submitted(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    client.generate_order_submitted = MagicMock()
    client.generate_order_rejected = MagicMock()
    http_client.submit_order.side_effect = RuntimeError("Alpaca request failed [504] gateway timeout")

    order = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-TIMEOUT-504",
        strategy_id=strategy.id,
    )
    command = SubmitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        order=order,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._submit_order(command)

    client.generate_order_submitted.assert_called_once()
    client.generate_order_rejected.assert_not_called()


@pytest.mark.asyncio
async def test_submit_order_list_submits_plain_orders(exec_client_builder, monkeypatch, equity_instrument, strategy):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    client.generate_order_denied = MagicMock()

    order_1 = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-LIST-1",
        strategy_id=strategy.id,
    )
    order_2 = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-LIST-2",
        strategy_id=strategy.id,
    )
    command = SubmitOrderList(
        trader_id=order_1.trader_id,
        strategy_id=strategy.id,
        order_list=OrderList(
            order_list_id=OrderListId("OL-PLAIN"),
            orders=[order_1, order_2],
        ),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._submit_order_list(command)

    assert http_client.submit_order.await_count == 2
    client.generate_order_denied.assert_not_called()


@pytest.mark.asyncio
async def test_submit_bracket_order_list_timeout_leaves_submitted(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    client.generate_order_submitted = MagicMock()
    client.generate_order_rejected = MagicMock()
    http_client.submit_order.side_effect = TimeoutError("request timed out")

    order_list_id = OrderListId("OL-BRACKET-TIMEOUT")
    entry = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-BRACKET-TIMEOUT-ENTRY",
        strategy_id=strategy.id,
        contingency_type=ContingencyType.OTO,
        order_list_id=order_list_id,
        linked_order_ids=[
            ClientOrderId("O-BRACKET-TIMEOUT-SL"),
            ClientOrderId("O-BRACKET-TIMEOUT-TP"),
        ],
    )
    stop_loss = _make_stop_market_order(
        equity_instrument.id,
        client_order_id="O-BRACKET-TIMEOUT-SL",
        strategy_id=strategy.id,
        order_side=OrderSide.SELL,
        reduce_only=True,
        contingency_type=ContingencyType.OUO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-BRACKET-TIMEOUT-TP")],
        parent_order_id=entry.client_order_id,
    )
    take_profit = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-BRACKET-TIMEOUT-TP",
        strategy_id=strategy.id,
        order_side=OrderSide.SELL,
        reduce_only=True,
        contingency_type=ContingencyType.OUO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-BRACKET-TIMEOUT-SL")],
        parent_order_id=entry.client_order_id,
    )
    command = SubmitOrderList(
        trader_id=entry.trader_id,
        strategy_id=strategy.id,
        order_list=OrderList(order_list_id=order_list_id, orders=[entry, stop_loss, take_profit]),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._submit_order_list(command)

    assert client.generate_order_submitted.call_count == 3
    client.generate_order_rejected.assert_not_called()


@pytest.mark.asyncio
async def test_submit_order_list_denies_contingent_orders(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    client.generate_order_denied = MagicMock()

    order_list_id = OrderListId("OL-CONTINGENT")
    order_1 = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-CONT-1",
        strategy_id=strategy.id,
        contingency_type=ContingencyType.OCO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-CONT-2")],
    )
    order_2 = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-CONT-2",
        strategy_id=strategy.id,
        contingency_type=ContingencyType.OCO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-CONT-1")],
    )
    command = SubmitOrderList(
        trader_id=order_1.trader_id,
        strategy_id=strategy.id,
        order_list=OrderList(order_list_id=order_list_id, orders=[order_1, order_2]),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._submit_order_list(command)

    assert client.generate_order_denied.call_count == 2
    denied_client_order_ids = {
        call.kwargs["client_order_id"] for call in client.generate_order_denied.call_args_list
    }
    assert denied_client_order_ids == {order_1.client_order_id, order_2.client_order_id}
    http_client.submit_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_submit_bracket_order_list_uses_advanced_order_payload(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    client.generate_order_accepted = MagicMock()

    order_list_id = OrderListId("OL-BRACKET")
    entry = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-BRACKET-ENTRY",
        strategy_id=strategy.id,
        contingency_type=ContingencyType.OTO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-BRACKET-SL"), ClientOrderId("O-BRACKET-TP")],
    )
    stop_loss = _make_stop_market_order(
        equity_instrument.id,
        client_order_id="O-BRACKET-SL",
        strategy_id=strategy.id,
        order_side=OrderSide.SELL,
        reduce_only=True,
        contingency_type=ContingencyType.OUO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-BRACKET-TP")],
        parent_order_id=entry.client_order_id,
    )
    take_profit = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-BRACKET-TP",
        strategy_id=strategy.id,
        order_side=OrderSide.SELL,
        reduce_only=True,
        contingency_type=ContingencyType.OUO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-BRACKET-SL")],
        parent_order_id=entry.client_order_id,
    )
    http_client.submit_order.return_value = {
        **make_alpaca_order(
            client_order_id="venue-parent",
            venue_order_id="order-parent",
            type_="limit",
            side="buy",
        ),
        "order_class": "bracket",
        "legs": [
            make_alpaca_order(
                client_order_id="venue-stop",
                venue_order_id="order-stop",
                type_="stop",
                side="sell",
                limit_price=None,
                stop_price="145.00",
            ),
            make_alpaca_order(
                client_order_id="venue-tp",
                venue_order_id="order-tp",
                type_="limit",
                side="sell",
            ),
        ],
    }
    command = SubmitOrderList(
        trader_id=entry.trader_id,
        strategy_id=strategy.id,
        order_list=OrderList(order_list_id=order_list_id, orders=[entry, stop_loss, take_profit]),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._submit_order_list(command)

    payload = http_client.submit_order.await_args.args[0]
    assert payload["order_class"] == "bracket"
    assert payload["take_profit"] == {"limit_price": "150.00"}
    assert payload["stop_loss"] == {"stop_price": "145.00"}
    assert client.generate_order_accepted.call_count == 3


@pytest.mark.asyncio
async def test_submit_oto_order_list_uses_take_profit_payload(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    client.generate_order_accepted = MagicMock()

    order_list_id = OrderListId("OL-OTO")
    entry = _make_market_order(
        equity_instrument.id,
        client_order_id="O-OTO-ENTRY",
        strategy_id=strategy.id,
        contingency_type=ContingencyType.OTO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-OTO-TP")],
    )
    take_profit = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-OTO-TP",
        strategy_id=strategy.id,
        order_side=OrderSide.SELL,
        reduce_only=True,
        order_list_id=order_list_id,
        parent_order_id=entry.client_order_id,
    )
    http_client.submit_order.return_value = {
        **make_alpaca_order(
            client_order_id="venue-parent",
            venue_order_id="order-parent",
            type_="market",
            side="buy",
            limit_price=None,
        ),
        "order_class": "oto",
        "legs": [
            make_alpaca_order(
                client_order_id="venue-tp",
                venue_order_id="order-tp",
                type_="limit",
                side="sell",
            ),
        ],
    }
    command = SubmitOrderList(
        trader_id=entry.trader_id,
        strategy_id=strategy.id,
        order_list=OrderList(order_list_id=order_list_id, orders=[entry, take_profit]),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._submit_order_list(command)

    payload = http_client.submit_order.await_args.args[0]
    assert payload["order_class"] == "oto"
    assert payload["take_profit"] == {"limit_price": "150.00"}
    assert client.generate_order_accepted.call_count == 2


@pytest.mark.asyncio
async def test_submit_oto_order_list_denies_same_side_child(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    client.generate_order_denied = MagicMock()

    order_list_id = OrderListId("OL-OTO-SAME-SIDE")
    entry = _make_market_order(
        equity_instrument.id,
        client_order_id="O-OTO-SAME-SIDE-ENTRY",
        strategy_id=strategy.id,
        contingency_type=ContingencyType.OTO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-OTO-SAME-SIDE-TP")],
    )
    take_profit = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-OTO-SAME-SIDE-TP",
        strategy_id=strategy.id,
        order_side=OrderSide.BUY,
        reduce_only=True,
        order_list_id=order_list_id,
        parent_order_id=entry.client_order_id,
    )
    command = SubmitOrderList(
        trader_id=entry.trader_id,
        strategy_id=strategy.id,
        order_list=OrderList(order_list_id=order_list_id, orders=[entry, take_profit]),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._submit_order_list(command)

    denied_reasons = {
        call.kwargs["reason"] for call in client.generate_order_denied.call_args_list
    }
    assert denied_reasons == {"ALPACA_ADVANCED_CHILD_SIDE_INVALID"}
    http_client.submit_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_submit_oto_order_list_denies_non_reduce_only_child(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    client.generate_order_denied = MagicMock()

    order_list_id = OrderListId("OL-OTO-NON-REDUCE")
    entry = _make_market_order(
        equity_instrument.id,
        client_order_id="O-OTO-NON-REDUCE-ENTRY",
        strategy_id=strategy.id,
        contingency_type=ContingencyType.OTO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-OTO-NON-REDUCE-TP")],
    )
    take_profit = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-OTO-NON-REDUCE-TP",
        strategy_id=strategy.id,
        order_side=OrderSide.SELL,
        reduce_only=False,
        order_list_id=order_list_id,
        parent_order_id=entry.client_order_id,
    )
    command = SubmitOrderList(
        trader_id=entry.trader_id,
        strategy_id=strategy.id,
        order_list=OrderList(order_list_id=order_list_id, orders=[entry, take_profit]),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._submit_order_list(command)

    denied_reasons = {
        call.kwargs["reason"] for call in client.generate_order_denied.call_args_list
    }
    assert denied_reasons == {"ALPACA_ADVANCED_CHILD_REDUCE_ONLY_REQUIRED"}
    http_client.submit_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_submit_oco_order_list_uses_stop_loss_payload(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    await client._connect()
    client.generate_order_accepted = MagicMock()

    order_list_id = OrderListId("OL-OCO")
    take_profit = _make_limit_order(
        equity_instrument.id,
        client_order_id="O-OCO-TP",
        strategy_id=strategy.id,
        order_side=OrderSide.SELL,
        reduce_only=True,
        contingency_type=ContingencyType.OCO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-OCO-SL")],
    )
    stop_loss = _make_stop_market_order(
        equity_instrument.id,
        client_order_id="O-OCO-SL",
        strategy_id=strategy.id,
        order_side=OrderSide.SELL,
        reduce_only=True,
        contingency_type=ContingencyType.OCO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-OCO-TP")],
    )
    http_client.submit_order.return_value = {
        **make_alpaca_order(
            client_order_id="venue-parent",
            venue_order_id="order-parent",
            type_="limit",
            side="sell",
        ),
        "order_class": "oco",
        "legs": [
            make_alpaca_order(
                client_order_id="venue-stop",
                venue_order_id="order-stop",
                type_="stop",
                side="sell",
                limit_price=None,
                stop_price="145.00",
            ),
        ],
    }
    command = SubmitOrderList(
        trader_id=take_profit.trader_id,
        strategy_id=strategy.id,
        order_list=OrderList(order_list_id=order_list_id, orders=[take_profit, stop_loss]),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._submit_order_list(command)

    payload = http_client.submit_order.await_args.args[0]
    assert payload["order_class"] == "oco"
    assert payload["stop_loss"] == {"stop_price": "145.00"}
    assert "take_profit" not in payload
    assert client.generate_order_accepted.call_count == 2


def test_build_submit_payload_uses_notional_for_quote_quantity(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
):
    client, _, _, _ = exec_client_builder(monkeypatch)

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-NOTIONAL-1"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("100.00"),
        price=Price.from_str("150.00"),
        quote_quantity=True,
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    command = SubmitOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        order=order,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    payload = client._build_submit_payload(command, equity_instrument)

    assert payload["symbol"] == "AAPL"
    assert payload["notional"] == "100.00"
    assert "qty" not in payload


def test_build_submit_payload_uses_equity_trailing_stop_basis_points(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
):
    client, _, _, _ = exec_client_builder(monkeypatch)

    order = TrailingStopMarketOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-TRAIL-1"),
        order_side=OrderSide.SELL,
        quantity=Quantity.from_int(10),
        trigger_price=None,
        trigger_type=TriggerType.DEFAULT,
        trailing_offset=Decimal(125),
        trailing_offset_type=TrailingOffsetType.BASIS_POINTS,
        time_in_force=TimeInForce.GTC,
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    command = SubmitOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        order=order,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    payload = client._build_submit_payload(command, equity_instrument)

    assert payload["symbol"] == "AAPL"
    assert payload["trail_percent"] == "1.25"


def test_validate_order_rejects_crypto_trailing_stop(exec_client_builder, monkeypatch, crypto_instrument, strategy):
    client, _, _, _ = exec_client_builder(monkeypatch)
    order = TrailingStopMarketOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=crypto_instrument.id,
        client_order_id=ClientOrderId("O-CRYPTO-TRAIL"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("0.1000"),
        trigger_price=None,
        trigger_type=TriggerType.DEFAULT,
        trailing_offset=Decimal(1),
        trailing_offset_type=TrailingOffsetType.PRICE,
        time_in_force=TimeInForce.GTC,
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reason = client._validate_order(order, crypto_instrument)

    assert reason == "ALPACA_ORDER_TYPE_UNSUPPORTED:TRAILING_STOP_MARKET"


def test_validate_order_rejects_option_stop_market(
    exec_client_builder,
    monkeypatch,
    option_instrument,
    strategy,
):
    client, _, _, _ = exec_client_builder(monkeypatch)
    order = _make_stop_market_order(
        option_instrument.id,
        client_order_id="O-OPT-STOP",
        strategy_id=strategy.id,
        order_side=OrderSide.BUY,
    )

    reason = client._validate_order(order, option_instrument)

    assert reason == "ALPACA_ORDER_TYPE_UNSUPPORTED:STOP_MARKET"


def test_validate_order_rejects_option_notional(
    exec_client_builder,
    monkeypatch,
    option_instrument,
    strategy,
):
    client, _, _, _ = exec_client_builder(monkeypatch)
    order = _make_market_order(
        option_instrument.id,
        client_order_id="O-OPT-NOTIONAL",
        strategy_id=strategy.id,
        quote_quantity=True,
    )

    reason = client._validate_order(order, option_instrument)

    assert reason == "ALPACA_OPTION_NOTIONAL_UNSUPPORTED"


def test_validate_order_rejects_option_non_day_tif(
    exec_client_builder,
    monkeypatch,
    option_instrument,
    strategy,
):
    client, _, _, _ = exec_client_builder(monkeypatch)
    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=option_instrument.id,
        client_order_id=ClientOrderId("O-OPT-GTC"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(1),
        price=Price.from_str("5.00"),
        time_in_force=TimeInForce.GTC,
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reason = client._validate_order(order, option_instrument)

    assert reason == "ALPACA_OPTION_TIF_UNSUPPORTED"


@pytest.mark.asyncio
async def test_submit_option_order_rejects_extended_hours(
    exec_client_builder,
    monkeypatch,
    option_instrument,
    strategy,
):
    client, _, _, instrument_provider = exec_client_builder(monkeypatch)
    instrument_provider.find.side_effect = (
        lambda instrument_id: option_instrument if instrument_id == option_instrument.id else None
    )
    client.generate_order_denied = MagicMock()
    order = _make_limit_order(
        option_instrument.id,
        client_order_id="O-OPT-EXT",
        strategy_id=strategy.id,
        time_in_force=TimeInForce.DAY,
    )

    await client._submit_order(
        SubmitOrder(
            trader_id=order.trader_id,
            strategy_id=order.strategy_id,
            order=order,
            command_id=TestIdStubs.uuid(),
            ts_init=0,
            params={"extended_hours": True},
        ),
    )

    client.generate_order_denied.assert_called_once()
    assert client.generate_order_denied.call_args.kwargs["reason"] == "ALPACA_EXTENDED_HOURS_EQUITIES_ONLY"


@pytest.mark.asyncio
async def test_modify_order(exec_client_builder, monkeypatch, equity_instrument, strategy, cache):
    client, _, _, _ = exec_client_builder(monkeypatch)
    await client._connect()
    client.generate_order_updated = MagicMock()

    cached_order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-001"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(10),
        price=Price.from_str("150.00"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(cached_order, None)
    cache.add_venue_order_id(cached_order.client_order_id, VenueOrderId("order-001"))

    command = ModifyOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-001"),
        venue_order_id=None,
        quantity=Quantity.from_int(12),
        price=Price.from_str("151.00"),
        trigger_price=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._modify_order(command)

    client._client.replace_order.assert_awaited_once()
    client.generate_order_updated.assert_called_once()


@pytest.mark.asyncio
async def test_modify_order_uses_cached_instrument_when_provider_misses(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
    cache,
):
    client, _, _, instrument_provider = exec_client_builder(monkeypatch)
    await client._connect()
    client.generate_order_updated = MagicMock()
    instrument_provider.find = MagicMock(return_value=None)

    cached_order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-001"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(10),
        price=Price.from_str("150.00"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(cached_order, None)
    cache.add_venue_order_id(cached_order.client_order_id, VenueOrderId("order-001"))

    command = ModifyOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-001"),
        venue_order_id=None,
        quantity=Quantity.from_int(12),
        price=Price.from_str("151.00"),
        trigger_price=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._modify_order(command)

    client.generate_order_updated.assert_called_once()


@pytest.mark.asyncio
async def test_cancel_order_calls_http(exec_client_builder, monkeypatch, equity_instrument):
    client, _, http_client, _ = exec_client_builder(monkeypatch)

    command = CancelOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-001"),
        venue_order_id=VenueOrderId("order-001"),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._cancel_order(command)

    http_client.cancel_order.assert_awaited_once_with("order-001")


@pytest.mark.asyncio
async def test_cancel_all_orders_without_filters_calls_http(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)

    command = SimpleNamespace(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=None,
        order_side=OrderSide.NO_ORDER_SIDE,
        ts_init=0,
        command_id=TestIdStubs.uuid(),
    )

    await client._cancel_all_orders(command)

    http_client.cancel_all_orders.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancel_all_orders_falls_back_to_cached_orders(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    http_client.cancel_all_orders.side_effect = RuntimeError("venue unavailable")

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-CANCEL-ALL"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(10),
        price=Price.from_str("150.00"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    order.apply(TestEventStubs.order_accepted(order, venue_order_id=VenueOrderId("order-001")))
    monkeypatch.setattr(client, "_open_orders_for_cancel_all", MagicMock(return_value=[order]))

    command = SimpleNamespace(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=None,
        order_side=OrderSide.NO_ORDER_SIDE,
        ts_init=0,
        command_id=TestIdStubs.uuid(),
    )

    await client._cancel_all_orders(command)

    http_client.cancel_all_orders.assert_awaited_once()
    http_client.cancel_order.assert_awaited_once_with("order-001")


@pytest.mark.asyncio
async def test_batch_cancel_orders_fans_out(exec_client_builder, monkeypatch, equity_instrument):
    client, _, _, _ = exec_client_builder(monkeypatch)
    cancel_1 = CancelOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-001"),
        venue_order_id=VenueOrderId("order-001"),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cancel_2 = CancelOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-002"),
        venue_order_id=VenueOrderId("order-002"),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    client._cancel_order = AsyncMock()

    command = BatchCancelOrders(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=equity_instrument.id,
        cancels=[cancel_1, cancel_2],
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._batch_cancel_orders(command)

    assert client._cancel_order.await_count == 2


@pytest.mark.asyncio
async def test_query_account_refreshes_state(exec_client_builder, monkeypatch):
    client, _, _, _ = exec_client_builder(monkeypatch)
    client._update_account_state = AsyncMock()

    command = QueryAccount(
        trader_id=TestIdStubs.trader_id(),
        account_id=TestIdStubs.account_id(),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._query_account(command)

    client._update_account_state.assert_awaited_once()


def test_handle_msg_generates_accept(exec_client_builder, monkeypatch, equity_instrument, strategy, cache):
    client, _, _, _ = exec_client_builder(monkeypatch)
    client.generate_order_accepted = MagicMock()

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-001"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(10),
        price=Price.from_str("150.00"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(order, None)

    client._handle_msg(make_trade_update(event="accepted"))

    client.generate_order_accepted.assert_called_once()


def test_handle_msg_replaced_generates_update(exec_client_builder, monkeypatch, equity_instrument, strategy, cache):
    client, _, _, _ = exec_client_builder(monkeypatch)
    client.generate_order_updated = MagicMock()

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-001"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(10),
        price=Price.from_str("150.00"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(order, None)
    cache.add_venue_order_id(order.client_order_id, VenueOrderId("order-001"))

    client._handle_msg(make_trade_update(event="replaced"))

    client.generate_order_updated.assert_called_once()


def test_handle_msg_uses_cached_instrument_when_provider_misses(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
    cache,
):
    client, _, _, instrument_provider = exec_client_builder(monkeypatch)
    client.generate_order_canceled = MagicMock()
    instrument_provider.instrument_for_symbol = MagicMock(return_value=None)
    instrument_provider.find = MagicMock(return_value=None)

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-001"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(10),
        price=Price.from_str("150.00"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(order, None)
    cache.add_venue_order_id(order.client_order_id, VenueOrderId("order-001"))

    client._handle_msg(make_trade_update(event="canceled"))

    client.generate_order_canceled.assert_called_once()


def test_handle_msg_replaced_marks_modified_venue_order_id(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
    cache,
):
    client, _, _, _ = exec_client_builder(monkeypatch)
    client.generate_order_updated = MagicMock()

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-001"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(10),
        price=Price.from_str("150.00"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(order, None)
    cache.add_venue_order_id(order.client_order_id, VenueOrderId("order-000"))

    client._handle_msg(
        make_trade_update(event="replaced", venue_order_id="order-001"),
    )

    assert client.generate_order_updated.call_args.kwargs["venue_order_id_modified"] is True


def test_handle_msg_fill_deduplicates(exec_client_builder, monkeypatch, equity_instrument, strategy, cache):
    client, _, _, _ = exec_client_builder(monkeypatch)
    client.generate_order_accepted = MagicMock()
    client.generate_order_filled = MagicMock()

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-001"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(10),
        price=Price.from_str("150.00"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(order, None)

    payload = make_trade_update(event="fill", execution_id="exec-001")
    client._handle_msg(payload)
    client._handle_msg(payload)

    client.generate_order_accepted.assert_called_once()
    client.generate_order_filled.assert_called_once()


def test_handle_msg_fill_processed_trade_cache_is_bounded(
    exec_client_builder,
    monkeypatch,
    equity_instrument,
    strategy,
    cache,
):
    client, _, _, _ = exec_client_builder(monkeypatch)
    client._processed_trade_id_limit = 1
    client.generate_order_accepted = MagicMock()
    client.generate_order_filled = MagicMock()

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-001"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(10),
        price=Price.from_str("150.00"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(order, None)

    client._handle_msg(make_trade_update(event="fill", execution_id="exec-001"))
    client._handle_msg(make_trade_update(event="fill", execution_id="exec-002"))

    assert client._processed_trade_ids == {"exec-002"}
    assert list(client._processed_trade_queue) == ["exec-002"]


def test_handle_msg_falls_back_to_venue_order_id(exec_client_builder, monkeypatch, equity_instrument, strategy, cache):
    client, _, _, _ = exec_client_builder(monkeypatch)
    client.generate_order_canceled = MagicMock()

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-001"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(10),
        price=Price.from_str("150.00"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(order, None)
    cache.add_venue_order_id(order.client_order_id, VenueOrderId("order-001"))

    client._handle_msg(
        make_trade_update(event="canceled", client_order_id="O-OTHER"),
    )

    client.generate_order_canceled.assert_called_once()


def test_handle_msg_rejected_generates_rejection(exec_client_builder, monkeypatch, equity_instrument, strategy, cache):
    client, _, _, _ = exec_client_builder(monkeypatch)
    client.generate_order_rejected = MagicMock()

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=strategy.id,
        instrument_id=equity_instrument.id,
        client_order_id=ClientOrderId("O-001"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(10),
        price=Price.from_str("150.00"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(order, None)
    cache.add_venue_order_id(order.client_order_id, VenueOrderId("order-001"))

    client._handle_msg(
        make_trade_update(event="rejected", reason="bad order"),
    )

    client.generate_order_rejected.assert_called_once()


@pytest.mark.asyncio
async def test_generate_mass_status_returns_status(exec_client_builder, monkeypatch):
    client, _, _, _ = exec_client_builder(monkeypatch)
    monkeypatch.setattr(client, "generate_order_status_reports", AsyncMock(return_value=[]))
    monkeypatch.setattr(client, "generate_fill_reports", AsyncMock(return_value=[]))
    monkeypatch.setattr(client, "generate_position_status_reports", AsyncMock(return_value=[]))

    mass_status = await client.generate_mass_status(lookback_mins=5)

    assert mass_status is not None
    assert mass_status.account_id == client.account_id
