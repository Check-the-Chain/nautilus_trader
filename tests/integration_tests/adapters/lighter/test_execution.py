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
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.lighter.config import LighterExecClientConfig
from nautilus_trader.adapters.lighter.constants import LIGHTER_MARGIN_MODE_CROSS
from nautilus_trader.adapters.lighter.constants import LIGHTER_UPDATE_MARGIN_ADD
from nautilus_trader.adapters.lighter.execution import LighterExecutionClient
from nautilus_trader.adapters.lighter.parsing import order_report_from_lighter
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.model.enums import ContingencyType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import OrderListId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import LimitOrder
from nautilus_trader.model.orders import OrderList
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from tests.integration_tests.adapters.lighter.conftest import _create_ws_mock


@pytest.fixture
def exec_client_builder(
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
            "nautilus_trader.adapters.lighter.execution.nautilus_pyo3.LighterWebSocketClient",
            lambda *args, **kwargs: next(ws_iter),
        )

        mock_http_client.reset_mock()
        mock_instrument_provider.initialize.reset_mock()

        config = LighterExecClientConfig(
            account_index=7,
            private_key="0xdeadbeef",
            api_key_index=3,
            testnet=False,
            **(config_kwargs or {}),
        )

        client = LighterExecutionClient(
            loop=event_loop,
            client=mock_http_client,
            msgbus=msgbus,
            cache=cache,
            clock=live_clock,
            instrument_provider=mock_instrument_provider,
            config=config,
            name=None,
        )

        client._client_order_index_to_id[777] = ClientOrderId("O-777")
        client._client_order_id_to_index[ClientOrderId("O-777")] = 777

        return client, ws_client, mock_http_client, mock_instrument_provider

    return builder


def _make_limit_order(
    instrument_id,
    client_order_id: str = "O-123456",
    *,
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
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument_id,
        client_order_id=ClientOrderId(client_order_id),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("0.1000"),
        price=Price.from_str("100000.00"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_connect_success(exec_client_builder, monkeypatch):
    client, ws_client, http_client, instrument_provider = exec_client_builder(monkeypatch)
    client.generate_account_state = MagicMock()

    await client._connect()

    try:
        instrument_provider.initialize.assert_awaited_once()
        http_client.create_auth_token.assert_awaited_once()
        http_client.request_account.assert_awaited_once()
        ws_client.connect.assert_awaited_once()
        ws_client.subscribe_account_all.assert_awaited_once_with(7)
        ws_client.subscribe_account_all_orders.assert_awaited_once_with(7)
        ws_client.subscribe_account_all_positions.assert_awaited_once_with(7)
        ws_client.subscribe_account_all_trades.assert_awaited_once_with(7)
        ws_client.subscribe_account_all_assets.assert_awaited_once_with(7)
        ws_client.subscribe_user_stats.assert_awaited_once_with(7)
    finally:
        await client._disconnect()

    ws_client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_disconnect_success(exec_client_builder, monkeypatch):
    client, ws_client, _, _ = exec_client_builder(monkeypatch)
    client.generate_account_state = MagicMock()

    await client._connect()
    await client._disconnect()

    ws_client.close.assert_awaited_once()


def test_account_id_set_on_initialization(exec_client_builder, monkeypatch):
    client, _, _, _ = exec_client_builder(monkeypatch)

    assert client.account_id.value == "LIGHTER-7"


@pytest.mark.asyncio
async def test_generate_order_status_reports_paginates_inactive_orders(
    exec_client_builder,
    monkeypatch,
    instrument,
    cache,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    cached_order = _make_limit_order(
        instrument.id,
        client_order_id="O-777",
        contingency_type=ContingencyType.OCO,
        order_list_id=OrderListId("OL-777"),
        linked_order_ids=[ClientOrderId("O-778")],
        parent_order_id=ClientOrderId("O-PARENT-777"),
    )
    cache.add_order(cached_order, None)
    http_client.request_account_inactive_orders.side_effect = [
        json.dumps(
            {
                "orders": [
                    {
                        "order_index": 102,
                        "status": "filled",
                        "type": 0,
                        "time_in_force": "gtt",
                        "client_order_index": 777,
                        "price": "100050.00",
                        "trigger_price": "0",
                        "created_at": 1704067000000,
                        "updated_at": 1704067300000,
                        "is_ask": False,
                        "initial_base_amount": "0.5000",
                        "filled_base_amount": "0.5000",
                        "filled_quote_amount": "50025.00",
                        "order_expiry": 1704153600000,
                        "reduce_only": False,
                    },
                ],
                "cursor": "next-page",
            },
        ),
        json.dumps({"orders": [], "cursor": None}),
    ]

    command = GenerateOrderStatusReports(
        instrument_id=instrument.id,
        start=None,
        end=None,
        open_only=False,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reports = await client.generate_order_status_reports(command)

    assert len(reports) == 2
    assert reports[0].venue_order_id == VenueOrderId("101")
    assert reports[1].order_status.name == "FILLED"
    assert reports[1].order_list_id == cached_order.order_list_id
    assert reports[1].linked_order_ids == list(cached_order.linked_order_ids)
    assert reports[1].parent_order_id == cached_order.parent_order_id
    assert reports[1].contingency_type == cached_order.contingency_type
    assert http_client.request_account_inactive_orders.await_count == 2


@pytest.mark.asyncio
async def test_generate_order_status_reports_handles_failure(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    http_client.request_account_active_orders.side_effect = RuntimeError("request failed")

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
async def test_generate_fill_reports_filters_by_instrument(exec_client_builder, monkeypatch, instrument):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    http_client.request_account_trades.side_effect = [
        json.dumps(
            {
                "trades": [
                    {
                        "trade_id": "fill-1",
                        "market_id": 1,
                        "ask_account_id": 7,
                        "bid_account_id": 8,
                        "ask_client_id": 777,
                        "bid_client_id": 0,
                        "ask_id": 101,
                        "bid_id": 202,
                        "size": "0.1000",
                        "price": "100010.00",
                        "timestamp": 1704067260000,
                        "is_maker_ask": True,
                        "maker_fee": "100",
                        "taker_fee": "200",
                        "position_id": "5001",
                    },
                ],
                "cursor": "cursor-1",
            },
        ),
        json.dumps({"trades": [], "cursor": None}),
    ]

    command = GenerateFillReports(
        instrument_id=instrument.id,
        venue_order_id=None,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reports = await client.generate_fill_reports(command)

    assert len(reports) == 1
    assert reports[0].venue_order_id == VenueOrderId("101")
    assert reports[0].client_order_id == ClientOrderId("O-777")
    assert http_client.request_account_trades.await_count == 2


@pytest.mark.asyncio
async def test_generate_fill_reports_handles_failure(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    http_client.request_account_trades.side_effect = RuntimeError("request failed")

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
async def test_generate_position_status_reports_returns_flat_for_missing_position(
    exec_client_builder,
    monkeypatch,
    instrument,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    http_client.request_account.return_value = json.dumps({"accounts": [{"assets": [], "positions": []}]})

    command = GeneratePositionStatusReports(
        instrument_id=instrument.id,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    reports = await client.generate_position_status_reports(command)

    assert len(reports) == 1
    assert reports[0].position_side.name == "FLAT"


@pytest.mark.asyncio
async def test_generate_position_status_reports_handles_failure(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    http_client.request_account.side_effect = RuntimeError("request failed")

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
async def test_submit_limit_order(exec_client_builder, monkeypatch, instrument, cache):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    client.generate_account_state = MagicMock()
    await client._connect()

    order = _make_limit_order(instrument.id)
    cache.add_order(order, None)

    command = SubmitOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        order=order,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        position_id=None,
        client_id=None,
    )

    try:
        await client._submit_order(command)
    finally:
        await client._disconnect()

    kwargs = http_client.submit_order.await_args.kwargs
    assert kwargs["market_index"] == 1
    assert kwargs["base_amount"] == 1000
    assert kwargs["price"] == 10000000
    assert kwargs["is_ask"] is False
    assert kwargs["api_key_index"] == 3


@pytest.mark.asyncio
async def test_submit_order_list_uses_batch_submit(exec_client_builder, monkeypatch, instrument, cache):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    client.generate_account_state = MagicMock()
    await client._connect()

    order_1 = _make_limit_order(instrument.id, client_order_id="O-BATCH-1")
    order_2 = _make_limit_order(instrument.id, client_order_id="O-BATCH-2")
    cache.add_order(order_1, None)
    cache.add_order(order_2, None)

    command = SimpleNamespace(order_list=SimpleNamespace(orders=[order_1, order_2]))

    try:
        await client._submit_order_list(command)
    finally:
        await client._disconnect()

    http_client.submit_order_batch.assert_awaited_once()
    http_client.submit_order.assert_not_awaited()
    payload = json.loads(http_client.submit_order_batch.await_args.kwargs["requests_json"])
    assert len(payload) == 2
    assert payload[0]["market_index"] == 1
    assert payload[1]["market_index"] == 1


@pytest.mark.asyncio
async def test_submit_order_list_denies_contingent_orders(
    exec_client_builder,
    monkeypatch,
    instrument,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    client.generate_order_denied = MagicMock()
    client._submit_order_batch = AsyncMock()

    order_list_id = OrderListId("OL-CONTINGENT")
    order_1 = _make_limit_order(
        instrument.id,
        client_order_id="O-CONT-1",
        contingency_type=ContingencyType.OCO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-CONT-2")],
    )
    order_2 = _make_limit_order(
        instrument.id,
        client_order_id="O-CONT-2",
        contingency_type=ContingencyType.OCO,
        order_list_id=order_list_id,
        linked_order_ids=[ClientOrderId("O-CONT-1")],
    )
    command = SubmitOrderList(
        trader_id=order_1.trader_id,
        strategy_id=order_1.strategy_id,
        order_list=OrderList(
            order_list_id=order_list_id,
            orders=[order_1, order_2],
        ),
        position_id=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        client_id=None,
    )

    await client._submit_order_list(command)

    assert client.generate_order_denied.call_count == 2
    denied_client_order_ids = {
        call.kwargs["client_order_id"] for call in client.generate_order_denied.call_args_list
    }
    assert denied_client_order_ids == {order_1.client_order_id, order_2.client_order_id}
    client._submit_order_batch.assert_not_awaited()
    http_client.submit_order_batch.assert_not_awaited()


@pytest.mark.asyncio
async def test_submit_order_rejection(exec_client_builder, monkeypatch, instrument, cache):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    client.generate_account_state = MagicMock()
    client.generate_order_rejected = MagicMock()
    await client._connect()
    http_client.submit_order.side_effect = RuntimeError("Insufficient margin")

    order = _make_limit_order(instrument.id)
    cache.add_order(order, None)

    command = SubmitOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        order=order,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        position_id=None,
        client_id=None,
    )

    try:
        await client._submit_order(command)
    finally:
        await client._disconnect()

    client.generate_order_rejected.assert_called_once()


@pytest.mark.asyncio
async def test_cancel_order_by_venue_id(exec_client_builder, monkeypatch, instrument):
    client, _, http_client, _ = exec_client_builder(monkeypatch)

    command = CancelOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-777"),
        venue_order_id=VenueOrderId("101"),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        client_id=None,
    )

    await client._cancel_order(command)

    http_client.cancel_order.assert_awaited_once_with(
        market_index=1,
        order_index=101,
        api_key_index=3,
    )


@pytest.mark.asyncio
async def test_cancel_order_rejected_when_no_venue_order_id(
    exec_client_builder,
    monkeypatch,
    instrument,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    client.generate_order_cancel_rejected = MagicMock()

    command = CancelOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-MISSING"),
        venue_order_id=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        client_id=None,
    )

    await client._cancel_order(command)

    http_client.cancel_order.assert_not_awaited()
    client.generate_order_cancel_rejected.assert_called_once()


@pytest.mark.asyncio
async def test_cancel_order_rejection(exec_client_builder, monkeypatch, instrument):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    client.generate_order_cancel_rejected = MagicMock()
    http_client.cancel_order.side_effect = RuntimeError("Order already filled")

    command = CancelOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-777"),
        venue_order_id=VenueOrderId("101"),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        client_id=None,
    )

    await client._cancel_order(command)

    http_client.cancel_order.assert_awaited_once()
    client.generate_order_cancel_rejected.assert_called_once()


@pytest.mark.asyncio
async def test_cancel_all_orders_uses_venue_endpoint(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)

    command = SimpleNamespace(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=None,
        order_side=None,
        ts_init=0,
        client_id=None,
    )

    await client._cancel_all_orders(command)

    http_client.cancel_all_orders.assert_awaited_once()
    http_client.cancel_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_all_orders_falls_back_to_cached_orders(
    exec_client_builder,
    monkeypatch,
    instrument,
    cache,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    http_client.cancel_all_orders.side_effect = RuntimeError("venue unavailable")
    client._cancel_orders_batch = AsyncMock()

    order = _make_limit_order(instrument.id, client_order_id="O-CANCEL-ALL")
    order.apply(TestEventStubs.order_accepted(order, venue_order_id=VenueOrderId("101")))
    monkeypatch.setattr(client, "_open_orders_for_cancel_all", MagicMock(return_value=[order]))

    command = SimpleNamespace(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=None,
        order_side=OrderSide.NO_ORDER_SIDE,
        ts_init=0,
        client_id=None,
    )

    await client._cancel_all_orders(command)

    http_client.cancel_all_orders.assert_awaited_once()
    client._cancel_orders_batch.assert_awaited_once()


@pytest.mark.asyncio
async def test_batch_cancel_orders_uses_batch_endpoint(exec_client_builder, monkeypatch, instrument):
    client, _, http_client, _ = exec_client_builder(monkeypatch)

    cancels = [
        CancelOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            instrument_id=instrument.id,
            client_order_id=ClientOrderId("O-701"),
            venue_order_id=VenueOrderId("701"),
            command_id=TestIdStubs.uuid(),
            ts_init=0,
            client_id=None,
        ),
        CancelOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            instrument_id=instrument.id,
            client_order_id=ClientOrderId("O-702"),
            venue_order_id=VenueOrderId("702"),
            command_id=TestIdStubs.uuid(),
            ts_init=0,
            client_id=None,
        ),
    ]

    await client._batch_cancel_orders(SimpleNamespace(cancels=cancels))

    http_client.cancel_order_batch.assert_awaited_once()
    http_client.cancel_order.assert_not_awaited()
    payload = json.loads(http_client.cancel_order_batch.await_args.kwargs["requests_json"])
    assert [item["order_index"] for item in payload] == [701, 702]


@pytest.mark.asyncio
async def test_modify_limit_order(exec_client_builder, monkeypatch, instrument, cache):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    client.generate_account_state = MagicMock()
    await client._connect()

    order = _make_limit_order(instrument.id)
    cache.add_order(order, None)
    order.apply(TestEventStubs.order_accepted(order, venue_order_id=VenueOrderId("101")))

    command = ModifyOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        instrument_id=order.instrument_id,
        client_order_id=order.client_order_id,
        venue_order_id=VenueOrderId("101"),
        quantity=Quantity.from_str("0.2000"),
        price=Price.from_str("100100.00"),
        trigger_price=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    try:
        await client._modify_order(command)
    finally:
        await client._disconnect()

    kwargs = http_client.modify_order.await_args.kwargs
    assert kwargs["market_index"] == 1
    assert kwargs["order_index"] == 101
    assert kwargs["base_amount"] == 2000
    assert kwargs["price"] == 10010000
    assert kwargs["api_key_index"] == 3


@pytest.mark.asyncio
async def test_modify_order_rejected_when_not_in_cache(
    exec_client_builder,
    monkeypatch,
    instrument,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    client.generate_order_modify_rejected = MagicMock()

    command = ModifyOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-UNKNOWN"),
        venue_order_id=VenueOrderId("101"),
        quantity=Quantity.from_str("0.2000"),
        price=Price.from_str("100100.00"),
        trigger_price=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._modify_order(command)

    http_client.modify_order.assert_not_awaited()
    client.generate_order_modify_rejected.assert_called_once()


@pytest.mark.asyncio
async def test_modify_order_rejected_when_no_venue_order_id(
    exec_client_builder,
    monkeypatch,
    instrument,
    cache,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    client.generate_order_modify_rejected = MagicMock()

    order = _make_limit_order(instrument.id, client_order_id="O-NO-VID")
    cache.add_order(order, None)

    command = ModifyOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        instrument_id=order.instrument_id,
        client_order_id=order.client_order_id,
        venue_order_id=None,
        quantity=Quantity.from_str("0.2000"),
        price=Price.from_str("100100.00"),
        trigger_price=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._modify_order(command)

    http_client.modify_order.assert_not_awaited()
    client.generate_order_modify_rejected.assert_called_once()


@pytest.mark.asyncio
async def test_modify_order_rejection_on_http_error(
    exec_client_builder,
    monkeypatch,
    instrument,
    cache,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    client.generate_account_state = MagicMock()
    client.generate_order_modify_rejected = MagicMock()
    http_client.modify_order.side_effect = RuntimeError("modify rejected")
    await client._connect()

    order = _make_limit_order(instrument.id)
    cache.add_order(order, None)
    order.apply(TestEventStubs.order_accepted(order, venue_order_id=VenueOrderId("101")))

    command = ModifyOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        instrument_id=order.instrument_id,
        client_order_id=order.client_order_id,
        venue_order_id=VenueOrderId("101"),
        quantity=Quantity.from_str("0.2000"),
        price=Price.from_str("100100.00"),
        trigger_price=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    try:
        await client._modify_order(command)
    finally:
        await client._disconnect()

    http_client.modify_order.assert_awaited_once()
    client.generate_order_modify_rejected.assert_called_once()


@pytest.mark.asyncio
async def test_query_account_refreshes_account_state(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    client.generate_account_state = MagicMock()

    command = SimpleNamespace()

    await client._query_account(command)

    http_client.create_auth_token.assert_awaited_once()
    http_client.request_account.assert_awaited_once()
    client.generate_account_state.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "http_method_name", "call_kwargs", "expected_args"),
    [
        ("request_account_api_keys", "request_account_api_keys", {}, (7, "auth-token")),
        ("request_account_limits", "request_account_limits", {}, (7, "auth-token")),
        ("request_account_metadata", "request_account_metadata", {}, (7, "auth-token")),
        ("request_l1_metadata", "request_l1_metadata", {"l1_address": "0xabc"}, ("0xabc", "auth-token")),
        (
            "request_public_pools_metadata",
            "request_public_pools_metadata",
            {},
            ("all", 0, 100, None, "auth-token"),
        ),
        ("request_account_pnl", "request_account_pnl", {}, (7, "auth-token")),
        (
            "request_liquidations",
            "request_liquidations",
            {"limit": 25, "market_id": 1, "cursor": "cursor-1", "account_index": 9},
            (9, 25, 1, "cursor-1", "auth-token"),
        ),
        ("request_position_fundings", "request_position_fundings", {}, (7, "auth-token")),
        ("request_deposit_history", "request_deposit_history", {"cursor": "cursor-1"}, (7, "auth-token", "cursor-1")),
        ("request_withdraw_history", "request_withdraw_history", {"cursor": "cursor-2"}, (7, "auth-token", "cursor-2")),
        ("request_transfer_history", "request_transfer_history", {"cursor": "cursor-3"}, (7, "auth-token", "cursor-3")),
        (
            "request_transfer_fee_info",
            "request_transfer_fee_info",
            {"to_account_index": 9},
            (7, 9, "auth-token"),
        ),
        ("request_api_tokens", "request_api_tokens", {}, (7, "auth-token")),
        (
            "request_user_referrals",
            "request_user_referrals",
            {"l1_address": "0xabc", "cursor": 2},
            ("0xabc", 2, "auth-token"),
        ),
        ("request_referral_code", "request_referral_code", {}, (7, "auth-token")),
        ("create_referral_code", "create_referral_code", {}, (7, "auth-token")),
        (
            "update_referral_code",
            "update_referral_code",
            {"new_referral_code": "LIGHTER7"},
            (7, "LIGHTER7", "auth-token"),
        ),
        (
            "update_referral_kickback",
            "update_referral_kickback",
            {"kickback_percentage": 25.0},
            (7, 25.0, "auth-token"),
        ),
        (
            "use_referral_code",
            "use_referral_code",
            {
                "l1_address": "0xabc",
                "referral_code": "LIGHTER7",
                "x": "x_user",
                "discord": "discord#7",
            },
            ("0xabc", "LIGHTER7", "x_user", "discord#7", None, None, "auth-token"),
        ),
        (
            "create_api_token",
            "create_api_token",
            {
                "name": "reporting",
                "expiry": 1767139200,
                "sub_account_access": True,
            },
            ("reporting", 7, 1767139200, True, "auth-token", "read.*"),
        ),
        ("revoke_api_token", "revoke_api_token", {"token_id": 11}, (11, 7, "auth-token")),
        ("change_account_tier", "change_account_tier", {"new_tier": "premium"}, (7, "premium", "auth-token")),
        (
            "acknowledge_notification",
            "acknowledge_notification",
            {"notif_id": "notif-1"},
            ("notif-1", 7, "auth-token"),
        ),
    ],
)
async def test_auth_helper_methods_use_auth_token(
    exec_client_builder,
    monkeypatch,
    method_name,
    http_method_name,
    call_kwargs,
    expected_args,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    ensure_auth_token = AsyncMock(return_value="auth-token")
    monkeypatch.setattr(client, "_ensure_auth_token", ensure_auth_token)

    result = await getattr(client, method_name)(**call_kwargs)

    ensure_auth_token.assert_awaited_once()
    getattr(http_client, http_method_name).assert_awaited_once_with(*expected_args)
    assert result["code"] == 200


@pytest.mark.asyncio
async def test_public_helper_methods_do_not_require_auth(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)

    announcements = await client.request_announcements()
    status = await client.request_status()
    system_config = await client.request_system_config()
    metrics = await client.request_exchange_metrics(period="d", kind="volume")
    execute_stats = await client.request_execute_stats("d")
    layer1_basic_info = await client.request_layer1_basic_info()
    zk_lighter_info = await client.request_zk_lighter_info()

    http_client.request_announcements.assert_awaited_once_with()
    http_client.request_status.assert_awaited_once_with()
    http_client.request_system_config.assert_awaited_once_with()
    http_client.request_exchange_metrics.assert_awaited_once_with("d", "volume", None, None)
    http_client.request_execute_stats.assert_awaited_once_with("d")
    http_client.request_layer1_basic_info.assert_awaited_once_with()
    http_client.request_zk_lighter_info.assert_awaited_once_with()
    http_client.create_auth_token.assert_not_awaited()
    assert announcements["code"] == 200
    assert status["status"] == 1
    assert system_config["code"] == 200
    assert metrics["code"] == 200
    assert execute_stats["code"] == 200
    assert layer1_basic_info["code"] == 200
    assert zk_lighter_info["contract_address"] == "0xcontract"


@pytest.mark.asyncio
async def test_request_withdrawal_delay_does_not_require_auth(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)

    result = await client.request_withdrawal_delay()

    http_client.request_withdrawal_delay.assert_awaited_once_with()
    http_client.create_auth_token.assert_not_awaited()
    assert result["seconds"] == 86400


@pytest.mark.asyncio
async def test_request_sub_accounts_passes_l1_address(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)

    result = await client.request_sub_accounts("0xabc")

    http_client.request_sub_accounts.assert_awaited_once_with("0xabc")
    http_client.create_auth_token.assert_not_awaited()
    assert result["l1_address"] == "0xabc"


@pytest.mark.asyncio
async def test_request_next_nonce_uses_configured_api_key_index(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)

    result = await client.request_next_nonce()

    http_client.request_next_nonce.assert_awaited_once_with(7, 3)
    assert result["nonce"] == 12345


@pytest.mark.asyncio
async def test_request_enriched_tx_passes_hash(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)

    result = await client.request_enriched_tx("0xabc")

    http_client.request_enriched_tx.assert_awaited_once_with("0xabc")
    assert result["tx_hash"] == "0xabc"


@pytest.mark.asyncio
async def test_request_tx_from_l1_tx_hash_passes_hash(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)

    result = await client.request_tx_from_l1_tx_hash("0xl1")

    http_client.request_tx_from_l1_tx_hash.assert_awaited_once_with("0xl1")
    assert result["hash"] == "0xl1"


@pytest.mark.asyncio
async def test_request_txs_does_not_require_auth(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)

    result = await client.request_txs(limit=25, index=10)

    http_client.request_txs.assert_awaited_once_with(25, 10)
    http_client.create_auth_token.assert_not_awaited()
    assert result["code"] == 200


@pytest.mark.asyncio
async def test_request_export_uses_auth(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    ensure_auth_token = AsyncMock(return_value="auth-token")
    monkeypatch.setattr(client, "_ensure_auth_token", ensure_auth_token)

    result = await client.request_export(
        export_type="trade",
        account_index=9,
        market_id=1,
        start_timestamp=10,
        end_timestamp=20,
        side="long",
        role="maker",
        trade_type="trade",
    )

    ensure_auth_token.assert_awaited_once()
    http_client.request_export.assert_awaited_once_with(
        "trade",
        "auth-token",
        9,
        1,
        10,
        20,
        "long",
        "maker",
        "trade",
    )
    assert result["code"] == 200


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "call_kwargs", "http_method_name", "expected_args"),
    [
        (
            "create_intent_address",
            {
                "chain_id": "1",
                "from_addr": "0xabc",
                "amount": "1000000",
                "is_external_deposit": True,
            },
            "create_intent_address",
            ("1", "0xabc", "1000000", True),
        ),
        ("request_fast_bridge_info", {}, "request_fast_bridge_info", ()),
        ("request_deposit_latest", {"l1_address": "0xabc"}, "request_deposit_latest", ("0xabc",)),
        ("request_deposit_networks", {}, "request_deposit_networks", ()),
        ("request_lease_options", {}, "request_lease_options", ()),
    ],
)
async def test_public_bridge_helper_methods_call_http_client(
    exec_client_builder,
    monkeypatch,
    method_name,
    call_kwargs,
    http_method_name,
    expected_args,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)

    result = await getattr(client, method_name)(**call_kwargs)

    getattr(http_client, http_method_name).assert_awaited_once_with(*expected_args)
    http_client.create_auth_token.assert_not_awaited()
    assert result["code"] == 200


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "call_kwargs", "http_method_name", "expected_kwargs"),
    [
        (
            "update_leverage",
            {"instrument_id": None, "initial_margin_fraction": 500},
            "update_leverage",
            {
                "market_index": 1,
                "initial_margin_fraction": 500,
                "margin_mode": LIGHTER_MARGIN_MODE_CROSS,
                "api_key_index": 3,
            },
        ),
        (
            "update_margin",
            {"instrument_id": None, "usdc_amount": 2500},
            "update_margin",
            {
                "market_index": 1,
                "usdc_amount": 2500,
                "direction": LIGHTER_UPDATE_MARGIN_ADD,
                "api_key_index": 3,
            },
        ),
        (
            "withdraw",
            {"asset_index": 2, "route_type": 0, "amount": 1000, "api_key_index": 3, "nonce": 44},
            "withdraw",
            {"asset_index": 2, "route_type": 0, "amount": 1000, "api_key_index": 3, "nonce": 44},
        ),
        (
            "transfer",
            {
                "to_account_index": 9,
                "asset_index": 2,
                "from_route_type": 0,
                "to_route_type": 1,
                "amount": 1000,
                "usdc_fee": 5,
                "memo": "rebalance",
                "api_key_index": 3,
                "nonce": 55,
            },
            "transfer",
            {
                "to_account_index": 9,
                "asset_index": 2,
                "from_route_type": 0,
                "to_route_type": 1,
                "amount": 1000,
                "usdc_fee": 5,
                "memo": "rebalance",
                "api_key_index": 3,
                "nonce": 55,
            },
        ),
        (
            "fast_withdraw",
            {"tx_info": '{"nonce":1}', "to_address": "0xdef"},
            "fast_withdraw",
            {"tx_info": '{"nonce":1}', "to_address": "0xdef", "auth_token": "lighter-auth-token"},
        ),
        (
            "change_pub_key",
            {"new_pub_key": "0xpub", "api_key_index": 4, "nonce": 66},
            "change_pub_key",
            {"new_pub_key": "0xpub", "api_key_index": 4, "nonce": 66},
        ),
        (
            "create_sub_account",
            {"api_key_index": 5, "nonce": 77},
            "create_sub_account",
            {"api_key_index": 5, "nonce": 77},
        ),
        (
            "create_public_pool",
            {
                "operator_fee": 10,
                "initial_total_shares": 1_000,
                "min_operator_share_rate": 25,
                "api_key_index": 6,
                "nonce": 88,
            },
            "create_public_pool",
            {
                "operator_fee": 10,
                "initial_total_shares": 1_000,
                "min_operator_share_rate": 25,
                "api_key_index": 6,
                "nonce": 88,
            },
        ),
        (
            "update_public_pool",
            {
                "public_pool_index": 11,
                "status": 1,
                "operator_fee": 12,
                "min_operator_share_rate": 30,
                "api_key_index": 7,
                "nonce": 89,
            },
            "update_public_pool",
            {
                "public_pool_index": 11,
                "status": 1,
                "operator_fee": 12,
                "min_operator_share_rate": 30,
                "api_key_index": 7,
                "nonce": 89,
            },
        ),
        (
            "mint_pool_shares",
            {
                "public_pool_index": 11,
                "share_amount": 250,
                "api_key_index": 8,
                "nonce": 90,
            },
            "mint_pool_shares",
            {
                "public_pool_index": 11,
                "share_amount": 250,
                "api_key_index": 8,
                "nonce": 90,
            },
        ),
        (
            "burn_pool_shares",
            {
                "public_pool_index": 11,
                "share_amount": 100,
                "api_key_index": 9,
                "nonce": 91,
            },
            "burn_pool_shares",
            {
                "public_pool_index": 11,
                "share_amount": 100,
                "api_key_index": 9,
                "nonce": 91,
            },
        ),
        (
            "lit_lease",
            {
                "tx_info": '{"nonce":2}',
                "lease_amount": "2500",
                "duration_days": 30,
            },
            "lit_lease",
            {
                "tx_info": '{"nonce":2}',
                "auth_token": "lighter-auth-token",
                "lease_amount": "2500",
                "duration_days": 30,
            },
        ),
    ],
)
async def test_admin_helper_methods_call_http_client(
    exec_client_builder,
    monkeypatch,
    instrument,
    method_name,
    call_kwargs,
    http_method_name,
    expected_kwargs,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    if "instrument_id" in call_kwargs:
        call_kwargs = dict(call_kwargs)
        call_kwargs["instrument_id"] = instrument.id

    result = await getattr(client, method_name)(**call_kwargs)

    getattr(http_client, http_method_name).assert_awaited_once_with(**expected_kwargs)
    assert result["code"] == 200


@pytest.mark.asyncio
async def test_request_fast_withdraw_info_uses_auth(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    ensure_auth_token = AsyncMock(return_value="auth-token")
    monkeypatch.setattr(client, "_ensure_auth_token", ensure_auth_token)

    result = await client.request_fast_withdraw_info(account_index=9)

    ensure_auth_token.assert_awaited_once()
    http_client.request_fast_withdraw_info.assert_awaited_once_with(9, "auth-token")
    assert result["code"] == 200


@pytest.mark.asyncio
async def test_request_leases_uses_auth(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    ensure_auth_token = AsyncMock(return_value="auth-token")
    monkeypatch.setattr(client, "_ensure_auth_token", ensure_auth_token)

    result = await client.request_leases(cursor="cursor-1", limit=25, account_index=9)

    ensure_auth_token.assert_awaited_once()
    http_client.request_leases.assert_awaited_once_with(9, "auth-token", "cursor-1", 25)
    assert result["code"] == 200


@pytest.mark.asyncio
async def test_change_account_tier_raises_on_error(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    monkeypatch.setattr(client, "_ensure_auth_token", AsyncMock(return_value="auth-token"))
    http_client.change_account_tier.return_value = json.dumps({"code": 409, "message": "conflict"})

    with pytest.raises(RuntimeError, match="change account tier failed"):
        await client.change_account_tier("premium")


@pytest.mark.asyncio
async def test_create_api_token_raises_on_error(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    monkeypatch.setattr(client, "_ensure_auth_token", AsyncMock(return_value="auth-token"))
    http_client.create_api_token.return_value = json.dumps({"code": 409, "message": "conflict"})

    with pytest.raises(RuntimeError, match="create api token failed"):
        await client.create_api_token(
            name="reporting",
            expiry=1767139200,
            sub_account_access=True,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "call_kwargs", "http_method_name", "error_message"),
    [
        (
            "create_intent_address",
            {"chain_id": "1", "from_addr": "0xabc", "amount": "100", "is_external_deposit": False},
            "create_intent_address",
            "create intent address failed",
        ),
        (
            "fast_withdraw",
            {"tx_info": '{"nonce":1}', "to_address": "0xdef"},
            "fast_withdraw",
            "fast withdraw failed",
        ),
        (
            "lit_lease",
            {"tx_info": '{"nonce":2}'},
            "lit_lease",
            "lit lease failed",
        ),
    ],
)
async def test_bridge_and_lease_helper_methods_raise_on_error(
    exec_client_builder,
    monkeypatch,
    method_name,
    call_kwargs,
    http_method_name,
    error_message,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    monkeypatch.setattr(client, "_ensure_auth_token", AsyncMock(return_value="auth-token"))
    getattr(http_client, http_method_name).return_value = json.dumps(
        {"code": 409, "message": "conflict"},
    )

    with pytest.raises(RuntimeError, match=error_message):
        await getattr(client, method_name)(**call_kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "call_kwargs", "http_method_name", "error_message"),
    [
        ("create_referral_code", {}, "create_referral_code", "create referral code failed"),
        (
            "update_referral_code",
            {"new_referral_code": "LIGHTER7"},
            "update_referral_code",
            "update referral code failed",
        ),
        (
            "update_referral_kickback",
            {"kickback_percentage": 25.0},
            "update_referral_kickback",
            "update referral kickback failed",
        ),
        (
            "use_referral_code",
            {"l1_address": "0xabc", "referral_code": "LIGHTER7", "x": "x_user"},
            "use_referral_code",
            "use referral code failed",
        ),
    ],
)
async def test_referral_helper_methods_raise_on_error(
    exec_client_builder,
    monkeypatch,
    method_name,
    call_kwargs,
    http_method_name,
    error_message,
):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    monkeypatch.setattr(client, "_ensure_auth_token", AsyncMock(return_value="auth-token"))
    getattr(http_client, http_method_name).return_value = json.dumps(
        {"code": 409, "message": "conflict"},
    )

    with pytest.raises(RuntimeError, match=error_message):
        await getattr(client, method_name)(**call_kwargs)


@pytest.mark.asyncio
async def test_change_pub_key_raises_on_tx_error(exec_client_builder, monkeypatch):
    client, _, http_client, _ = exec_client_builder(monkeypatch)
    http_client.change_pub_key.return_value = json.dumps({"code": 500, "message": "bad key"})

    with pytest.raises(RuntimeError, match="Lighter tx failed"):
        await client.change_pub_key(new_pub_key="0xpub")


@pytest.mark.asyncio
async def test_handle_order_report_accepts_cached_order(exec_client_builder, monkeypatch, instrument, cache):
    client, _, _, _ = exec_client_builder(monkeypatch)
    client.generate_order_accepted = MagicMock()
    client._send_order_status_report = MagicMock()

    order = _make_limit_order(instrument.id, client_order_id="O-WS-ACCEPT")
    cache.add_order(order, None)
    client._client_order_index_to_id[456] = order.client_order_id

    payload = {
        "order_index": 101,
        "status": "open",
        "type": 0,
        "time_in_force": "gtt",
        "client_order_index": 456,
        "price": "100000.00",
        "trigger_price": "0",
        "created_at": 1704067200000,
        "updated_at": 1704067260000,
        "is_ask": False,
        "initial_base_amount": "0.1000",
        "filled_base_amount": "0.0000",
        "filled_quote_amount": "0.00",
        "order_expiry": 1704153600000,
        "reduce_only": False,
    }

    client._handle_orders_update({"orders": {"1": [payload]}})

    client.generate_order_accepted.assert_called_once()
    client._send_order_status_report.assert_not_called()


@pytest.mark.asyncio
async def test_handle_order_report_applies_cached_contingency_metadata(
    exec_client_builder,
    monkeypatch,
    instrument,
    cache,
):
    client, _, _, _ = exec_client_builder(monkeypatch)
    client._handle_cached_order_report = MagicMock()
    client._send_order_status_report = MagicMock()

    order = _make_limit_order(
        instrument.id,
        client_order_id="O-WS-CONT",
        contingency_type=ContingencyType.OCO,
        order_list_id=OrderListId("OL-WS-CONT"),
        linked_order_ids=[ClientOrderId("O-WS-LINKED")],
        parent_order_id=ClientOrderId("O-WS-PARENT"),
    )
    cache.add_order(order, None)

    report = order_report_from_lighter(
        {
            "order_index": 111,
            "status": "open",
            "type": 0,
            "time_in_force": "gtt",
            "client_order_index": 0,
            "client_order_id": order.client_order_id.value,
            "price": "100000.00",
            "trigger_price": "0",
            "created_at": 1704067200000,
            "updated_at": 1704067260000,
            "is_ask": False,
            "initial_base_amount": "0.1000",
            "filled_base_amount": "0.0000",
            "filled_quote_amount": "0.00",
            "order_expiry": 1704153600000,
            "reduce_only": False,
        },
        client.account_id,
        instrument,
        lambda _: order.client_order_id,
    )

    client._handle_order_report(report)

    client._handle_cached_order_report.assert_called_once()
    propagated_report = client._handle_cached_order_report.call_args.args[1]
    assert propagated_report.order_list_id == order.order_list_id
    assert propagated_report.linked_order_ids == list(order.linked_order_ids)
    assert propagated_report.parent_order_id == order.parent_order_id
    assert propagated_report.contingency_type == order.contingency_type
    client._send_order_status_report.assert_not_called()


@pytest.mark.asyncio
async def test_handle_order_report_deduplicates_same_state(exec_client_builder, monkeypatch, instrument):
    client, _, _, _ = exec_client_builder(monkeypatch)
    client._send_order_status_report = MagicMock()

    report = order_report_from_lighter(
        {
            "order_index": 101,
            "status": "open",
            "type": 0,
            "time_in_force": "gtt",
            "client_order_index": 777,
            "price": "100000.00",
            "trigger_price": "0",
            "created_at": 1704067200000,
            "updated_at": 1704067260000,
            "is_ask": False,
            "initial_base_amount": "0.1000",
            "filled_base_amount": "0.0000",
            "filled_quote_amount": "0.00",
            "order_expiry": 1704153600000,
            "reduce_only": False,
        },
        client.account_id,
        instrument,
        lambda _: ClientOrderId("O-DUPE"),
    )

    client._handle_order_report(report)
    client._handle_order_report(report)

    client._send_order_status_report.assert_called_once()


@pytest.mark.asyncio
async def test_generate_mass_status_returns_status(exec_client_builder, monkeypatch):
    client, _, _, _ = exec_client_builder(monkeypatch)
    monkeypatch.setattr(client, "generate_order_status_reports", AsyncMock(return_value=[]))
    monkeypatch.setattr(client, "generate_fill_reports", AsyncMock(return_value=[]))
    monkeypatch.setattr(client, "generate_position_status_reports", AsyncMock(return_value=[]))

    mass_status = await client.generate_mass_status(lookback_mins=5)

    assert mass_status is not None
    assert mass_status.account_id == client.account_id
