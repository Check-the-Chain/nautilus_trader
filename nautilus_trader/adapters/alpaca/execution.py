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

import asyncio
from collections import deque
from contextlib import suppress
from decimal import Decimal
from typing import Any

import pandas as pd

from nautilus_trader.adapters.alpaca.common import ALPACA_ORDER_TYPE
from nautilus_trader.adapters.alpaca.common import account_balance_from_account
from nautilus_trader.adapters.alpaca.common import account_type_from_account
from nautilus_trader.adapters.alpaca.common import activity_to_fill_report
from nautilus_trader.adapters.alpaca.common import data_symbol_for_instrument
from nautilus_trader.adapters.alpaca.common import get_timestamp_ns
from nautilus_trader.adapters.alpaca.common import is_crypto_instrument
from nautilus_trader.adapters.alpaca.common import is_equity_instrument
from nautilus_trader.adapters.alpaca.common import is_option_instrument
from nautilus_trader.adapters.alpaca.common import order_to_report
from nautilus_trader.adapters.alpaca.common import position_to_report
from nautilus_trader.adapters.alpaca.common import quote_currency_for_instrument
from nautilus_trader.adapters.alpaca.common import symbol_to_instrument_id
from nautilus_trader.adapters.alpaca.config import AlpacaExecClientConfig
from nautilus_trader.adapters.alpaca.constants import ALPACA_LIVE_TRADING_WS_URL
from nautilus_trader.adapters.alpaca.constants import ALPACA_PAPER_TRADING_WS_URL
from nautilus_trader.adapters.alpaca.constants import ALPACA_VENUE
from nautilus_trader.adapters.alpaca.http import AlpacaHttpClient
from nautilus_trader.adapters.alpaca.providers import AlpacaInstrumentProvider
from nautilus_trader.adapters.alpaca.websocket import AlpacaWebSocketClient
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import BatchCancelOrders
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import QueryAccount
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import ContingencyType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Money


class AlpacaExecutionClient(LiveExecutionClient):
    """
    Provides a live execution client for Alpaca.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: AlpacaHttpClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: AlpacaInstrumentProvider,
        config: AlpacaExecClientConfig,
        name: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or ALPACA_VENUE.value),
            venue=ALPACA_VENUE,
            oms_type=OmsType.NETTING,
            account_type=config.account_type,
            base_currency=USD,
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )

        self._client = client
        self._config = config
        self._instrument_provider = instrument_provider
        self._ws_client: AlpacaWebSocketClient | None = None
        self._reconnect_task: asyncio.Task | None = None
        self._is_disconnecting = False
        self._processed_trade_ids: set[str] = set()
        self._processed_trade_queue: deque[str] = deque()
        self._processed_trade_id_limit = 10_000

        self._log.info(f"config.paper={config.paper}", LogColor.BLUE)
        self._log.info(f"config.http_timeout_secs={config.http_timeout_secs}", LogColor.BLUE)

    async def _connect(self) -> None:
        self._is_disconnecting = False
        await self._instrument_provider.initialize()
        await self._update_account_state()
        await self._await_account_registered()
        await self._ensure_ws_connected()

    async def _disconnect(self) -> None:
        self._is_disconnecting = True
        if self._reconnect_task is not None:
            self._reconnect_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._reconnect_task
            self._reconnect_task = None
        if self._ws_client is not None:
            await self._ws_client.close()
            self._ws_client = None

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        try:
            if command.client_order_id is not None:
                order = await self._client.get_order_by_client_order_id(command.client_order_id.value)
            elif command.venue_order_id is not None:
                order = await self._client.get_order(command.venue_order_id.value)
            else:
                raise ValueError("Either client_order_id or venue_order_id must be provided")

            instrument = await self._load_instrument_for_symbol(order["symbol"])
            if instrument is None:
                return None
            report = order_to_report(self.account_id, instrument, order)
            cached_order = self._cached_order_for_report(
                client_order_id=report.client_order_id,
                venue_order_id=report.venue_order_id,
            )
            if cached_order is not None:
                self._apply_cached_order_report_metadata(cached_order, report)
            return report
        except (asyncio.CancelledError, Exception) as e:
            self._log_report_error(e, "OrderStatusReport")
            return None

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        try:
            symbols = None
            if command.instrument_id is not None:
                instrument = await self._ensure_instrument(command.instrument_id)
                if instrument is None:
                    return []
                symbols = [data_symbol_for_instrument(instrument)]

            orders = await self._list_orders_paginated(
                status="open" if command.open_only else "all",
                after=self._timestamp_to_iso(command.start),
                until=self._timestamp_to_iso(command.end),
                symbols=symbols,
                nested=True,
            )

            reports_by_venue_order_id: dict[str, OrderStatusReport] = {}
            for order in orders:
                instrument = await self._load_instrument_for_symbol(order["symbol"])
                if instrument is None:
                    continue
                for report in self._order_reports_from_payload(order, instrument):
                    reports_by_venue_order_id.setdefault(report.venue_order_id.value, report)

            reports = list(reports_by_venue_order_id.values())

            self._log_report_receipt(
                len(reports),
                "OrderStatusReport",
                self._report_log_level(command),
            )
            return reports
        except (asyncio.CancelledError, Exception) as e:
            self._log_report_error(e, "OrderStatusReports")
            return []

    async def generate_fill_reports(
        self,
        command: GenerateFillReports,
    ) -> list[FillReport]:
        try:
            activities = await self._get_activities_paginated(
                activity_type="FILL",
                after=self._timestamp_to_iso(command.start),
                until=self._timestamp_to_iso(command.end),
            )

            reports: list[FillReport] = []
            for activity in activities:
                instrument = await self._load_instrument_for_symbol(activity["symbol"])
                if instrument is None:
                    continue
                if command.instrument_id is not None and instrument.id != command.instrument_id:
                    continue
                if command.venue_order_id is not None and activity["order_id"] != command.venue_order_id.value:
                    continue

                reports.append(
                    activity_to_fill_report(
                        self.account_id,
                        instrument,
                        activity,
                        client_order_id=self._cache.client_order_id(
                            VenueOrderId(activity["order_id"]),
                        ),
                    ),
                )

            self._log_report_receipt(
                len(reports),
                "FillReport",
                self._report_log_level(command),
            )
            return reports
        except (asyncio.CancelledError, Exception) as e:
            self._log_report_error(e, "FillReports")
            return []

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        try:
            positions = await self._client.get_positions()
            reports: list[PositionStatusReport] = []

            for position in positions:
                instrument = await self._load_instrument_for_symbol(position["symbol"])
                if instrument is None:
                    continue
                if command.instrument_id is not None and instrument.id != command.instrument_id:
                    continue
                reports.append(position_to_report(self.account_id, instrument, position))

            if command.instrument_id is not None and not reports:
                instrument = await self._ensure_instrument(command.instrument_id)
                if instrument is not None:
                    ts_now = self._clock.timestamp_ns()
                    reports.append(
                        PositionStatusReport(
                            account_id=self.account_id,
                            instrument_id=command.instrument_id,
                            position_side=PositionSide.FLAT,
                            quantity=instrument.make_qty("0"),
                            report_id=UUID4(),
                            ts_last=ts_now,
                            ts_init=ts_now,
                        ),
                    )

            self._log_report_receipt(
                len(reports),
                "PositionStatusReport",
                self._report_log_level(command),
            )
            return reports
        except (asyncio.CancelledError, Exception) as e:
            self._log_report_error(e, "PositionStatusReports")
            return []

    async def _submit_order(self, command: SubmitOrder) -> None:
        await self._submit_order_inner(command.order, command.params)

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        contingent_kind = self._contingent_order_list_kind(command.order_list)
        if contingent_kind is not None:
            await self._submit_contingent_order_list(command, contingent_kind)
            return

        if any(self._is_contingent_order(order) for order in command.order_list.orders):
            self._deny_order_list_pre_submit(command.order_list.orders, "UNSUPPORTED_CONTINGENT_ORDER_LIST")
            return

        for order in command.order_list.orders:
            await self._submit_order_inner(order, command.params)

    async def _modify_order(self, command: ModifyOrder) -> None:
        venue_order_id = command.venue_order_id or self._cache.venue_order_id(command.client_order_id)
        order = self._cache.order(command.client_order_id)
        if order is None or venue_order_id is None:
            self.generate_order_modify_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=venue_order_id or VenueOrderId("UNKNOWN"),
                reason="ORDER_NOT_FOUND",
                ts_event=self._clock.timestamp_ns(),
            )
            return

        payload: dict[str, Any] = {}
        if command.quantity is not None:
            payload["qty"] = str(command.quantity)
        if command.price is not None:
            payload["limit_price"] = str(command.price)
        if command.trigger_price is not None:
            payload["stop_price"] = str(command.trigger_price)
        if not payload:
            return

        if order.time_in_force == TimeInForce.GTC:
            payload["time_in_force"] = "gtc"
        elif order.time_in_force == TimeInForce.DAY:
            payload["time_in_force"] = "day"

        try:
            venue_order = await self._client.replace_order(venue_order_id.value, payload)
            instrument = await self._ensure_instrument(command.instrument_id)
            if instrument is None:
                return
            report = order_to_report(
                self.account_id,
                instrument,
                venue_order,
                client_order_id=command.client_order_id,
            )
            self.generate_order_updated(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=report.venue_order_id,
                quantity=report.quantity,
                price=report.price,
                trigger_price=report.trigger_price,
                ts_event=report.ts_last,
                venue_order_id_modified=(report.venue_order_id != venue_order_id),
            )
        except Exception as e:
            self.generate_order_modify_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=venue_order_id,
                reason=str(e),
                ts_event=self._clock.timestamp_ns(),
            )

    async def _cancel_order(self, command: CancelOrder) -> None:
        venue_order_id = command.venue_order_id or self._cache.venue_order_id(command.client_order_id)
        if venue_order_id is None:
            self.generate_order_cancel_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=VenueOrderId("UNKNOWN"),
                reason="ORDER_NOT_FOUND",
                ts_event=self._clock.timestamp_ns(),
            )
            return

        try:
            await self._client.cancel_order(venue_order_id.value)
        except Exception as e:
            self.generate_order_cancel_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=venue_order_id,
                reason=str(e),
                ts_event=self._clock.timestamp_ns(),
            )

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        order_side = (
            command.order_side
            if getattr(command, "order_side", None) is not None
            else OrderSide.NO_ORDER_SIDE
        )
        if command.instrument_id is None and order_side == OrderSide.NO_ORDER_SIDE:
            try:
                await self._client.cancel_all_orders()
                return
            except Exception as exc:
                self._log.warning(
                    "Alpaca venue cancel_all_orders failed; falling back to cached open orders "
                    f"({exc})",
                )

        open_orders = self._open_orders_for_cancel_all(command.instrument_id, order_side)
        for order in open_orders:
            await self._cancel_order(
                CancelOrder(
                    trader_id=command.trader_id,
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=order.venue_order_id,
                    command_id=command.command_id,
                    ts_init=command.ts_init,
                ),
            )

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        for cancel in command.cancels:
            await self._cancel_order(cancel)

    async def _query_account(self, command: QueryAccount) -> None:
        await self._update_account_state()

    def _report_log_level(self, command: Any) -> LogLevel:
        return getattr(command, "log_receipt_level", LogLevel.INFO)

    def _contingent_order_list_kind(self, order_list) -> str | None:
        if order_list.is_bracket():
            return "bracket"

        orders = list(order_list.orders)
        if len(orders) != 2:
            return None

        first, second = orders
        if (
            first.contingency_type == ContingencyType.OTO
            and first.parent_order_id is None
            and second.parent_order_id == first.client_order_id
        ):
            return "oto"

        if self._is_oco_pair(orders):
            return "oco"

        return None

    def _is_contingent_order(self, order: Any) -> bool:
        contingency_type = getattr(order, "contingency_type", ContingencyType.NO_CONTINGENCY)
        linked_order_ids = getattr(order, "linked_order_ids", None)
        parent_order_id = getattr(order, "parent_order_id", None)
        return (
            contingency_type not in (None, ContingencyType.NO_CONTINGENCY)
            or bool(linked_order_ids)
            or parent_order_id is not None
        )

    @staticmethod
    def _is_oco_pair(orders: list[Any]) -> bool:
        if len(orders) != 2:
            return False

        first, second = orders
        return (
            first.parent_order_id is None
            and second.parent_order_id is None
            and first.contingency_type == ContingencyType.OCO
            and second.contingency_type == ContingencyType.OCO
            and first.side == second.side
        )

    def _deny_order_pre_submit(self, order: Any, reason: str) -> None:
        self.generate_order_denied(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            reason=reason,
            ts_event=self._clock.timestamp_ns(),
        )

    def _deny_order_list_pre_submit(self, orders: list[Any], reason: str) -> None:
        for order in orders:
            self._deny_order_pre_submit(order, reason)

    def _reject_order_list_submit(self, orders: list[Any], reason: str) -> None:
        ts_event = self._clock.timestamp_ns()
        for order in orders:
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=reason,
                ts_event=ts_event,
            )

    def _submit_outcome_unknown(self, orders: list[Any], error: Exception) -> bool:
        if not self._is_ambiguous_submit_error(error):
            return False

        client_order_ids = ", ".join(order.client_order_id.value for order in orders)
        self._log.warning(
            "Alpaca submit outcome unknown "
            f"for [{client_order_ids}]; leaving SUBMITTED for reconciliation ({error})",
        )
        return True

    @staticmethod
    def _is_ambiguous_submit_error(error: Exception) -> bool:
        if isinstance(error, (TimeoutError, OSError)):
            return True

        error_text = str(error).lower()
        return (
            "timeout" in error_text
            or "timed out" in error_text
            or "[408]" in error_text
            or "[504]" in error_text
        )

    def _apply_cached_order_report_metadata(self, order: Any, report: OrderStatusReport) -> None:
        if getattr(order, "order_list_id", None) is not None:
            report.order_list_id = order.order_list_id
        if getattr(order, "linked_order_ids", None) is not None:
            report.linked_order_ids = list(order.linked_order_ids)
        if getattr(order, "parent_order_id", None) is not None:
            report.parent_order_id = order.parent_order_id
        if getattr(order, "contingency_type", None) is not None:
            report.contingency_type = order.contingency_type

    def _cached_order_for_report(
        self,
        *,
        client_order_id: ClientOrderId | None,
        venue_order_id: VenueOrderId | None,
    ) -> Any:
        if client_order_id is not None:
            cached_order = self._cache.order(client_order_id)
            if cached_order is not None:
                return cached_order

        if venue_order_id is not None:
            resolved_id = self._cache.client_order_id(venue_order_id)
            if resolved_id is not None:
                return self._cache.order(resolved_id)

        return None

    async def _load_instrument_for_symbol(self, symbol: str) -> Any:
        instrument = self._instrument_provider.instrument_for_symbol(symbol)
        if instrument is not None:
            return instrument

        instrument_id = symbol_to_instrument_id(symbol)
        await self._instrument_provider.load_async(instrument_id)
        return self._instrument_provider.find(instrument_id)

    async def _ensure_instrument(self, instrument_id) -> Any:
        instrument = self._cache.instrument(instrument_id)
        if instrument is not None:
            return instrument

        instrument = self._instrument_provider.find(instrument_id)
        if instrument is not None:
            return instrument

        await self._instrument_provider.load_async(instrument_id)
        return self._instrument_provider.find(instrument_id)

    def _order_reports_from_payload(
        self,
        order: dict[str, Any],
        instrument,
    ) -> list[OrderStatusReport]:
        parent_report = order_to_report(self.account_id, instrument, order)
        cached_parent = self._cached_order_for_report(
            client_order_id=parent_report.client_order_id,
            venue_order_id=parent_report.venue_order_id,
        )
        if cached_parent is not None:
            self._apply_cached_order_report_metadata(cached_parent, parent_report)

        reports = [parent_report]
        reports.extend(
            self._nested_child_reports(
                order=order,
                instrument=instrument,
                cached_parent=cached_parent,
            ),
        )
        return reports

    def _nested_child_reports(
        self,
        *,
        order: dict[str, Any],
        instrument,
        cached_parent,
    ) -> list[OrderStatusReport]:
        matched_legs = self._matched_nested_leg_orders(order, cached_parent)
        reports: list[OrderStatusReport] = []

        for cached_order, leg in matched_legs:
            report = order_to_report(
                self.account_id,
                instrument,
                leg,
                client_order_id=cached_order.client_order_id,
            )
            self._apply_cached_order_report_metadata(cached_order, report)
            reports.append(report)

        return reports

    def _matched_nested_leg_orders(
        self,
        order: dict[str, Any],
        cached_parent,
    ) -> list[tuple[Any, dict[str, Any]]]:
        legs = order.get("legs")
        if not isinstance(legs, list) or cached_parent is None:
            return []

        stop_order, take_profit_order = self._linked_child_orders_for_nested_report(cached_parent)
        stop_leg = next((leg for leg in legs if leg.get("type") in {"stop", "stop_limit"}), None)
        take_profit_leg = next((leg for leg in legs if leg.get("type") == "limit"), None)

        matched: list[tuple[Any, dict[str, Any]]] = []
        if stop_order is not None and stop_leg is not None:
            matched.append((stop_order, stop_leg))
        if take_profit_order is not None and take_profit_leg is not None:
            matched.append((take_profit_order, take_profit_leg))

        return matched

    def _linked_child_orders_for_nested_report(self, cached_parent) -> tuple[Any, Any]:
        linked_order_ids = list(getattr(cached_parent, "linked_order_ids", []) or [])
        linked_orders = [
            order
            for client_order_id in linked_order_ids
            if (order := self._cache.order(client_order_id)) is not None
        ]

        stop_order = next(
            (
                order
                for order in linked_orders
                if order.order_type in {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}
            ),
            None,
        )
        take_profit_order = next(
            (order for order in linked_orders if order.order_type == OrderType.LIMIT),
            None,
        )

        return stop_order, take_profit_order

    def _open_orders_for_cancel_all(self, instrument_id, order_side: OrderSide) -> list[Any]:
        return list(
            self._cache.orders_open(
                venue=self.venue,
                instrument_id=instrument_id,
                side=order_side,
            ),
        )

    async def _submit_contingent_order_list(self, command: SubmitOrderList, kind: str) -> None:
        instrument = await self._ensure_instrument(command.order_list.instrument_id)
        if instrument is None:
            self._deny_order_list_pre_submit(command.order_list.orders, "INSTRUMENT_NOT_FOUND")
            return

        validation_error = self._validate_contingent_order_list(
            order_list=command.order_list,
            instrument=instrument,
            kind=kind,
            params=command.params,
        )
        if validation_error is not None:
            self._deny_order_list_pre_submit(command.order_list.orders, validation_error)
            return

        for order in command.order_list.orders:
            self.generate_order_submitted(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                ts_event=self._clock.timestamp_ns(),
            )

        try:
            payload = self._build_contingent_order_list_payload(
                order_list=command.order_list,
                instrument=instrument,
                kind=kind,
                params=command.params,
            )
            venue_order = await self._client.submit_order(payload)
            self._generate_contingent_order_accepts(
                order_list=command.order_list,
                instrument=instrument,
                venue_order=venue_order,
                kind=kind,
            )
        except Exception as e:
            if self._submit_outcome_unknown(list(command.order_list.orders), e):
                return
            self._reject_order_list_submit(command.order_list.orders, str(e))

    async def _submit_order_inner(self, order, params: dict[str, Any] | None) -> None:
        instrument = await self._ensure_instrument(order.instrument_id)

        if instrument is None:
            self._deny_order_pre_submit(order, "INSTRUMENT_NOT_FOUND")
            return

        validation_error = self._validate_order(order, instrument)
        if validation_error is not None:
            self._deny_order_pre_submit(order, validation_error)
            return
        params_error = self._validate_submit_params(instrument, params)
        if params_error is not None:
            self._deny_order_pre_submit(order, params_error)
            return

        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

        try:
            payload = self._build_submit_payload(order, instrument, params)
            venue_order = await self._client.submit_order(payload)
            report = order_to_report(
                self.account_id,
                instrument,
                venue_order,
                client_order_id=order.client_order_id,
            )
            self.generate_order_accepted(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=report.venue_order_id,
                ts_event=report.ts_accepted,
            )
        except Exception as e:
            if self._submit_outcome_unknown([order], e):
                return
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=str(e),
                ts_event=self._clock.timestamp_ns(),
            )

    def _validate_contingent_order_list(
        self,
        *,
        order_list,
        instrument,
        kind: str,
        params: dict[str, Any] | None,
    ) -> str | None:
        capability_error = self._validate_contingent_order_list_capabilities(
            instrument=instrument,
            params=params,
        )
        if capability_error is not None:
            return capability_error

        primary_order, stop_order, take_profit_order = self._contingent_order_list_components(
            order_list=order_list,
            kind=kind,
        )
        shape_error = self._validate_contingent_order_list_shape(
            kind=kind,
            primary_order=primary_order,
            stop_order=stop_order,
            take_profit_order=take_profit_order,
        )
        if shape_error is not None:
            return shape_error

        primary_error = self._validate_contingent_primary_order(
            kind=kind,
            primary_order=primary_order,
            instrument=instrument,
        )
        if primary_error is not None:
            return primary_error

        side_error = self._validate_contingent_order_sides(
            kind=kind,
            primary_order=primary_order,
            stop_order=stop_order,
            take_profit_order=take_profit_order,
        )
        if side_error is not None:
            return side_error

        return self._validate_contingent_child_orders(
            primary_order=primary_order,
            stop_order=stop_order,
            take_profit_order=take_profit_order,
            instrument=instrument,
        )

    @staticmethod
    def _validate_contingent_order_list_capabilities(
        *,
        instrument,
        params: dict[str, Any] | None,
    ) -> str | None:
        if not is_equity_instrument(instrument):
            return "ALPACA_ADVANCED_ORDER_LIST_EQUITIES_ONLY"
        if params and params.get("extended_hours"):
            return "ALPACA_ADVANCED_ORDER_LIST_EXTENDED_HOURS_NOT_SUPPORTED"
        return None

    @staticmethod
    def _validate_contingent_order_list_shape(
        *,
        kind: str,
        primary_order,
        stop_order,
        take_profit_order,
    ) -> str | None:
        if primary_order is None:
            return "ALPACA_CONTINGENT_ORDER_LIST_SHAPE_UNSUPPORTED"
        if kind == "bracket" and (stop_order is None or take_profit_order is None):
            return "ALPACA_BRACKET_ORDER_LIST_SHAPE_UNSUPPORTED"
        if kind == "oto" and stop_order is None and take_profit_order is None:
            return "ALPACA_OTO_ORDER_LIST_SHAPE_UNSUPPORTED"
        if kind == "oco" and (stop_order is None or take_profit_order is None):
            return "ALPACA_OCO_ORDER_LIST_SHAPE_UNSUPPORTED"
        return None

    def _validate_contingent_primary_order(
        self,
        *,
        kind: str,
        primary_order,
        instrument,
    ) -> str | None:
        if kind == "oco":
            return self._validate_advanced_take_profit_order(
                primary_order,
                instrument,
                primary_order.quantity,
            )

        if primary_order.order_type not in {OrderType.MARKET, OrderType.LIMIT}:
            return "ALPACA_ADVANCED_PRIMARY_ORDER_TYPE_UNSUPPORTED"

        primary_error = self._validate_order(primary_order, instrument)
        if primary_error is not None:
            return primary_error
        if primary_order.time_in_force not in {TimeInForce.DAY, TimeInForce.GTC}:
            return "ALPACA_ADVANCED_TIME_IN_FORCE_UNSUPPORTED"

        return None

    @staticmethod
    def _validate_contingent_order_sides(
        *,
        kind: str,
        primary_order,
        stop_order,
        take_profit_order,
    ) -> str | None:
        if kind == "oco":
            if stop_order is not None and stop_order.side != primary_order.side:
                return "ALPACA_OCO_ORDER_LIST_SIDE_MISMATCH"
            return None

        child_orders = [order for order in (stop_order, take_profit_order) if order is not None]
        if any(order.side == primary_order.side for order in child_orders):
            return "ALPACA_ADVANCED_CHILD_SIDE_INVALID"

        return None

    def _validate_contingent_child_orders(
        self,
        *,
        primary_order,
        stop_order,
        take_profit_order,
        instrument,
    ) -> str | None:
        if stop_order is not None:
            stop_error = self._validate_advanced_stop_order(stop_order, instrument, primary_order.quantity)
            if stop_error is not None:
                return stop_error

        if take_profit_order is not None:
            tp_error = self._validate_advanced_take_profit_order(
                take_profit_order,
                instrument,
                primary_order.quantity,
            )
            if tp_error is not None:
                return tp_error

        return None

    def _validate_advanced_stop_order(self, order, instrument, quantity) -> str | None:
        if order.order_type not in {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}:
            return "ALPACA_ADVANCED_STOP_ORDER_TYPE_UNSUPPORTED"
        if order.quantity != quantity:
            return "ALPACA_ADVANCED_CHILD_QUANTITY_MISMATCH"
        if order.is_quote_quantity:
            return "ALPACA_ADVANCED_CHILD_QUOTE_QUANTITY_UNSUPPORTED"
        if not order.is_reduce_only:
            return "ALPACA_ADVANCED_CHILD_REDUCE_ONLY_REQUIRED"
        if order.time_in_force not in {TimeInForce.DAY, TimeInForce.GTC}:
            return "ALPACA_ADVANCED_TIME_IN_FORCE_UNSUPPORTED"
        if order.trigger_price is None:
            return "ALPACA_ADVANCED_STOP_TRIGGER_PRICE_REQUIRED"
        if order.order_type == OrderType.STOP_LIMIT and order.price is None:
            return "ALPACA_ADVANCED_STOP_LIMIT_PRICE_REQUIRED"
        if getattr(order, "is_post_only", False):
            return "ALPACA_POST_ONLY_NOT_SUPPORTED"
        if not is_equity_instrument(instrument):
            return "ALPACA_ADVANCED_ORDER_LIST_EQUITIES_ONLY"
        return None

    def _validate_advanced_take_profit_order(self, order, instrument, quantity) -> str | None:
        if order.order_type != OrderType.LIMIT:
            return "ALPACA_ADVANCED_TAKE_PROFIT_ORDER_TYPE_UNSUPPORTED"
        if order.quantity != quantity:
            return "ALPACA_ADVANCED_CHILD_QUANTITY_MISMATCH"
        if order.is_quote_quantity:
            return "ALPACA_ADVANCED_CHILD_QUOTE_QUANTITY_UNSUPPORTED"
        if not order.is_reduce_only:
            return "ALPACA_ADVANCED_CHILD_REDUCE_ONLY_REQUIRED"
        if order.time_in_force not in {TimeInForce.DAY, TimeInForce.GTC}:
            return "ALPACA_ADVANCED_TIME_IN_FORCE_UNSUPPORTED"
        if order.price is None:
            return "ALPACA_ADVANCED_TAKE_PROFIT_LIMIT_PRICE_REQUIRED"
        if getattr(order, "is_post_only", False):
            return "ALPACA_POST_ONLY_NOT_SUPPORTED"
        if not is_equity_instrument(instrument):
            return "ALPACA_ADVANCED_ORDER_LIST_EQUITIES_ONLY"
        return None

    @staticmethod
    def _contingent_order_list_components(order_list, kind: str) -> tuple[Any, Any, Any]:
        orders = list(order_list.orders)

        if kind == "bracket":
            primary_order = order_list.first
            stop_order = next(
                (order for order in orders[1:] if order.order_type in {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}),
                None,
            )
            take_profit_order = next(
                (order for order in orders[1:] if order.order_type == OrderType.LIMIT),
                None,
            )
            return primary_order, stop_order, take_profit_order

        if kind == "oto":
            primary_order = orders[0]
            child_order = orders[1]
            if child_order.order_type == OrderType.LIMIT:
                return primary_order, None, child_order
            if child_order.order_type in {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}:
                return primary_order, child_order, None
            return primary_order, None, None

        if kind == "oco":
            stop_order = next(
                (order for order in orders if order.order_type in {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}),
                None,
            )
            take_profit_order = next((order for order in orders if order.order_type == OrderType.LIMIT), None)
            return take_profit_order, stop_order, take_profit_order

        return None, None, None

    def _build_contingent_order_list_payload(
        self,
        *,
        order_list,
        instrument,
        kind: str,
        params: dict[str, Any] | None,
    ) -> dict[str, Any]:
        primary_order, stop_order, take_profit_order = self._contingent_order_list_components(
            order_list=order_list,
            kind=kind,
        )
        payload = self._build_submit_payload(primary_order, instrument, params)
        payload["order_class"] = kind

        if take_profit_order is not None and kind in {"bracket", "oto"}:
            payload["take_profit"] = {"limit_price": str(take_profit_order.price)}

        if stop_order is not None:
            stop_payload = {"stop_price": str(stop_order.trigger_price)}
            if stop_order.order_type == OrderType.STOP_LIMIT:
                stop_payload["limit_price"] = str(stop_order.price)
            payload["stop_loss"] = stop_payload

        return payload

    def _generate_contingent_order_accepts(
        self,
        *,
        order_list,
        instrument,
        venue_order: dict[str, Any],
        kind: str,
    ) -> None:
        primary_order, stop_order, take_profit_order = self._contingent_order_list_components(
            order_list=order_list,
            kind=kind,
        )
        self._generate_order_accept(primary_order, instrument, venue_order)

        legs = venue_order.get("legs")
        if not isinstance(legs, list):
            if len(order_list.orders) > 1:
                self._log.warning("Alpaca advanced order response missing child legs")
            return

        stop_leg = next((leg for leg in legs if leg.get("type") in {"stop", "stop_limit"}), None)
        take_profit_leg = next((leg for leg in legs if leg.get("type") == "limit"), None)

        if stop_order is not None and stop_leg is not None:
            self._generate_order_accept(stop_order, instrument, stop_leg)
        if take_profit_order is not None and take_profit_leg is not None:
            self._generate_order_accept(take_profit_order, instrument, take_profit_leg)

    def _generate_order_accept(self, order, instrument, order_payload: dict[str, Any]) -> None:
        report = order_to_report(
            self.account_id,
            instrument,
            order_payload,
            client_order_id=order.client_order_id,
        )
        self.generate_order_accepted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=report.venue_order_id,
            ts_event=report.ts_accepted,
        )

    def _remember_processed_trade_id(self, trade_id: str) -> bool:
        if trade_id in self._processed_trade_ids:
            return False

        self._processed_trade_ids.add(trade_id)
        self._processed_trade_queue.append(trade_id)
        while len(self._processed_trade_ids) > self._processed_trade_id_limit:
            self._processed_trade_ids.discard(self._processed_trade_queue.popleft())

        return True

    async def _handle_ws_disconnect(self, error: Exception | None) -> None:
        self._ws_client = None
        if self._is_disconnecting:
            return
        self._log.warning(f"Alpaca execution websocket disconnected, reconnecting ({error or 'closed'})")
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = self._loop.create_task(self._reconnect_ws())

    async def _reconnect_ws(self) -> None:
        while not self._is_disconnecting:
            try:
                await self._ensure_ws_connected()
                return
            except Exception as exc:
                self._log.warning(f"Alpaca execution websocket reconnect failed: {exc}")
                await asyncio.sleep(1.0)

    async def _ensure_ws_connected(self) -> None:
        if self._ws_client is not None and not self._ws_client.is_closed():
            return

        url = self._config.trading_ws_url or (
            ALPACA_PAPER_TRADING_WS_URL if self._config.paper else ALPACA_LIVE_TRADING_WS_URL
        )
        self._ws_client = AlpacaWebSocketClient(url=url, headers=self._client.auth_headers)
        try:
            await self._ws_client.connect(
                self._loop,
                self._handle_msg,
                handler_disconnect=self._handle_ws_disconnect,
            )
            await self._ws_client.send_json(
                {
                    "action": "authenticate",
                    "data": {
                        "key_id": self._client.api_key,
                        "secret_key": self._client.api_secret,
                    },
                },
            )
            await self._ws_client.send_json(
                {
                    "action": "listen",
                    "data": {"streams": ["trade_updates"]},
                },
            )
        except Exception:
            await self._ws_client.close()
            self._ws_client = None
            raise

    async def _update_account_state(self) -> None:
        account = await self._client.get_account()
        account_id = AccountId(f"{self.id.value}-{account['account_number']}")
        self._set_account_id(account_id)
        detected_account_type = account_type_from_account(account)
        if detected_account_type != self.account_type:
            self._log.warning(
                "Configured Alpaca account_type does not match account payload; "
                f"configured={self.account_type.name}, detected={detected_account_type.name}",
            )
        self.generate_account_state(
            balances=[account_balance_from_account(account)],
            margins=[],
            reported=True,
            ts_event=self._clock.timestamp_ns(),
            info=account,
        )

    def _handle_msg(self, msg: dict[str, Any]) -> None:
        parsed = self._parse_trade_update_message(msg)
        if parsed is None:
            return

        event, data, order_payload = parsed
        venue_order_id = VenueOrderId(order_payload["id"])
        client_order_id, cached_order = self._resolve_cached_order(venue_order_id, order_payload)
        if cached_order is None or client_order_id is None:
            self._log.debug(f"Ignoring external Alpaca order event {event} for {venue_order_id}")
            return

        instrument = self._cache.instrument(cached_order.instrument_id)
        if instrument is None:
            instrument = self._instrument_provider.find(cached_order.instrument_id)
        if instrument is None:
            instrument = self._instrument_provider.instrument_for_symbol(order_payload["symbol"])
        if instrument is None:
            self._log.warning(
                "Ignoring Alpaca order event due to unknown instrument "
                f"{cached_order.instrument_id} / {order_payload['symbol']}",
            )
            return

        ts_event = get_timestamp_ns(
            data.get("timestamp")
            or order_payload.get("updated_at")
            or order_payload.get("filled_at")
            or order_payload.get("submitted_at"),
        )

        self._dispatch_trade_update(
            event=event,
            cached_order=cached_order,
            client_order_id=client_order_id,
            data=data,
            instrument=instrument,
            order_payload=order_payload,
            ts_event=ts_event,
            venue_order_id=venue_order_id,
        )

    def _dispatch_trade_update(
        self,
        *,
        event: str,
        cached_order,
        client_order_id: ClientOrderId,
        data: dict[str, Any],
        instrument,
        order_payload: dict[str, Any],
        ts_event: int,
        venue_order_id: VenueOrderId,
    ) -> None:

        if event in {"accepted", "pending_new", "new"}:
            self._generate_accept_if_needed(cached_order, client_order_id, venue_order_id, ts_event)
            return

        if event == "replaced":
            self._handle_replace_event(
                cached_order=cached_order,
                client_order_id=client_order_id,
                instrument=instrument,
                order_payload=order_payload,
            )
            return

        if event in {"partial_fill", "fill"}:
            self._handle_fill_event(
                cached_order=cached_order,
                client_order_id=client_order_id,
                data=data,
                instrument=instrument,
                order_payload=order_payload,
                ts_event=ts_event,
                venue_order_id=venue_order_id,
            )
            return

        if event == "canceled":
            self._handle_terminal_event(
                event=event,
                cached_order=cached_order,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=ts_event,
                data=data,
            )
            return

        if event in {"expired", "rejected"}:
            self._handle_terminal_event(
                event=event,
                cached_order=cached_order,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=ts_event,
                data=data,
            )

    def _validate_order(self, order, instrument) -> str | None:
        if order.is_post_only:
            return "ALPACA_POST_ONLY_NOT_SUPPORTED"
        if order.is_reduce_only:
            return "ALPACA_REDUCE_ONLY_NOT_SUPPORTED"
        if order.order_type not in self._supported_order_types(instrument):
            return f"ALPACA_ORDER_TYPE_UNSUPPORTED:{order.order_type.name}"

        quantity_error = self._validate_quantity_constraints(order, instrument)
        if quantity_error is not None:
            return quantity_error

        return self._validate_time_in_force(order, instrument)

    @staticmethod
    def _submit_payload_context(
        command_or_order,
        params: dict[str, Any] | None,
    ) -> tuple[Any, dict[str, Any] | None]:
        if hasattr(command_or_order, "order"):
            if params is None:
                params = getattr(command_or_order, "params", None)
            return command_or_order.order, params
        return command_or_order, params

    def _build_submit_payload(
        self,
        command_or_order,
        instrument,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        order, params = self._submit_payload_context(command_or_order, params)

        payload: dict[str, Any] = {
            "symbol": data_symbol_for_instrument(instrument),
            "side": order.side.name.lower(),
            "type": self._alpaca_order_type(order.order_type),
            "time_in_force": self._alpaca_time_in_force(order.time_in_force),
            "client_order_id": order.client_order_id.value,
        }
        if order.is_quote_quantity:
            payload["notional"] = str(order.quantity)
        else:
            payload["qty"] = str(order.quantity)

        if order.has_price:
            payload["limit_price"] = str(order.price)
        if order.has_trigger_price:
            payload["stop_price"] = str(order.trigger_price)
        if order.order_type == OrderType.TRAILING_STOP_MARKET:
            if order.trailing_offset is None:
                raise ValueError("Trailing stop orders require trailing_offset")
            if order.trailing_offset_type is None:
                raise ValueError("Trailing stop orders require trailing_offset_type")
            if order.trailing_offset_type.name == "PRICE":
                payload["trail_price"] = str(order.trailing_offset)
            else:
                payload["trail_percent"] = str(
                    Decimal(str(order.trailing_offset)) / 100,
                )

        extended_hours = params.get("extended_hours") if params else None
        if extended_hours is not None:
            payload["extended_hours"] = bool(extended_hours)

        return payload

    @staticmethod
    def _alpaca_order_type(order_type: OrderType) -> str:
        mapping = {
            OrderType.MARKET: "market",
            OrderType.LIMIT: "limit",
            OrderType.STOP_MARKET: "stop",
            OrderType.STOP_LIMIT: "stop_limit",
            OrderType.TRAILING_STOP_MARKET: "trailing_stop",
        }
        return mapping[order_type]

    @staticmethod
    def _alpaca_time_in_force(time_in_force: TimeInForce) -> str:
        mapping = {
            TimeInForce.DAY: "day",
            TimeInForce.GTC: "gtc",
            TimeInForce.AT_THE_OPEN: "opg",
            TimeInForce.AT_THE_CLOSE: "cls",
            TimeInForce.IOC: "ioc",
            TimeInForce.FOK: "fok",
        }
        return mapping[time_in_force]

    async def _list_orders_paginated(
        self,
        *,
        status: str,
        after: str | None,
        until: str | None,
        symbols: list[str] | None,
        nested: bool = False,
    ) -> list[dict[str, Any]]:
        cursor_until = until
        orders_by_id: dict[str, dict[str, Any]] = {}

        while True:
            payload = await self._client.list_orders(
                status=status,
                after=after,
                until=cursor_until,
                limit=500,
                symbols=symbols,
                nested=nested,
                direction="desc",
            )
            if not payload:
                break

            for order in payload:
                order_id = order.get("id")
                if order_id is None:
                    continue
                orders_by_id.setdefault(str(order_id), order)

            if len(payload) < 500:
                break

            next_until = self._next_order_until(payload)
            if next_until is None or next_until == cursor_until:
                break
            if after is not None and pd.Timestamp(next_until) <= pd.Timestamp(after):
                break
            cursor_until = next_until

        return list(orders_by_id.values())

    async def _get_activities_paginated(
        self,
        *,
        activity_type: str,
        after: str | None,
        until: str | None,
    ) -> list[dict[str, Any]]:
        page_token: str | None = None
        activities: list[dict[str, Any]] = []

        while True:
            payload = await self._client.get_activities(
                activity_type=activity_type,
                after=after,
                until=until,
                page_size=100,
                direction="desc",
                page_token=page_token,
            )
            if not payload:
                break
            activities.extend(payload)
            if len(payload) < 100:
                break
            next_page_token = self._next_activity_page_token(payload)
            if next_page_token is None or next_page_token == page_token:
                break
            page_token = next_page_token

        return activities

    def _resolve_cached_order(
        self,
        venue_order_id: VenueOrderId,
        order_payload: dict[str, Any],
    ) -> tuple[ClientOrderId | None, Any]:
        client_order_id = ClientOrderId(order_payload["client_order_id"])
        cached_order = self._cache.order(client_order_id)
        if cached_order is not None:
            return client_order_id, cached_order

        resolved_id = self._cache.client_order_id(venue_order_id)
        if resolved_id is None:
            return None, None

        return resolved_id, self._cache.order(resolved_id)

    def _generate_accept_if_needed(
        self,
        cached_order,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        ts_event: int,
    ) -> None:
        if self._cache.venue_order_id(client_order_id) is not None:
            return

        self.generate_order_accepted(
            strategy_id=cached_order.strategy_id,
            instrument_id=cached_order.instrument_id,
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            ts_event=ts_event,
        )

    def _handle_replace_event(
        self,
        *,
        cached_order,
        client_order_id: ClientOrderId,
        instrument,
        order_payload: dict[str, Any],
    ) -> None:
        previous_venue_order_id = self._cache.venue_order_id(client_order_id)
        report = order_to_report(
            self.account_id,
            instrument,
            order_payload,
            client_order_id=client_order_id,
        )
        self.generate_order_updated(
            strategy_id=cached_order.strategy_id,
            instrument_id=cached_order.instrument_id,
            client_order_id=client_order_id,
            venue_order_id=report.venue_order_id,
            quantity=report.quantity,
            price=report.price,
            trigger_price=report.trigger_price,
            ts_event=report.ts_last,
            venue_order_id_modified=(
                previous_venue_order_id is not None and report.venue_order_id != previous_venue_order_id
            ),
        )

    def _handle_fill_event(
        self,
        *,
        cached_order,
        client_order_id: ClientOrderId,
        data: dict[str, Any],
        instrument,
        order_payload: dict[str, Any],
        ts_event: int,
        venue_order_id: VenueOrderId,
    ) -> None:
        trade_id_value = str(
            data.get("execution_id")
            or data.get("at")
            or data.get("timestamp")
            or f"{venue_order_id.value}-{ts_event}"
        )
        if not self._remember_processed_trade_id(trade_id_value):
            return

        self._generate_accept_if_needed(cached_order, client_order_id, venue_order_id, ts_event)

        order_type = ALPACA_ORDER_TYPE[order_payload["type"]]
        self.generate_order_filled(
            strategy_id=cached_order.strategy_id,
            instrument_id=cached_order.instrument_id,
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            venue_position_id=None,
            trade_id=TradeId(trade_id_value),
            order_side=cached_order.side,
            order_type=order_type,
            last_qty=instrument.make_qty(data["qty"]),
            last_px=instrument.make_price(data["price"]),
            quote_currency=quote_currency_for_instrument(instrument),
            commission=Money(0, quote_currency_for_instrument(instrument)),
            liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
            ts_event=ts_event,
            info=data,
        )

    def _handle_terminal_event(
        self,
        *,
        event: str,
        cached_order,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        ts_event: int,
        data: dict[str, Any],
    ) -> None:
        if event == "canceled":
            self.generate_order_canceled(
                strategy_id=cached_order.strategy_id,
                instrument_id=cached_order.instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=ts_event,
            )
            return

        if event == "expired":
            self.generate_order_expired(
                strategy_id=cached_order.strategy_id,
                instrument_id=cached_order.instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=ts_event,
            )
            return

        self.generate_order_rejected(
            strategy_id=cached_order.strategy_id,
            instrument_id=cached_order.instrument_id,
            client_order_id=client_order_id,
            reason=str(data.get("reason") or "REJECTED"),
            ts_event=ts_event,
        )

    @staticmethod
    def _parse_trade_update_message(
        msg: dict[str, Any],
    ) -> tuple[str, dict[str, Any], dict[str, Any]] | None:
        stream = msg.get("stream")
        if stream in {"authorization", "listening"} or stream != "trade_updates":
            return None

        data = msg.get("data")
        if not isinstance(data, dict):
            return None

        order_payload = data.get("order")
        if not isinstance(order_payload, dict):
            return None

        return str(data.get("event") or ""), data, order_payload

    @staticmethod
    def _supported_order_types(instrument) -> set[OrderType]:
        if is_option_instrument(instrument):
            return {
                OrderType.MARKET,
                OrderType.LIMIT,
            }
        if is_crypto_instrument(instrument):
            return {
                OrderType.MARKET,
                OrderType.LIMIT,
                OrderType.STOP_LIMIT,
            }
        return {
            OrderType.MARKET,
            OrderType.LIMIT,
            OrderType.STOP_MARKET,
            OrderType.STOP_LIMIT,
            OrderType.TRAILING_STOP_MARKET,
        }

    @staticmethod
    def _validate_quantity_constraints(order, instrument) -> str | None:
        if is_option_instrument(instrument):
            if order.is_quote_quantity:
                return "ALPACA_OPTION_NOTIONAL_UNSUPPORTED"

            quantity = Decimal(str(order.quantity))
            if quantity != quantity.to_integral_value():
                return "ALPACA_OPTION_QUANTITY_MUST_BE_WHOLE_NUMBER"
            return None

        if is_equity_instrument(instrument):
            if order.is_quote_quantity and order.time_in_force != TimeInForce.DAY:
                return "ALPACA_EQUITY_NOTIONAL_TIF_UNSUPPORTED"

            quantity = Decimal(str(order.quantity))
            if not order.is_quote_quantity and quantity != quantity.to_integral_value():
                return "ALPACA_FRACTIONAL_EQUITIES_NOT_SUPPORTED_BY_NAUTILUS_EQUITY_MODEL"

        return None

    @staticmethod
    def _validate_time_in_force(order, instrument) -> str | None:
        if is_option_instrument(instrument):
            return None if order.time_in_force == TimeInForce.DAY else "ALPACA_OPTION_TIF_UNSUPPORTED"

        if is_crypto_instrument(instrument):
            return (
                None
                if order.time_in_force in {TimeInForce.GTC, TimeInForce.IOC}
                else "ALPACA_CRYPTO_TIF_UNSUPPORTED"
            )

        return (
            None
            if order.time_in_force
            in {
                TimeInForce.DAY,
                TimeInForce.GTC,
                TimeInForce.AT_THE_OPEN,
                TimeInForce.AT_THE_CLOSE,
                TimeInForce.IOC,
                TimeInForce.FOK,
            }
            else "ALPACA_EQUITY_TIF_UNSUPPORTED"
        )

    @staticmethod
    def _validate_submit_params(instrument, params: dict[str, Any] | None) -> str | None:
        if not params or not params.get("extended_hours"):
            return None
        if not is_equity_instrument(instrument):
            return "ALPACA_EXTENDED_HOURS_EQUITIES_ONLY"
        return None

    @staticmethod
    def _next_order_until(payload: list[dict[str, Any]]) -> str | None:
        for order in reversed(payload):
            cursor = order.get("submitted_at") or order.get("created_at") or order.get("updated_at")
            if cursor:
                return str(cursor)
        return None

    @staticmethod
    def _next_activity_page_token(payload: list[dict[str, Any]]) -> str | None:
        if not payload:
            return None
        token = payload[-1].get("id")
        return str(token) if token else None

    @staticmethod
    def _timestamp_to_iso(value: Any) -> str | None:
        if value is None:
            return None
        return pd.Timestamp(value).isoformat()
