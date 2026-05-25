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
import hashlib
import json
import time
from collections import deque
from datetime import timedelta
from decimal import Decimal
from typing import Any

from nautilus_trader.adapters.lighter.config import LighterExecClientConfig
from nautilus_trader.adapters.lighter.constants import LIGHTER_DEFAULT_MARKET_SLIPPAGE
from nautilus_trader.adapters.lighter.constants import LIGHTER_DEFAULT_ORDER_EXPIRY_SECS
from nautilus_trader.adapters.lighter.constants import LIGHTER_LIMIT_ORDER
from nautilus_trader.adapters.lighter.constants import LIGHTER_MARGIN_MODE_CROSS
from nautilus_trader.adapters.lighter.constants import LIGHTER_MARKET_ORDER
from nautilus_trader.adapters.lighter.constants import LIGHTER_MAX_BATCH_TX_COUNT
from nautilus_trader.adapters.lighter.constants import LIGHTER_MAX_CLIENT_ORDER_INDEX
from nautilus_trader.adapters.lighter.constants import LIGHTER_STOP_LOSS_LIMIT_ORDER
from nautilus_trader.adapters.lighter.constants import LIGHTER_STOP_LOSS_ORDER
from nautilus_trader.adapters.lighter.constants import LIGHTER_TAKE_PROFIT_LIMIT_ORDER
from nautilus_trader.adapters.lighter.constants import LIGHTER_TAKE_PROFIT_ORDER
from nautilus_trader.adapters.lighter.constants import LIGHTER_TIF_GTT
from nautilus_trader.adapters.lighter.constants import LIGHTER_TIF_IOC
from nautilus_trader.adapters.lighter.constants import LIGHTER_TIF_POST_ONLY
from nautilus_trader.adapters.lighter.constants import LIGHTER_UPDATE_MARGIN_ADD
from nautilus_trader.adapters.lighter.constants import LIGHTER_VENUE
from nautilus_trader.adapters.lighter.parsing import account_balances_from_assets
from nautilus_trader.adapters.lighter.parsing import datetime_to_nanos
from nautilus_trader.adapters.lighter.parsing import fill_report_from_lighter_trade
from nautilus_trader.adapters.lighter.parsing import loads
from nautilus_trader.adapters.lighter.parsing import margin_balances_from_positions
from nautilus_trader.adapters.lighter.parsing import order_report_from_lighter
from nautilus_trader.adapters.lighter.parsing import position_report_from_lighter
from nautilus_trader.adapters.lighter.parsing import to_lighter_price
from nautilus_trader.adapters.lighter.parsing import to_lighter_size
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import LighterEnvironment
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
from nautilus_trader.execution.reports import ExecutionMassStatus
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import ContingencyType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import VenueOrderId


class LighterExecutionClient(LiveExecutionClient):
    """
    Provides an execution client for the Lighter exchange.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider,
        config: LighterExecClientConfig,
        name: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or LIGHTER_VENUE.value),
            venue=LIGHTER_VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            base_currency=None,
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )

        self._client = client
        self._instrument_provider = instrument_provider
        self._config = config
        self._account_index = int(config.account_index or 0)
        self._set_account_id(AccountId(f"{name or LIGHTER_VENUE.value}-{self._account_index}"))

        self._ws_client = None
        self._auth_token: str | None = None
        self._auth_token_expires_at: float = 0.0

        self._client_order_index_to_id: dict[int, ClientOrderId] = {}
        self._client_order_id_to_index: dict[ClientOrderId, int] = {}
        self._venue_order_id_by_client_order_id: dict[ClientOrderId, VenueOrderId] = {}
        self._recent_order_states: dict[str, tuple[str, int, str]] = {}
        self._recent_order_state_queue: deque[str] = deque(maxlen=10_000)
        self._processed_trade_ids: set[str] = set()
        self._processed_trade_queue: deque[str] = deque(maxlen=10_000)

    def _in_window(self, ts_event: int, start, end) -> bool:
        start_ns = datetime_to_nanos(start)
        end_ns = datetime_to_nanos(end)
        if start_ns is not None and ts_event < start_ns:
            return False
        return not (end_ns is not None and ts_event > end_ns)

    def _flatten_ws_values(self, payload: Any) -> list[dict[str, Any]]:
        if payload is None:
            return []
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            flattened: list[dict[str, Any]] = []
            for value in payload.values():
                if isinstance(value, list):
                    flattened.extend(item for item in value if isinstance(item, dict))
                elif isinstance(value, dict):
                    flattened.append(value)
            return flattened
        return []

    def _iter_grouped_values(self, payload: Any) -> list[tuple[int, dict[str, Any]]]:
        if not isinstance(payload, dict):
            return []
        grouped: list[tuple[int, dict[str, Any]]] = []
        for market_key, value in payload.items():
            try:
                market_id = int(market_key)
            except (TypeError, ValueError):
                continue
            if isinstance(value, list):
                grouped.extend((market_id, item) for item in value if isinstance(item, dict))
            elif isinstance(value, dict):
                grouped.append((market_id, value))
        return grouped

    async def _connect(self) -> None:
        await self._instrument_provider.initialize()
        self._sync_client_order_index_cache()
        token = await self._ensure_auth_token()
        environment = (
            self._config.environment
            if self._config.environment is not None
            else (LighterEnvironment.TESTNET if self._config.testnet else LighterEnvironment.MAINNET)
        )

        self._ws_client = nautilus_pyo3.LighterWebSocketClient(  # type: ignore[attr-defined]
            url=self._config.base_url_ws,
            testnet=environment == LighterEnvironment.TESTNET,
            auth_token=token,
        )
        await self._ws_client.connect(self._loop, self._handle_msg)
        await self._ws_client.subscribe_account_all(self._account_index)
        await self._ws_client.subscribe_account_all_orders(self._account_index)
        await self._ws_client.subscribe_account_all_positions(self._account_index)
        await self._ws_client.subscribe_account_all_trades(self._account_index)
        await self._ws_client.subscribe_account_all_assets(self._account_index)
        await self._ws_client.subscribe_user_stats(self._account_index)

        await self._update_account_state()

    def _sync_client_order_index_cache(self) -> None:
        count = 0
        for order in self._cache.orders(venue=self.venue):
            if order.is_closed:
                continue
            client_order_index = self._lighter_client_order_index(order.client_order_id)
            self._client_order_index_to_id[client_order_index] = order.client_order_id
            self._client_order_id_to_index[order.client_order_id] = client_order_index
            if order.venue_order_id is not None:
                self._track_venue_order_id(order.client_order_id, order.venue_order_id)
            count += 1
        if count:
            self._log.info(
                f"Cached Lighter client order indexes for {count} existing order(s)",
                LogColor.BLUE,
            )

    async def _disconnect(self) -> None:
        await asyncio.sleep(0.25)
        if self._ws_client is not None and not self._ws_client.is_closed():
            await self._ws_client.close()

    async def _ensure_auth_token(self, min_ttl_secs: int = 30) -> str:
        now = time.time()
        if self._auth_token and self._auth_token_expires_at - now > min_ttl_secs:
            return self._auth_token

        ttl_secs = max(int(self._config.default_auth_token_ttl_secs), min_ttl_secs)
        token = await self._client.create_auth_token(
            deadline_secs=ttl_secs,
            api_key_index=self._config.api_key_index,
        )
        self._auth_token = token
        self._auth_token_expires_at = now + ttl_secs
        if self._ws_client is not None:
            await self._ws_client.set_auth_token(token)
        return token

    async def _update_account_state(self) -> None:
        token = await self._ensure_auth_token()
        payload = loads(await self._client.request_account(self._account_index, token))
        accounts = payload.get("accounts") or []
        if not accounts:
            return
        account = accounts[0]
        balances = account_balances_from_assets(account.get("assets") or [])
        positions_raw = account.get("positions") or []
        margins = margin_balances_from_positions(
            positions_raw,
            self._instrument_provider.instrument_for_market_id,
        )
        self.generate_account_state(
            balances=balances,
            margins=margins,
            reported=True,
            ts_event=self._clock.timestamp_ns(),
        )

    def _lighter_client_order_index(self, client_order_id: ClientOrderId) -> int:
        existing = self._client_order_id_to_index.get(client_order_id)
        if existing is not None:
            return existing

        digest = hashlib.blake2b(client_order_id.value.encode("utf-8"), digest_size=8).digest()
        candidate = int.from_bytes(digest, "big") & LIGHTER_MAX_CLIENT_ORDER_INDEX
        if candidate == 0:
            candidate = 1

        while candidate in self._client_order_index_to_id:
            if self._client_order_index_to_id[candidate] == client_order_id:
                return candidate
            candidate += 1
            if candidate > LIGHTER_MAX_CLIENT_ORDER_INDEX:
                candidate = 1

        self._client_order_index_to_id[candidate] = client_order_id
        self._client_order_id_to_index[client_order_id] = candidate
        return candidate

    def _resolve_client_order_id(self, numeric_value: int) -> ClientOrderId | None:
        return self._client_order_index_to_id.get(int(numeric_value))

    def _track_venue_order_id(
        self,
        client_order_id: ClientOrderId | None,
        venue_order_id: VenueOrderId | None,
    ) -> None:
        if client_order_id is None or venue_order_id is None:
            return
        self._venue_order_id_by_client_order_id[client_order_id] = venue_order_id

    def _metadata(self, instrument_id) -> dict[str, Any]:
        metadata = self._instrument_provider.metadata_for_instrument(instrument_id)
        if metadata is None:
            raise ValueError(f"No Lighter metadata cached for {instrument_id}")
        return metadata

    async def _estimate_market_price(self, order) -> Decimal:
        quote = self._cache.quote_tick(order.instrument_id)
        if quote is not None:
            return Decimal(str(quote.ask_price if order.side == OrderSide.BUY else quote.bid_price))

        market_id = self._instrument_provider.market_id_for_instrument(order.instrument_id)
        if market_id is None:
            raise ValueError(f"No market_id cached for {order.instrument_id}")

        payload = loads(await self._client.request_order_book_snapshot(market_id, limit=1))
        levels = payload.get("asks") if order.side == OrderSide.BUY else payload.get("bids")
        if not levels:
            raise ValueError(f"No order book liquidity available for {order.instrument_id}")

        base_price = Decimal(str(levels[0]["price"]))
        if order.side == OrderSide.BUY:
            return base_price * (Decimal(1) + LIGHTER_DEFAULT_MARKET_SLIPPAGE)
        return base_price * (Decimal(1) - LIGHTER_DEFAULT_MARKET_SLIPPAGE)

    def _lighter_order_type(self, order) -> int:
        if order.order_type == OrderType.LIMIT:
            return LIGHTER_LIMIT_ORDER
        if order.order_type == OrderType.MARKET:
            return LIGHTER_MARKET_ORDER
        if order.order_type == OrderType.STOP_MARKET:
            return LIGHTER_STOP_LOSS_ORDER
        if order.order_type == OrderType.STOP_LIMIT:
            return LIGHTER_STOP_LOSS_LIMIT_ORDER
        if order.order_type == OrderType.MARKET_IF_TOUCHED:
            return LIGHTER_TAKE_PROFIT_ORDER
        if order.order_type == OrderType.LIMIT_IF_TOUCHED:
            return LIGHTER_TAKE_PROFIT_LIMIT_ORDER
        raise ValueError(f"Unsupported order type {order.order_type}")

    def _lighter_time_in_force(self, order) -> int:
        if order.is_post_only:
            return LIGHTER_TIF_POST_ONLY
        if order.time_in_force == TimeInForce.IOC:
            return LIGHTER_TIF_IOC
        if order.time_in_force not in (TimeInForce.GTC, TimeInForce.GTD):
            raise ValueError(f"Unsupported time in force {order.time_in_force}")
        return LIGHTER_TIF_GTT

    def _order_expiry_ms(self, order) -> int:
        if order.time_in_force == TimeInForce.IOC:
            return 0
        if order.expire_time is not None:
            expire_time_ns = int(order.expire_time)
            return expire_time_ns // 1_000_000
        return int((time.time() + LIGHTER_DEFAULT_ORDER_EXPIRY_SECS) * 1000)

    async def _submit_order_request(self, order) -> dict[str, Any]:
        instrument = self._cache.instrument(order.instrument_id) or self._instrument_provider.find(
            order.instrument_id,
        )
        if instrument is None:
            raise ValueError(f"Instrument {order.instrument_id} not found")

        metadata = self._metadata(order.instrument_id)
        price_precision = int(metadata["price_decimals"])
        size_precision = int(metadata["size_decimals"])
        market_index = int(metadata["market_id"])
        quantity = Decimal(str(order.quantity))
        base_amount = to_lighter_size(quantity, size_precision)

        if order.has_price:
            price = Decimal(str(order.price))
        elif order.order_type in (
            OrderType.MARKET,
            OrderType.STOP_MARKET,
            OrderType.MARKET_IF_TOUCHED,
        ):
            price = await self._estimate_market_price(order)
        else:
            raise ValueError("Price required for non-market orders")

        client_order_index = self._lighter_client_order_index(order.client_order_id)
        trigger_price = (
            to_lighter_price(Decimal(str(order.trigger_price)), price_precision)
            if order.has_trigger_price
            else 0
        )

        return {
            "market_index": market_index,
            "client_order_index": client_order_index,
            "base_amount": base_amount,
            "price": to_lighter_price(price, price_precision),
            "is_ask": order.side == OrderSide.SELL,
            "order_type": self._lighter_order_type(order),
            "time_in_force": self._lighter_time_in_force(order),
            "reduce_only": order.is_reduce_only,
            "trigger_price": trigger_price,
            "order_expiry": self._order_expiry_ms(order),
            "api_key_index": self._config.api_key_index,
        }

    def _reject_submit_order(self, order, reason: str) -> None:
        self.generate_order_rejected(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            reason=reason,
            ts_event=self._clock.timestamp_ns(),
        )

    def _resolve_cancel_venue_order_id(self, command: CancelOrder) -> VenueOrderId | None:
        venue_order_id = command.venue_order_id or self._venue_order_id_by_client_order_id.get(
            command.client_order_id,
        )
        if venue_order_id is not None:
            return venue_order_id

        order = self._cache.order(command.client_order_id)
        return order.venue_order_id if order is not None else None

    def _cancel_order_request(
        self,
        command: CancelOrder,
        venue_order_id: VenueOrderId,
    ) -> dict[str, Any]:
        metadata = self._metadata(command.instrument_id)
        return {
            "market_index": int(metadata["market_id"]),
            "order_index": int(venue_order_id.value),
            "api_key_index": self._config.api_key_index,
        }

    async def _submit_order_batch(self, orders: list[Any]) -> None:
        valid_orders: list[Any] = []
        requests: list[dict[str, Any]] = []

        for order in orders:
            self.generate_order_submitted(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                ts_event=self._clock.timestamp_ns(),
            )
            try:
                requests.append(await self._submit_order_request(order))
                valid_orders.append(order)
            except Exception as e:
                self._reject_submit_order(order, str(e))

        for start in range(0, len(requests), LIGHTER_MAX_BATCH_TX_COUNT):
            request_chunk = requests[start : start + LIGHTER_MAX_BATCH_TX_COUNT]
            order_chunk = valid_orders[start : start + LIGHTER_MAX_BATCH_TX_COUNT]
            try:
                response = loads(
                    await self._client.submit_order_batch(
                        requests_json=json.dumps(request_chunk),
                    ),
                )
                self._raise_if_tx_error(response)
            except Exception as e:
                for order in order_chunk:
                    self._reject_submit_order(order, str(e))

    async def _cancel_orders_batch(self, cancels: list[CancelOrder]) -> None:
        valid_cancels: list[tuple[CancelOrder, VenueOrderId]] = []
        requests: list[dict[str, Any]] = []

        for cancel in cancels:
            venue_order_id = self._resolve_cancel_venue_order_id(cancel)
            if venue_order_id is None:
                self.generate_order_cancel_rejected(
                    strategy_id=cancel.strategy_id,
                    instrument_id=cancel.instrument_id,
                    client_order_id=cancel.client_order_id,
                    venue_order_id=None,
                    reason="VENUE_ORDER_ID_REQUIRED",
                    ts_event=self._clock.timestamp_ns(),
                )
                continue

            try:
                requests.append(self._cancel_order_request(cancel, venue_order_id))
                valid_cancels.append((cancel, venue_order_id))
            except Exception as e:
                self.generate_order_cancel_rejected(
                    strategy_id=cancel.strategy_id,
                    instrument_id=cancel.instrument_id,
                    client_order_id=cancel.client_order_id,
                    venue_order_id=venue_order_id,
                    reason=str(e),
                    ts_event=self._clock.timestamp_ns(),
                )

        for start in range(0, len(requests), LIGHTER_MAX_BATCH_TX_COUNT):
            request_chunk = requests[start : start + LIGHTER_MAX_BATCH_TX_COUNT]
            cancel_chunk = valid_cancels[start : start + LIGHTER_MAX_BATCH_TX_COUNT]
            try:
                response = loads(
                    await self._client.cancel_order_batch(
                        requests_json=json.dumps(request_chunk),
                    ),
                )
                self._raise_if_tx_error(response)
            except Exception as e:
                for cancel, venue_order_id in cancel_chunk:
                    self.generate_order_cancel_rejected(
                        strategy_id=cancel.strategy_id,
                        instrument_id=cancel.instrument_id,
                        client_order_id=cancel.client_order_id,
                        venue_order_id=venue_order_id,
                        reason=str(e),
                        ts_event=self._clock.timestamp_ns(),
                    )

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        reports = await self.generate_order_status_reports(
            GenerateOrderStatusReports(
                instrument_id=command.instrument_id,
                start=None,
                end=None,
                open_only=False,
                command_id=UUID4(),
                ts_init=self._clock.timestamp_ns(),
                params=command.params,
            ),
        )
        for report in reports:
            if command.client_order_id and report.client_order_id == command.client_order_id:
                return report
            if command.venue_order_id and report.venue_order_id == command.venue_order_id:
                return report
        return None

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        try:
            token = await self._ensure_auth_token()
            reports: list[OrderStatusReport] = []
            for market_id in self._order_status_market_ids(command):
                instrument = self._instrument_provider.instrument_for_market_id(market_id)
                if instrument is None:
                    continue
                orders = await self._load_order_status_payloads(market_id, token, command.open_only)
                for order in orders:
                    report = self._order_status_report_from_payload(order, instrument, command)
                    if report is not None:
                        reports.append(report)

            self._log_report_receipt(
                len(reports),
                "OrderStatusReport",
                self._report_log_level(command),
                "Generated",
            )
            return reports
        except (asyncio.CancelledError, Exception) as e:
            self._log_report_error(e, "OrderStatusReports")
            return []

    def _order_status_market_ids(self, command: GenerateOrderStatusReports) -> list[int]:
        market_ids = (
            [self._instrument_provider.market_id_for_instrument(command.instrument_id)]
            if command.instrument_id is not None
            else self._instrument_provider.market_ids()
        )
        return [market_id for market_id in market_ids if market_id is not None]

    async def _load_order_status_payloads(
        self,
        market_id: int,
        token: str,
        open_only: bool,
    ) -> list[dict[str, Any]]:
        active_payload = loads(
            await self._client.request_account_active_orders(
                self._account_index,
                market_id,
                token,
            ),
        )
        orders = list(active_payload.get("orders") or [])
        if open_only:
            return orders

        cursor = None
        while True:
            inactive_payload = loads(
                await self._client.request_account_inactive_orders(
                    self._account_index,
                    market_id,
                    token,
                    cursor=cursor,
                ),
            )
            orders.extend(inactive_payload.get("orders") or [])
            cursor = inactive_payload.get("cursor")
            if not cursor:
                return orders

    def _order_status_report_from_payload(
        self,
        order: dict[str, Any],
        instrument,
        command: GenerateOrderStatusReports,
    ) -> OrderStatusReport | None:
        report = order_report_from_lighter(
            order,
            self.account_id,
            instrument,
            self._clock.timestamp_ns(),
            self._resolve_client_order_id,
        )
        if command.open_only and report.order_status in {
            OrderStatus.CANCELED,
            OrderStatus.EXPIRED,
            OrderStatus.FILLED,
            OrderStatus.REJECTED,
        }:
            return None
        if not self._in_window(report.ts_last, command.start, command.end):
            return None

        self._track_venue_order_id(report.client_order_id, report.venue_order_id)
        if report.client_order_id is not None:
            cached_order = self._cache.order(report.client_order_id)
            if cached_order is not None:
                self._apply_cached_order_report_metadata(cached_order, report)
        return report

    async def generate_fill_reports(
        self,
        command: GenerateFillReports,
    ) -> list[FillReport]:
        try:
            token = await self._ensure_auth_token()
            reports: list[FillReport] = []
            cursor = None
            while True:
                payload = loads(
                    await self._client.request_account_trades(
                        self._account_index,
                        token,
                        limit=500,
                        cursor=cursor,
                    ),
                )
                trades = payload.get("trades") or []
                if not trades:
                    break

                for trade in trades:
                    report = self._fill_report_from_trade(trade, command)
                    if report is not None:
                        reports.append(report)

                cursor = payload.get("cursor")
                if not cursor:
                    break

            self._log_report_receipt(
                len(reports),
                "FillReport",
                self._report_log_level(command),
                "Generated",
            )
            return reports
        except (asyncio.CancelledError, Exception) as e:
            self._log_report_error(e, "FillReports")
            return []

    def _fill_report_from_trade(
        self,
        trade: dict[str, Any],
        command: GenerateFillReports,
    ) -> FillReport | None:
        market_id = int(trade.get("market_id") or 0)
        instrument = self._instrument_provider.instrument_for_market_id(market_id)
        if instrument is None:
            return None
        if command.instrument_id is not None and instrument.id != command.instrument_id:
            return None

        report = fill_report_from_lighter_trade(
            trade,
            self._account_index,
            self.account_id,
            instrument,
            self._clock.timestamp_ns(),
            self._resolve_client_order_id,
        )
        if report is None:
            return None
        if command.venue_order_id and report.venue_order_id != command.venue_order_id:
            return None
        if not self._in_window(report.ts_event, command.start, command.end):
            return None
        return report

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        try:
            token = await self._ensure_auth_token()
            payload = loads(await self._client.request_account(self._account_index, token))
            accounts = payload.get("accounts") or []
            if not accounts:
                return []
            reports: list[PositionStatusReport] = []
            ts_init = self._clock.timestamp_ns()
            for position in accounts[0].get("positions") or []:
                instrument = self._instrument_provider.instrument_for_market_id(
                    int(position["market_id"])
                )
                if instrument is None:
                    continue
                if command.instrument_id is not None and instrument.id != command.instrument_id:
                    continue
                reports.append(
                    position_report_from_lighter(position, self.account_id, instrument, ts_init)
                )
            if command.instrument_id is not None and not reports:
                instrument = self._cache.instrument(
                    command.instrument_id
                ) or self._instrument_provider.find(
                    command.instrument_id,
                )
                if instrument is not None:
                    reports.append(
                        PositionStatusReport.create_flat(
                            account_id=self.account_id,
                            instrument_id=instrument.id,
                            size_precision=instrument.size_precision,
                            ts_init=ts_init,
                        ),
                    )

            self._log_report_receipt(
                len(reports),
                "PositionStatusReport",
                self._report_log_level(command),
                "Generated",
            )
            return reports
        except (asyncio.CancelledError, Exception) as e:
            self._log_report_error(e, "PositionStatusReports")
            return []

    def _report_log_level(self, command: Any) -> LogLevel:
        return getattr(command, "log_receipt_level", LogLevel.INFO)

    def _is_contingent_order(self, order: Any) -> bool:
        contingency_type = getattr(order, "contingency_type", ContingencyType.NO_CONTINGENCY)
        linked_order_ids = getattr(order, "linked_order_ids", None)
        parent_order_id = getattr(order, "parent_order_id", None)
        return (
            contingency_type not in (None, ContingencyType.NO_CONTINGENCY)
            or bool(linked_order_ids)
            or parent_order_id is not None
        )

    def _deny_order_pre_submit(self, order: Any, reason: str) -> None:
        self.generate_order_denied(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            reason=reason,
            ts_event=self._clock.timestamp_ns(),
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

    def _open_orders_for_cancel_all(
        self,
        instrument_id,
        order_side: OrderSide,
    ) -> list[Any]:
        return list(
            self._cache.orders_open(
                venue=self.venue,
                instrument_id=instrument_id,
                side=order_side,
            ),
        )

    async def generate_mass_status(
        self, lookback_mins: int | None = None
    ) -> ExecutionMassStatus | None:
        try:
            self.reconciliation_active = True
            since = None
            if lookback_mins is not None:
                since = self._clock.utc_now() - timedelta(minutes=lookback_mins)

            order_reports, fill_reports, position_reports = await asyncio.gather(
                self.generate_order_status_reports(
                    GenerateOrderStatusReports(
                        instrument_id=None,
                        start=since,
                        end=None,
                        open_only=False,
                        command_id=UUID4(),
                        ts_init=self._clock.timestamp_ns(),
                    ),
                ),
                self.generate_fill_reports(
                    GenerateFillReports(
                        instrument_id=None,
                        venue_order_id=None,
                        start=since,
                        end=None,
                        command_id=UUID4(),
                        ts_init=self._clock.timestamp_ns(),
                    ),
                ),
                self.generate_position_status_reports(
                    GeneratePositionStatusReports(
                        instrument_id=None,
                        start=since,
                        end=None,
                        command_id=UUID4(),
                        ts_init=self._clock.timestamp_ns(),
                    ),
                ),
            )
            mass_status = ExecutionMassStatus(
                client_id=self.id,
                account_id=self.account_id,
                venue=LIGHTER_VENUE,
                report_id=UUID4(),
                ts_init=self._clock.timestamp_ns(),
            )
            mass_status.add_order_reports(order_reports)
            mass_status.add_fill_reports(fill_reports)
            mass_status.add_position_reports(position_reports)
            return mass_status
        except Exception as e:
            self._log.exception("Cannot reconcile Lighter execution state", e)
            return None
        finally:
            self.reconciliation_active = False

    async def _query_account(self, command: QueryAccount) -> None:
        await self._update_account_state()

    async def _submit_order(self, command: SubmitOrder) -> None:
        order = command.order
        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

        try:
            request = await self._submit_order_request(order)
            response = loads(
                await self._client.submit_order(
                    **request,
                ),
            )
            self._raise_if_tx_error(response)
            self._log.info(f"Submitted Lighter order {order.client_order_id}: {response}")
        except Exception as e:
            self._reject_submit_order(order, str(e))

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        if any(self._is_contingent_order(order) for order in command.order_list.orders):
            for order in command.order_list.orders:
                self._deny_order_pre_submit(order, "UNSUPPORTED_CONTINGENT_ORDER_LIST")
            return
        await self._submit_order_batch(list(command.order_list.orders))

    async def _modify_order(self, command: ModifyOrder) -> None:
        order = self._cache.order(command.client_order_id)
        if order is None:
            self.generate_order_modify_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=command.venue_order_id,
                reason="ORDER_NOT_FOUND_IN_CACHE",
                ts_event=self._clock.timestamp_ns(),
            )
            return

        instrument = self._cache.instrument(
            command.instrument_id
        ) or self._instrument_provider.find(
            command.instrument_id,
        )
        if instrument is None:
            self.generate_order_modify_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=command.venue_order_id,
                reason="INSTRUMENT_NOT_FOUND",
                ts_event=self._clock.timestamp_ns(),
            )
            return

        venue_order_id = (
            order.venue_order_id
            or command.venue_order_id
            or self._venue_order_id_by_client_order_id.get(command.client_order_id)
        )
        if venue_order_id is None:
            self.generate_order_modify_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=venue_order_id,
                reason="VENUE_ORDER_ID_REQUIRED",
                ts_event=self._clock.timestamp_ns(),
            )
            return

        try:
            metadata = self._metadata(command.instrument_id)
            price_precision = int(metadata["price_decimals"])
            size_precision = int(metadata["size_decimals"])
            market_index = int(metadata["market_id"])
            order_price = order.price if order.has_price else None
            order_trigger_price = order.trigger_price if order.has_trigger_price else None
            price_value = command.price if command.price is not None else order_price
            quantity_value = command.quantity if command.quantity is not None else order.leaves_qty
            trigger_value = (
                command.trigger_price
                if command.trigger_price is not None
                else order_trigger_price
            )

            if price_value is None:
                raise ValueError("PRICE_REQUIRED")

            price = Decimal(str(price_value))
            quantity = Decimal(str(quantity_value))
            trigger = Decimal(str(trigger_value or 0))
            response = loads(
                await self._client.modify_order(
                    market_index=market_index,
                    order_index=int(venue_order_id.value),
                    base_amount=to_lighter_size(quantity, size_precision),
                    price=to_lighter_price(price, price_precision),
                    trigger_price=to_lighter_price(trigger, price_precision) if trigger else 0,
                    api_key_index=self._config.api_key_index,
                ),
            )
            self._raise_if_tx_error(response)
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
        venue_order_id = self._resolve_cancel_venue_order_id(command)
        if venue_order_id is None:
            self.generate_order_cancel_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=None,
                reason="VENUE_ORDER_ID_REQUIRED",
                ts_event=self._clock.timestamp_ns(),
            )
            return

        try:
            response = loads(
                await self._client.cancel_order(
                    **self._cancel_order_request(command, venue_order_id),
                ),
            )
            self._raise_if_tx_error(response)
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
        if command.instrument_id is None and command.order_side in {None, OrderSide.NO_ORDER_SIDE}:
            try:
                response = loads(
                    await self._client.cancel_all_orders(
                        time_in_force=LIGHTER_TIF_GTT,
                        timestamp_ms=int((time.time() + self._config.cancel_all_gtt_secs) * 1000),
                        api_key_index=self._config.api_key_index,
                    ),
                )
                self._raise_if_tx_error(response)
                return
            except Exception as e:
                self._log.warning(
                    f"Venue-level cancel_all failed, falling back to per-order cancellation: {e}",
                )

        order_side = command.order_side
        if order_side is None:
            order_side = OrderSide.NO_ORDER_SIDE

        open_orders = self._open_orders_for_cancel_all(command.instrument_id, order_side)
        await self._cancel_orders_batch(
            [
                CancelOrder(
                    trader_id=command.trader_id,
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=order.venue_order_id,
                    command_id=UUID4(),
                    ts_init=command.ts_init,
                    client_id=command.client_id,
                )
                for order in open_orders
            ],
        )

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        await self._cancel_orders_batch(list(command.cancels))

    async def update_leverage(
        self,
        instrument_id,
        *,
        initial_margin_fraction: int,
        margin_mode: int = LIGHTER_MARGIN_MODE_CROSS,
    ) -> dict[str, Any]:
        metadata = self._metadata(instrument_id)
        response = await self._client.update_leverage(
            market_index=int(metadata["market_id"]),
            initial_margin_fraction=initial_margin_fraction,
            margin_mode=margin_mode,
            api_key_index=self._config.api_key_index,
        )
        return loads(response)

    async def update_margin(
        self,
        instrument_id,
        *,
        usdc_amount: int,
        direction: int = LIGHTER_UPDATE_MARGIN_ADD,
    ) -> dict[str, Any]:
        metadata = self._metadata(instrument_id)
        response = await self._client.update_margin(
            market_index=int(metadata["market_id"]),
            usdc_amount=usdc_amount,
            direction=direction,
            api_key_index=self._config.api_key_index,
        )
        return loads(response)

    async def request_account_api_keys(self) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(await self._client.request_account_api_keys(self._account_index, token))

    async def request_announcements(self) -> dict[str, Any]:
        return loads(await self._client.request_announcements())

    async def request_status(self) -> dict[str, Any]:
        return loads(await self._client.request_status())

    async def request_system_config(self) -> dict[str, Any]:
        return loads(await self._client.request_system_config())

    async def request_exchange_metrics(
        self,
        *,
        period: str,
        kind: str,
        filter: str | None = None,
        value: str | None = None,
    ) -> dict[str, Any]:
        return loads(await self._client.request_exchange_metrics(period, kind, filter, value))

    async def request_execute_stats(self, period: str) -> dict[str, Any]:
        return loads(await self._client.request_execute_stats(period))

    async def request_layer1_basic_info(self) -> dict[str, Any]:
        return loads(await self._client.request_layer1_basic_info())

    async def request_zk_lighter_info(self) -> dict[str, Any]:
        return loads(await self._client.request_zk_lighter_info())

    async def request_account_limits(self) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(await self._client.request_account_limits(self._account_index, token))

    async def request_account_metadata(self) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(await self._client.request_account_metadata(self._account_index, token))

    async def request_l1_metadata(self, l1_address: str) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(await self._client.request_l1_metadata(l1_address, token))

    async def request_sub_accounts(self, l1_address: str) -> dict[str, Any]:
        return loads(await self._client.request_sub_accounts(l1_address))

    async def request_public_pools_metadata(
        self,
        *,
        filter: str = "all",
        index: int = 0,
        limit: int = 100,
        account_index: int | None = None,
    ) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(
            await self._client.request_public_pools_metadata(
                filter,
                index,
                limit,
                account_index,
                token,
            ),
        )

    async def request_account_pnl(self) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(await self._client.request_account_pnl(self._account_index, token))

    async def request_liquidations(
        self,
        *,
        limit: int = 100,
        market_id: int | None = None,
        cursor: str | None = None,
        account_index: int | None = None,
    ) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(
            await self._client.request_liquidations(
                int(account_index if account_index is not None else self._account_index),
                limit,
                market_id,
                cursor,
                token,
            ),
        )

    async def request_position_fundings(self) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(await self._client.request_position_fundings(self._account_index, token))

    async def request_deposit_history(self, cursor: str | None = None) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(await self._client.request_deposit_history(self._account_index, token, cursor))

    async def request_withdraw_history(self, cursor: str | None = None) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(
            await self._client.request_withdraw_history(self._account_index, token, cursor)
        )

    async def request_transfer_history(self, cursor: str | None = None) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(
            await self._client.request_transfer_history(self._account_index, token, cursor)
        )

    async def request_next_nonce(self, api_key_index: int | None = None) -> dict[str, Any]:
        return loads(
            await self._client.request_next_nonce(
                self._account_index,
                int(
                    api_key_index if api_key_index is not None else self._config.api_key_index or 0
                ),
            ),
        )

    async def request_enriched_tx(self, tx_hash: str) -> dict[str, Any]:
        return loads(await self._client.request_enriched_tx(tx_hash))

    async def request_tx_from_l1_tx_hash(self, l1_tx_hash: str) -> dict[str, Any]:
        return loads(await self._client.request_tx_from_l1_tx_hash(l1_tx_hash))

    async def request_txs(self, *, limit: int, index: int | None = None) -> dict[str, Any]:
        return loads(await self._client.request_txs(limit, index))

    async def request_export(
        self,
        *,
        export_type: str,
        account_index: int | None = None,
        market_id: int | None = None,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        side: str | None = None,
        role: str | None = None,
        trade_type: str | None = None,
    ) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(
            await self._client.request_export(
                export_type,
                token,
                account_index,
                market_id,
                start_timestamp,
                end_timestamp,
                side,
                role,
                trade_type,
            ),
        )

    async def request_transfer_fee_info(
        self,
        *,
        to_account_index: int | None = None,
        account_index: int | None = None,
    ) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(
            await self._client.request_transfer_fee_info(
                int(account_index if account_index is not None else self._account_index),
                to_account_index,
                token,
            ),
        )

    async def request_withdrawal_delay(self) -> dict[str, Any]:
        return loads(await self._client.request_withdrawal_delay())

    async def create_intent_address(
        self,
        *,
        chain_id: str,
        from_addr: str,
        amount: str,
        is_external_deposit: bool = False,
    ) -> dict[str, Any]:
        response = loads(
            await self._client.create_intent_address(
                chain_id,
                from_addr,
                amount,
                is_external_deposit,
            ),
        )
        self._raise_if_response_error(
            response,
            action="create intent address",
            default_message="Unknown Lighter intent-address error",
        )
        return response

    async def request_fast_bridge_info(self) -> dict[str, Any]:
        return loads(await self._client.request_fast_bridge_info())

    async def request_deposit_latest(self, l1_address: str) -> dict[str, Any]:
        return loads(await self._client.request_deposit_latest(l1_address))

    async def request_deposit_networks(self) -> dict[str, Any]:
        return loads(await self._client.request_deposit_networks())

    async def request_fast_withdraw_info(
        self,
        *,
        account_index: int | None = None,
    ) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(
            await self._client.request_fast_withdraw_info(
                int(account_index if account_index is not None else self._account_index),
                token,
            ),
        )

    async def request_lease_options(self) -> dict[str, Any]:
        return loads(await self._client.request_lease_options())

    async def request_leases(
        self,
        *,
        cursor: str | None = None,
        limit: int | None = None,
        account_index: int | None = None,
    ) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(
            await self._client.request_leases(
                int(account_index if account_index is not None else self._account_index),
                token,
                cursor,
                limit,
            ),
        )

    async def request_api_tokens(self) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(await self._client.request_api_tokens(self._account_index, token))

    async def request_user_referrals(
        self,
        l1_address: str,
        *,
        cursor: int | None = None,
    ) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(await self._client.request_user_referrals(l1_address, cursor, token))

    async def request_referral_code(self, *, account_index: int | None = None) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        return loads(
            await self._client.request_referral_code(
                int(account_index if account_index is not None else self._account_index),
                token,
            ),
        )

    async def create_api_token(
        self,
        *,
        name: str,
        expiry: int,
        sub_account_access: bool,
        scopes: str = "read.*",
        account_index: int | None = None,
    ) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        response = loads(
            await self._client.create_api_token(
                name,
                int(account_index if account_index is not None else self._account_index),
                expiry,
                sub_account_access,
                token,
                scopes,
            ),
        )
        self._raise_if_response_error(
            response,
            action="create api token",
            default_message="Unknown Lighter API token error",
        )
        return response

    async def revoke_api_token(
        self,
        token_id: int,
        *,
        account_index: int | None = None,
    ) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        response = loads(
            await self._client.revoke_api_token(
                token_id,
                int(account_index if account_index is not None else self._account_index),
                token,
            ),
        )
        self._raise_if_response_error(
            response,
            action="revoke api token",
            default_message="Unknown Lighter API token revoke error",
        )
        return response

    async def change_account_tier(self, new_tier: str) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        response = loads(await self._client.change_account_tier(self._account_index, new_tier, token))
        self._raise_if_response_error(
            response,
            action="change account tier",
            default_message="Unknown Lighter account tier error",
        )
        return response

    async def create_referral_code(
        self,
        *,
        account_index: int | None = None,
    ) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        response = loads(
            await self._client.create_referral_code(
                int(account_index if account_index is not None else self._account_index),
                token,
            ),
        )
        self._raise_if_response_error(
            response,
            action="create referral code",
            default_message="Unknown Lighter referral create error",
        )
        return response

    async def update_referral_code(
        self,
        new_referral_code: str,
        *,
        account_index: int | None = None,
    ) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        response = loads(
            await self._client.update_referral_code(
                int(account_index if account_index is not None else self._account_index),
                new_referral_code,
                token,
            ),
        )
        self._raise_if_response_error(
            response,
            action="update referral code",
            default_message="Unknown Lighter referral update error",
        )
        return response

    async def update_referral_kickback(
        self,
        kickback_percentage: float,
        *,
        account_index: int | None = None,
    ) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        response = loads(
            await self._client.update_referral_kickback(
                int(account_index if account_index is not None else self._account_index),
                kickback_percentage,
                token,
            ),
        )
        self._raise_if_response_error(
            response,
            action="update referral kickback",
            default_message="Unknown Lighter referral kickback update error",
        )
        return response

    async def use_referral_code(
        self,
        *,
        l1_address: str,
        referral_code: str,
        x: str,
        discord: str | None = None,
        telegram: str | None = None,
        signature: str | None = None,
    ) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        response = loads(
            await self._client.use_referral_code(
                l1_address,
                referral_code,
                x,
                discord,
                telegram,
                signature,
                token,
            ),
        )
        self._raise_if_response_error(
            response,
            action="use referral code",
            default_message="Unknown Lighter referral use error",
        )
        return response

    async def withdraw(
        self,
        *,
        asset_index: int,
        route_type: int,
        amount: int,
        api_key_index: int | None = None,
        nonce: int | None = None,
    ) -> dict[str, Any]:
        response = loads(
            await self._client.withdraw(
                asset_index=asset_index,
                route_type=route_type,
                amount=amount,
                api_key_index=api_key_index,
                nonce=nonce,
            ),
        )
        self._raise_if_tx_error(response)
        return response

    async def fast_withdraw(self, *, tx_info: str, to_address: str) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        response = loads(
            await self._client.fast_withdraw(
                tx_info=tx_info,
                to_address=to_address,
                auth_token=token,
            ),
        )
        self._raise_if_response_error(
            response,
            action="fast withdraw",
            default_message="Unknown Lighter fast withdraw error",
        )
        return response

    async def acknowledge_notification(
        self,
        notif_id: str,
        *,
        account_index: int | None = None,
    ) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        response = loads(
            await self._client.acknowledge_notification(
                notif_id,
                int(account_index if account_index is not None else self._account_index),
                token,
            ),
        )
        self._raise_if_response_error(
            response,
            action="acknowledge notification",
            default_message="Unknown Lighter notification acknowledgement error",
        )
        return response

    async def transfer(
        self,
        *,
        to_account_index: int,
        asset_index: int,
        from_route_type: int,
        to_route_type: int,
        amount: int,
        usdc_fee: int = 0,
        memo: str = "",
        api_key_index: int | None = None,
        nonce: int | None = None,
    ) -> dict[str, Any]:
        response = loads(
            await self._client.transfer(
                to_account_index=to_account_index,
                asset_index=asset_index,
                from_route_type=from_route_type,
                to_route_type=to_route_type,
                amount=amount,
                usdc_fee=usdc_fee,
                memo=memo,
                api_key_index=api_key_index,
                nonce=nonce,
            ),
        )
        self._raise_if_tx_error(response)
        return response

    async def lit_lease(
        self,
        *,
        tx_info: str,
        lease_amount: str | None = None,
        duration_days: int | None = None,
    ) -> dict[str, Any]:
        token = await self._ensure_auth_token()
        response = loads(
            await self._client.lit_lease(
                tx_info=tx_info,
                auth_token=token,
                lease_amount=lease_amount,
                duration_days=duration_days,
            ),
        )
        self._raise_if_response_error(
            response,
            action="lit lease",
            default_message="Unknown Lighter lit lease error",
        )
        return response

    async def create_public_pool(
        self,
        *,
        operator_fee: int,
        initial_total_shares: int,
        min_operator_share_rate: int,
        api_key_index: int | None = None,
        nonce: int | None = None,
    ) -> dict[str, Any]:
        response = loads(
            await self._client.create_public_pool(
                operator_fee=operator_fee,
                initial_total_shares=initial_total_shares,
                min_operator_share_rate=min_operator_share_rate,
                api_key_index=api_key_index,
                nonce=nonce,
            ),
        )
        self._raise_if_tx_error(response)
        return response

    async def update_public_pool(
        self,
        *,
        public_pool_index: int,
        status: int,
        operator_fee: int,
        min_operator_share_rate: int,
        api_key_index: int | None = None,
        nonce: int | None = None,
    ) -> dict[str, Any]:
        response = loads(
            await self._client.update_public_pool(
                public_pool_index=public_pool_index,
                status=status,
                operator_fee=operator_fee,
                min_operator_share_rate=min_operator_share_rate,
                api_key_index=api_key_index,
                nonce=nonce,
            ),
        )
        self._raise_if_tx_error(response)
        return response

    async def mint_pool_shares(
        self,
        *,
        public_pool_index: int,
        share_amount: int,
        api_key_index: int | None = None,
        nonce: int | None = None,
    ) -> dict[str, Any]:
        response = loads(
            await self._client.mint_pool_shares(
                public_pool_index=public_pool_index,
                share_amount=share_amount,
                api_key_index=api_key_index,
                nonce=nonce,
            ),
        )
        self._raise_if_tx_error(response)
        return response

    async def burn_pool_shares(
        self,
        *,
        public_pool_index: int,
        share_amount: int,
        api_key_index: int | None = None,
        nonce: int | None = None,
    ) -> dict[str, Any]:
        response = loads(
            await self._client.burn_pool_shares(
                public_pool_index=public_pool_index,
                share_amount=share_amount,
                api_key_index=api_key_index,
                nonce=nonce,
            ),
        )
        self._raise_if_tx_error(response)
        return response

    async def change_pub_key(
        self,
        *,
        new_pub_key: str,
        api_key_index: int | None = None,
        nonce: int | None = None,
    ) -> dict[str, Any]:
        response = loads(
            await self._client.change_pub_key(
                new_pub_key=new_pub_key,
                api_key_index=api_key_index,
                nonce=nonce,
            ),
        )
        self._raise_if_tx_error(response)
        return response

    async def create_sub_account(
        self,
        *,
        api_key_index: int | None = None,
        nonce: int | None = None,
    ) -> dict[str, Any]:
        response = loads(
            await self._client.create_sub_account(
                api_key_index=api_key_index,
                nonce=nonce,
            ),
        )
        self._raise_if_tx_error(response)
        return response

    def _handle_msg(self, msg: Any) -> None:
        try:
            payload = loads(msg)
            msg_type = str(payload.get("type") or "")
            if "account_all_orders" in msg_type:
                self._handle_orders_update(payload)
            elif "account_all_trades" in msg_type:
                self._handle_trades_update(payload)
            elif "account_all_positions" in msg_type:
                self._handle_positions_update(payload)
            elif "account_all_assets" in msg_type:
                self._handle_assets_update(payload)
            elif "account_all" in msg_type:
                self._handle_account_all_update(payload)
            elif msg_type not in {"connected", "ping", "pong"}:
                self._log.debug(f"Unhandled Lighter execution message: {payload}", LogColor.MAGENTA)
        except Exception as e:
            self._log.exception("Error handling Lighter execution message", e)

    def _handle_assets_update(self, payload: dict[str, Any]) -> None:
        self.create_task(self._refresh_account_state())

    def _handle_account_all_update(self, payload: dict[str, Any]) -> None:
        assets = self._flatten_ws_values(payload.get("assets"))
        positions = self._flatten_ws_values(payload.get("positions"))
        balances = account_balances_from_assets(assets)
        margins = margin_balances_from_positions(
            positions,
            self._instrument_provider.instrument_for_market_id,
        )
        self.generate_account_state(
            balances=balances,
            margins=margins,
            reported=True,
            ts_event=self._clock.timestamp_ns(),
        )

        self._handle_orders_map(payload.get("orders"))
        self._handle_trades_map(payload.get("trades"))

    def _handle_positions_update(self, payload: dict[str, Any]) -> None:
        ts_init = self._clock.timestamp_ns()
        for market_id, position in self._iter_grouped_values(payload.get("positions")):
            instrument = self._instrument_provider.instrument_for_market_id(market_id)
            if instrument is None:
                continue
            report = position_report_from_lighter(position, self.account_id, instrument, ts_init)
            self._send_position_status_report(report)

    def _handle_orders_update(self, payload: dict[str, Any]) -> None:
        self._handle_orders_map(payload.get("orders"))

    def _handle_orders_map(self, orders_map: Any) -> None:
        for market_id, order in self._iter_grouped_values(orders_map):
            instrument = self._instrument_provider.instrument_for_market_id(market_id)
            if instrument is None:
                continue
            report = order_report_from_lighter(
                order,
                self.account_id,
                instrument,
                self._clock.timestamp_ns(),
                self._resolve_client_order_id,
            )
            self._track_venue_order_id(report.client_order_id, report.venue_order_id)
            self._handle_order_report(report)

    def _handle_order_report(self, report: OrderStatusReport) -> None:
        if self._is_duplicate_order_report(report):
            return

        if report.client_order_id is None and report.venue_order_id is not None:
            report.client_order_id = self._cache.client_order_id(report.venue_order_id)

        if report.client_order_id is None:
            self._send_order_status_report(report)
            return

        order = self._cache.order(report.client_order_id)
        if order is None:
            self._send_order_status_report(report)
            return

        self._apply_cached_order_report_metadata(order, report)
        self._handle_cached_order_report(order, report)

    def _is_duplicate_order_report(self, report: OrderStatusReport) -> bool:
        state_key = report.venue_order_id.value
        state_value = (
            report.order_status.value,
            report.ts_last,
            str(report.filled_qty),
        )
        if self._recent_order_states.get(state_key) == state_value:
            return True
        self._recent_order_states[state_key] = state_value
        self._recent_order_state_queue.append(state_key)
        if len(self._recent_order_states) > self._recent_order_state_queue.maxlen:
            while len(self._recent_order_states) > self._recent_order_state_queue.maxlen:
                expired_key = self._recent_order_state_queue.popleft()
                self._recent_order_states.pop(expired_key, None)
        return False

    def _handle_cached_order_report(self, order, report: OrderStatusReport) -> None:
        if report.order_status == OrderStatus.REJECTED:
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=report.cancel_reason or "Order rejected by exchange",
                ts_event=report.ts_last,
            )
            return

        if report.order_status in (
            OrderStatus.SUBMITTED,
            OrderStatus.ACCEPTED,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
        ):
            if order.venue_order_id is None:
                self.generate_order_accepted(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=report.venue_order_id,
                    ts_event=report.ts_last,
                )
            elif (
                report.quantity != order.quantity
                or report.price != order.price
                or report.trigger_price != order.trigger_price
            ):
                self.generate_order_updated(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=report.venue_order_id,
                    quantity=report.quantity,
                    price=report.price,
                    trigger_price=report.trigger_price,
                    ts_event=report.ts_last,
                )
            return

        if report.order_status == OrderStatus.CANCELED:
            self.generate_order_canceled(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=report.venue_order_id,
                ts_event=report.ts_last,
            )
            return

        if report.order_status == OrderStatus.EXPIRED:
            self.generate_order_expired(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=report.venue_order_id,
                ts_event=report.ts_last,
            )

    def _handle_trades_update(self, payload: dict[str, Any]) -> None:
        self._handle_trades_map(payload.get("trades"))

    def _handle_trades_map(self, trades_map: Any) -> None:
        for market_id, trade in self._iter_grouped_values(trades_map):
            instrument = self._instrument_provider.instrument_for_market_id(market_id)
            if instrument is None:
                continue
            trade_id = str(trade.get("trade_id"))
            if trade_id in self._processed_trade_ids:
                continue
            self._processed_trade_ids.add(trade_id)
            self._processed_trade_queue.append(trade_id)
            if len(self._processed_trade_ids) > self._processed_trade_queue.maxlen:
                while len(self._processed_trade_ids) > self._processed_trade_queue.maxlen:
                    self._processed_trade_ids.discard(self._processed_trade_queue.popleft())

            report = fill_report_from_lighter_trade(
                trade,
                self._account_index,
                self.account_id,
                instrument,
                self._clock.timestamp_ns(),
                self._resolve_client_order_id,
            )
            if report is None:
                continue
            if report.client_order_id is not None:
                self._track_venue_order_id(report.client_order_id, report.venue_order_id)
            self._handle_fill_report(report)

    def _handle_fill_report(self, report: FillReport) -> None:
        if report.client_order_id is None and report.venue_order_id is not None:
            report.client_order_id = self._cache.client_order_id(report.venue_order_id)

        if report.client_order_id is None:
            self._send_fill_report(report)
            return

        order = self._cache.order(report.client_order_id)
        if order is None:
            self._send_fill_report(report)
            return

        instrument = self._cache.instrument(order.instrument_id) or self._instrument_provider.find(
            order.instrument_id,
        )
        if instrument is None:
            return

        if (
            order.venue_order_id is None
            and report.client_order_id not in self._venue_order_id_by_client_order_id
        ):
            self.generate_order_accepted(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=report.venue_order_id,
                ts_event=report.ts_event,
            )
            self._track_venue_order_id(order.client_order_id, report.venue_order_id)

        self.generate_order_filled(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=report.venue_order_id,
            venue_position_id=report.venue_position_id,
            trade_id=report.trade_id,
            order_side=order.side,
            order_type=order.order_type,
            last_qty=report.last_qty,
            last_px=report.last_px,
            quote_currency=instrument.quote_currency,
            commission=report.commission,
            liquidity_side=report.liquidity_side,
            ts_event=report.ts_event,
        )

    def _raise_if_response_error(
        self,
        response: dict[str, Any],
        *,
        action: str,
        default_message: str,
    ) -> None:
        code = int(response.get("code") or 0)
        if code == 200:
            return
        message = response.get("message") or default_message
        raise RuntimeError(f"Lighter {action} failed ({code}): {message}")

    def _raise_if_tx_error(self, response: dict[str, Any]) -> None:
        self._raise_if_response_error(
            response,
            action="tx",
            default_message="Unknown Lighter transaction error",
        )
