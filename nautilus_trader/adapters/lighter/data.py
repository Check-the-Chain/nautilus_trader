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
from decimal import Decimal
from typing import Any

from nautilus_trader.adapters.lighter.config import LighterDataClientConfig
from nautilus_trader.adapters.lighter.constants import LIGHTER_MARKET_TYPE_PERP
from nautilus_trader.adapters.lighter.constants import LIGHTER_VENUE
from nautilus_trader.adapters.lighter.parsing import candles_to_bars
from nautilus_trader.adapters.lighter.parsing import datetime_to_nanos
from nautilus_trader.adapters.lighter.parsing import epoch_to_nanos
from nautilus_trader.adapters.lighter.parsing import loads
from nautilus_trader.adapters.lighter.parsing import market_id_from_channel
from nautilus_trader.adapters.lighter.parsing import market_stats_to_updates
from nautilus_trader.adapters.lighter.parsing import order_book_deltas
from nautilus_trader.adapters.lighter.parsing import order_book_snapshot
from nautilus_trader.adapters.lighter.parsing import quote_tick_from_ticker
from nautilus_trader.adapters.lighter.parsing import trade_tick_from_trade
from nautilus_trader.adapters.lighter.providers import LighterInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import LighterEnvironment
from nautilus_trader.data.messages import RequestBars
from nautilus_trader.data.messages import RequestFundingRates
from nautilus_trader.data.messages import RequestInstrument
from nautilus_trader.data.messages import RequestInstruments
from nautilus_trader.data.messages import RequestOrderBookSnapshot
from nautilus_trader.data.messages import RequestQuoteTicks
from nautilus_trader.data.messages import RequestTradeTicks
from nautilus_trader.data.messages import SubscribeBars
from nautilus_trader.data.messages import SubscribeFundingRates
from nautilus_trader.data.messages import SubscribeIndexPrices
from nautilus_trader.data.messages import SubscribeInstrument
from nautilus_trader.data.messages import SubscribeInstruments
from nautilus_trader.data.messages import SubscribeMarkPrices
from nautilus_trader.data.messages import SubscribeOrderBook
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import SubscribeTradeTicks
from nautilus_trader.data.messages import UnsubscribeBars
from nautilus_trader.data.messages import UnsubscribeFundingRates
from nautilus_trader.data.messages import UnsubscribeIndexPrices
from nautilus_trader.data.messages import UnsubscribeInstrument
from nautilus_trader.data.messages import UnsubscribeInstruments
from nautilus_trader.data.messages import UnsubscribeMarkPrices
from nautilus_trader.data.messages import UnsubscribeOrderBook
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeTradeTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import BarAggregation
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.enums import book_type_to_str
from nautilus_trader.model.identifiers import ClientId


def _bar_granularity(bar_type) -> str:
    spec = bar_type.spec
    if spec.price_type != PriceType.LAST:
        raise ValueError("Lighter only exposes external LAST bars")
    if not spec.is_time_aggregated():
        raise ValueError("Lighter only exposes time bars")

    if spec.aggregation == BarAggregation.MINUTE:
        return f"{spec.step}m"
    if spec.aggregation == BarAggregation.HOUR:
        return f"{spec.step}h"
    if spec.aggregation == BarAggregation.DAY:
        return f"{spec.step}d"
    if spec.aggregation == BarAggregation.WEEK:
        return f"{spec.step}w"
    raise ValueError(f"Unsupported bar aggregation {spec.aggregation}")


def _in_window(ts_event: int, start, end) -> bool:
    start_ns = datetime_to_nanos(start)
    end_ns = datetime_to_nanos(end)
    if start_ns is not None and ts_event < start_ns:
        return False
    return not (end_ns is not None and ts_event > end_ns)


class LighterDataClient(LiveMarketDataClient):
    """
    Provides a data client for the Lighter exchange.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: LighterInstrumentProvider,
        config: LighterDataClientConfig,
        name: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or LIGHTER_VENUE.value),
            venue=LIGHTER_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )

        self._client = client
        self._instrument_provider = instrument_provider
        self._config = config
        self._ws_client = None
        self._book_offsets: dict[int, int] = {}
        self._book_states: dict[int, dict[str, dict[str, str]]] = {}
        self._last_quotes: dict[str, Any] = {}
        self._market_stats_refcount = 0

    @property
    def instrument_provider(self) -> LighterInstrumentProvider:
        return self._instrument_provider

    async def _connect(self) -> None:
        await self._instrument_provider.initialize()
        self._send_all_instruments_to_data_engine()
        environment = (
            self._config.environment
            if self._config.environment is not None
            else (LighterEnvironment.TESTNET if self._config.testnet else LighterEnvironment.MAINNET)
        )

        self._ws_client = nautilus_pyo3.LighterWebSocketClient(  # type: ignore[attr-defined]
            url=self._config.base_url_ws,
            testnet=environment == LighterEnvironment.TESTNET,
        )
        await self._ws_client.connect(self._loop, self._handle_msg)
        self._log.info(f"Connected to WebSocket {self._ws_client.url}", LogColor.BLUE)

    async def _disconnect(self) -> None:
        await asyncio.sleep(0.25)
        if self._ws_client is not None and not self._ws_client.is_closed():
            await self._ws_client.close()

    def _send_all_instruments_to_data_engine(self) -> None:
        for instrument in self._instrument_provider.get_all().values():
            self._handle_data(instrument)
        for currency in self._instrument_provider.currencies().values():
            self._cache.add_currency(currency)

    def _handle_msg(self, msg: Any) -> None:
        try:
            payload = loads(msg)
            msg_type = str(payload.get("type") or "")
            channel = str(payload.get("channel") or "")

            if "order_book" in msg_type:
                self._handle_order_book(payload)
            elif "ticker" in msg_type:
                self._handle_ticker(payload)
            elif "trade" in msg_type:
                self._handle_trade(payload)
            elif "market_stats" in msg_type:
                self._handle_market_stats(payload, channel)
            elif msg_type not in {"connected", "ping", "pong"}:
                self._log.debug(f"Unhandled Lighter data message: {payload}")
        except Exception as e:
            self._log.exception("Error handling Lighter websocket message", e)

    def _is_perp_instrument(self, instrument_id) -> bool:
        metadata = self._instrument_provider.metadata_for_instrument(instrument_id)
        return bool(metadata and metadata.get("market_type") == LIGHTER_MARKET_TYPE_PERP)

    def _iter_market_stats(
        self,
        payload: dict[str, Any],
        channel: str,
    ) -> list[tuple[int, dict[str, Any]]]:
        stats_payload = (
            payload.get("market_stats") or payload.get("market") or payload.get("spot_market_stats")
        )
        if stats_payload is None:
            return []

        stats_map: list[tuple[int, dict[str, Any]]] = []
        if isinstance(stats_payload, dict) and stats_payload.get("market_id") is not None:
            stats_map.append((int(stats_payload["market_id"]), stats_payload))
            return stats_map

        if isinstance(stats_payload, dict):
            for market_key, value in stats_payload.items():
                if isinstance(value, dict):
                    stats_map.append((int(value.get("market_id") or market_key), value))
            return stats_map

        market_id = market_id_from_channel(channel)
        if market_id is None or not isinstance(stats_payload, list):
            return []

        for value in stats_payload:
            if isinstance(value, dict):
                stats_map.append((int(value.get("market_id") or market_id), value))

        return stats_map

    def _handle_order_book(self, payload: dict[str, Any]) -> None:
        market_id = market_id_from_channel(payload.get("channel"))
        if market_id is None:
            return
        instrument = self._instrument_provider.instrument_for_market_id(market_id)
        if instrument is None:
            return

        order_book = payload.get("order_book") or {}
        ts_init = self._clock.timestamp_ns()
        ts_event = epoch_to_nanos(payload.get("timestamp")) or ts_init
        sequence = int(payload.get("offset") or order_book.get("offset") or 0)
        bids = order_book.get("bids") or []
        asks = order_book.get("asks") or []
        message_type = str(payload.get("type") or "")
        if message_type.startswith("subscribed/"):
            self._set_book_snapshot(market_id, bids, asks)
            self._book_offsets[market_id] = sequence
            self._handle_data(
                order_book_snapshot(
                    instrument,
                    bids=bids,
                    asks=asks,
                    sequence=sequence,
                    ts_event=ts_event,
                    ts_init=ts_init,
                ),
            )
            self._publish_book_quote(market_id, instrument, ts_event, ts_init)
            return

        if market_id not in self._book_states:
            self._log.warning(
                f"Missing order book snapshot for market_id={market_id}, requesting resync",
            )
            self.create_task(
                self._resync_order_book(market_id, instrument),
                log_msg=f"resync_order_book:{market_id}",
            )
            return

        if sequence <= self._book_offsets.get(market_id, 0):
            return

        self._book_offsets[market_id] = sequence
        self._apply_book_delta(market_id, bids, asks)
        self._handle_data(
            order_book_deltas(
                instrument,
                bids=bids,
                asks=asks,
                sequence=sequence,
                ts_event=ts_event,
                ts_init=ts_init,
            ),
        )
        self._publish_book_quote(market_id, instrument, ts_event, ts_init)

    def _handle_ticker(self, payload: dict[str, Any]) -> None:
        market_id = market_id_from_channel(payload.get("channel"))
        if market_id is None:
            return
        instrument = self._instrument_provider.instrument_for_market_id(market_id)
        if instrument is None:
            return
        ticker = payload.get("ticker") or {}
        ts_init = self._clock.timestamp_ns()
        quote = quote_tick_from_ticker(instrument, ticker, ts_init, ts_init)
        if quote is not None:
            self._last_quotes[instrument.id.value] = quote
            self._handle_data(quote)

    def _handle_trade(self, payload: dict[str, Any]) -> None:
        market_id = market_id_from_channel(payload.get("channel"))
        if market_id is None:
            return
        instrument = self._instrument_provider.instrument_for_market_id(market_id)
        if instrument is None:
            return
        ts_init = self._clock.timestamp_ns()
        trades = payload.get("trades")
        if trades is None and payload.get("trade") is not None:
            trades = [payload["trade"]]
        for trade in trades or []:
            self._handle_data(trade_tick_from_trade(instrument, trade, ts_init))

    def _handle_market_stats(self, payload: dict[str, Any], channel: str) -> None:
        ts_init = self._clock.timestamp_ns()
        for market_id, market_stats in self._iter_market_stats(payload, channel):
            instrument = self._instrument_provider.instrument_for_market_id(market_id)
            if instrument is None:
                continue
            for update in market_stats_to_updates(
                instrument,
                market_stats,
                ts_event=ts_init,
                ts_init=ts_init,
            ):
                if update.__class__.__name__ != "FundingRateUpdate" or self._is_perp_instrument(
                    instrument.id,
                ):
                    self._handle_data(update)

    def _set_book_snapshot(
        self,
        market_id: int,
        bids: list[dict[str, Any]],
        asks: list[dict[str, Any]],
    ) -> None:
        self._book_states[market_id] = {
            "bids": {
                str(level["price"]): str(level["size"]) for level in bids if level.get("price")
            },
            "asks": {
                str(level["price"]): str(level["size"]) for level in asks if level.get("price")
            },
        }

    def _apply_book_delta(
        self,
        market_id: int,
        bids: list[dict[str, Any]],
        asks: list[dict[str, Any]],
    ) -> None:
        state = self._book_states.setdefault(market_id, {"bids": {}, "asks": {}})
        self._apply_book_side(state["bids"], bids)
        self._apply_book_side(state["asks"], asks)

    def _apply_book_side(
        self,
        side_state: dict[str, str],
        levels: list[dict[str, Any]],
    ) -> None:
        for level in levels:
            price = str(level.get("price") or "")
            if not price:
                continue
            size = Decimal(str(level.get("size") or 0))
            if size <= 0:
                side_state.pop(price, None)
            else:
                side_state[price] = str(level["size"])

    def _publish_book_quote(
        self,
        market_id: int,
        instrument,
        ts_event: int,
        ts_init: int,
    ) -> None:
        state = self._book_states.get(market_id)
        if not state or not state["bids"] or not state["asks"]:
            return

        best_bid_price = max(state["bids"], key=lambda value: Decimal(value))
        best_ask_price = min(state["asks"], key=lambda value: Decimal(value))
        quote = quote_tick_from_ticker(
            instrument,
            {
                "b": {"price": best_bid_price, "size": state["bids"][best_bid_price]},
                "a": {"price": best_ask_price, "size": state["asks"][best_ask_price]},
            },
            ts_event=ts_event,
            ts_init=ts_init,
        )
        if quote is not None:
            self._last_quotes[instrument.id.value] = quote
            self._handle_data(quote)

    async def _resync_order_book(self, market_id: int, instrument) -> None:
        response = loads(await self._client.request_order_book_snapshot(market_id, limit=100))
        bids = response.get("bids") or []
        asks = response.get("asks") or []
        ts_init = self._clock.timestamp_ns()
        self._set_book_snapshot(market_id, bids, asks)
        self._book_offsets[market_id] = int(response.get("offset") or 0)
        self._handle_data(
            order_book_snapshot(
                instrument,
                bids=bids,
                asks=asks,
                sequence=self._book_offsets[market_id],
                ts_event=ts_init,
                ts_init=ts_init,
            ),
        )
        self._publish_book_quote(market_id, instrument, ts_init, ts_init)

    async def _acquire_market_stats(self, instrument_id) -> None:
        if not self._is_perp_instrument(instrument_id):
            self._log.warning(
                f"Lighter market stats updates are only available for perpetuals: {instrument_id}",
            )
            return

        if self._market_stats_refcount == 0:
            await self._ws_client.subscribe_market_stats()
        self._market_stats_refcount += 1

    async def _release_market_stats(self, instrument_id) -> None:
        if not self._is_perp_instrument(instrument_id):
            return

        if self._market_stats_refcount == 0:
            return

        self._market_stats_refcount -= 1
        if self._market_stats_refcount == 0:
            await self._ws_client.unsubscribe_market_stats()

    async def _subscribe_instrument(self, command: SubscribeInstrument) -> None:
        self._log.info(f"Subscribed to instrument updates for {command.instrument_id}")

    async def _subscribe_instruments(self, command: SubscribeInstruments) -> None:
        self._log.info("Subscribed to instruments updates")

    async def _subscribe_order_book_deltas(self, command: SubscribeOrderBook) -> None:
        if command.book_type != BookType.L2_MBP:
            self._log.warning(
                f"Book type {book_type_to_str(command.book_type)} not supported by Lighter",
            )
            return
        market_id = self._instrument_provider.market_id_for_instrument(command.instrument_id)
        if market_id is not None:
            await self._ws_client.subscribe_book(market_id)

    async def _subscribe_order_book_depth(self, command: SubscribeOrderBook) -> None:
        await self._subscribe_order_book_deltas(command)

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        market_id = self._instrument_provider.market_id_for_instrument(command.instrument_id)
        if market_id is not None:
            await self._ws_client.subscribe_quotes(market_id)

    async def _subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None:
        market_id = self._instrument_provider.market_id_for_instrument(command.instrument_id)
        if market_id is not None:
            await self._ws_client.subscribe_trades(market_id)

    async def _subscribe_mark_prices(self, command: SubscribeMarkPrices) -> None:
        await self._acquire_market_stats(command.instrument_id)

    async def _subscribe_index_prices(self, command: SubscribeIndexPrices) -> None:
        await self._acquire_market_stats(command.instrument_id)

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        self._log.warning(
            f"Live external bars are not available from Lighter for {command.bar_type}"
        )

    async def _subscribe_funding_rates(self, command: SubscribeFundingRates) -> None:
        await self._acquire_market_stats(command.instrument_id)

    async def _unsubscribe_instrument(self, command: UnsubscribeInstrument) -> None:
        self._log.info(f"Unsubscribed from instrument updates for {command.instrument_id}")

    async def _unsubscribe_instruments(self, command: UnsubscribeInstruments) -> None:
        self._log.info("Unsubscribed from instruments updates")

    async def _unsubscribe_order_book_deltas(self, command: UnsubscribeOrderBook) -> None:
        market_id = self._instrument_provider.market_id_for_instrument(command.instrument_id)
        if market_id is not None:
            await self._ws_client.unsubscribe_book(market_id)

    async def _unsubscribe_order_book(self, command: UnsubscribeOrderBook) -> None:
        await self._unsubscribe_order_book_deltas(command)

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        market_id = self._instrument_provider.market_id_for_instrument(command.instrument_id)
        if market_id is not None:
            await self._ws_client.unsubscribe_quotes(market_id)

    async def _unsubscribe_trade_ticks(self, command: UnsubscribeTradeTicks) -> None:
        market_id = self._instrument_provider.market_id_for_instrument(command.instrument_id)
        if market_id is not None:
            await self._ws_client.unsubscribe_trades(market_id)

    async def _unsubscribe_mark_prices(self, command: UnsubscribeMarkPrices) -> None:
        await self._release_market_stats(command.instrument_id)

    async def _unsubscribe_index_prices(self, command: UnsubscribeIndexPrices) -> None:
        await self._release_market_stats(command.instrument_id)

    async def _unsubscribe_bars(self, command: UnsubscribeBars) -> None:
        self._log.info(f"Unsubscribed from bars for {command.bar_type}")

    async def _unsubscribe_funding_rates(self, command: UnsubscribeFundingRates) -> None:
        await self._release_market_stats(command.instrument_id)

    async def _request_instrument(self, request: RequestInstrument) -> None:
        instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot find instrument for {request.instrument_id}")
            return
        self._handle_data_response(
            data_type=request.data_type,
            data=instrument,
            correlation_id=request.id,
        )

    async def _request_instruments(self, request: RequestInstruments) -> None:
        instruments = list(self._instrument_provider.get_all().values())
        self._handle_data_response(
            data_type=request.data_type,
            data=instruments,
            correlation_id=request.id,
        )

    async def _request_order_book_snapshot(self, request: RequestOrderBookSnapshot) -> None:
        instrument = self._cache.instrument(request.instrument_id)
        if instrument is None:
            instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot find instrument for {request.instrument_id}")
            return

        market_id = self._instrument_provider.market_id_for_instrument(request.instrument_id)
        if market_id is None:
            self._log.error(f"Cannot determine market_id for {request.instrument_id}")
            return

        limit = getattr(request, "limit", None) or 100
        response = loads(await self._client.request_order_book_snapshot(market_id, limit=limit))
        ts_init = self._clock.timestamp_ns()
        data = order_book_snapshot(
            instrument,
            bids=response.get("bids") or [],
            asks=response.get("asks") or [],
            sequence=int(response.get("total_bids") or response.get("total_asks") or 0),
            ts_event=ts_init,
            ts_init=ts_init,
        )
        self._handle_data_response(
            data_type=request.data_type,
            data=[data],
            correlation_id=request.id,
            params=request.params,
        )

    async def _request_quote_ticks(self, request: RequestQuoteTicks) -> None:
        self._log.warning(
            "Cannot request historical quotes from Lighter. Subscribe to ticker or order book instead.",
        )

    async def _request_trade_ticks(self, request: RequestTradeTicks) -> None:
        market_id = self._instrument_provider.market_id_for_instrument(request.instrument_id)
        instrument = self._cache.instrument(
            request.instrument_id
        ) or self._instrument_provider.find(
            request.instrument_id,
        )
        if market_id is None or instrument is None:
            self._log.error(f"Cannot request trades for {request.instrument_id}")
            return
        limit = request.limit if request.limit and request.limit > 0 else 200
        response = loads(await self._client.request_recent_trades(market_id, limit=limit))
        ts_init = self._clock.timestamp_ns()
        trades = [
            trade_tick_from_trade(instrument, trade, ts_init)
            for trade in response.get("trades", [])
        ]
        if request.start is not None or request.end is not None:
            trades = [
                trade for trade in trades if _in_window(trade.ts_event, request.start, request.end)
            ]
        self._handle_data_response(
            data_type=request.data_type,
            data=trades,
            correlation_id=request.id,
            start=request.start,
            end=request.end,
            params=request.params,
        )

    async def _request_bars(self, request: RequestBars) -> None:
        if request.bar_type.is_internally_aggregated():
            self._log.error(
                f"Cannot request {request.bar_type} bars: only EXTERNAL bars are available from Lighter",
            )
            return
        if not request.bar_type.spec.is_time_aggregated():
            self._log.error(
                f"Cannot request {request.bar_type} bars: only time bars are exposed by Lighter",
            )
            return
        if request.bar_type.spec.price_type != PriceType.LAST:
            self._log.error(
                f"Cannot request {request.bar_type} bars: only LAST bars are exposed by Lighter",
            )
            return

        market_id = self._instrument_provider.market_id_for_instrument(
            request.bar_type.instrument_id
        )
        instrument = self._cache.instrument(
            request.bar_type.instrument_id
        ) or self._instrument_provider.find(
            request.bar_type.instrument_id,
        )
        if market_id is None or instrument is None:
            self._log.error(f"Cannot request bars for {request.bar_type.instrument_id}")
            return

        granularity = _bar_granularity(request.bar_type)
        response = loads(await self._client.request_candles(market_id, granularity))
        bars = candles_to_bars(instrument, request.bar_type, response.get("candles", []))
        if request.start is not None or request.end is not None:
            bars = [bar for bar in bars if _in_window(bar.ts_event, request.start, request.end)]
        self._handle_bars(
            request.bar_type,
            bars,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_funding_rates(self, request: RequestFundingRates) -> None:
        market_id = self._instrument_provider.market_id_for_instrument(request.instrument_id)
        instrument = self._cache.instrument(
            request.instrument_id
        ) or self._instrument_provider.find(
            request.instrument_id,
        )
        if market_id is None or instrument is None:
            self._log.error(f"Cannot request funding rates for {request.instrument_id}")
            return
        if not self._is_perp_instrument(request.instrument_id):
            self._log.warning(
                f"Cannot request funding rates for non-perpetual instrument {request.instrument_id}",
            )
            return

        response = loads(await self._client.request_funding_rates(market_id))
        ts_init = self._clock.timestamp_ns()
        updates = []
        for item in response.get("funding_rates", []):
            settlement_ts = epoch_to_nanos(item.get("settlement_time")) or ts_init
            updates.extend(
                market_stats_to_updates(
                    instrument,
                    item,
                    ts_event=settlement_ts,
                    ts_init=ts_init,
                ),
            )
        if request.start is not None or request.end is not None:
            updates = [
                update
                for update in updates
                if _in_window(update.ts_event, request.start, request.end)
            ]

        self._handle_data_response(
            data_type=request.data_type,
            data=updates,
            correlation_id=request.id,
            start=request.start,
            end=request.end,
            params=request.params,
        )
