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
from contextlib import suppress
from typing import Any

import pandas as pd

from nautilus_trader.adapters.alpaca.common import bar_type_to_timeframe
from nautilus_trader.adapters.alpaca.common import data_symbol_for_instrument
from nautilus_trader.adapters.alpaca.common import extract_items_for_symbol
from nautilus_trader.adapters.alpaca.common import is_crypto_instrument
from nautilus_trader.adapters.alpaca.common import is_option_instrument
from nautilus_trader.adapters.alpaca.common import make_bar
from nautilus_trader.adapters.alpaca.common import make_quote_tick
from nautilus_trader.adapters.alpaca.common import make_trade_tick
from nautilus_trader.adapters.alpaca.config import AlpacaDataClientConfig
from nautilus_trader.adapters.alpaca.constants import ALPACA_DATA_WS_BASE_URL
from nautilus_trader.adapters.alpaca.constants import ALPACA_DEFAULT_CRYPTO_LOC
from nautilus_trader.adapters.alpaca.constants import ALPACA_DEFAULT_STOCK_FEED
from nautilus_trader.adapters.alpaca.constants import ALPACA_VENUE
from nautilus_trader.adapters.alpaca.http import AlpacaHttpClient
from nautilus_trader.adapters.alpaca.providers import AlpacaInstrumentProvider
from nautilus_trader.adapters.alpaca.websocket import AlpacaWebSocketClient
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.data.messages import RequestBars
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
from nautilus_trader.model.data import BarSpecification
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.identifiers import ClientId


class AlpacaDataClient(LiveMarketDataClient):
    """
    Provides a live market data client for Alpaca.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: AlpacaHttpClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: AlpacaInstrumentProvider,
        config: AlpacaDataClientConfig,
        name: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or ALPACA_VENUE.value),
            venue=ALPACA_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            config=config,
        )

        self._client = client
        self._config = config
        self._instrument_provider = instrument_provider

        self._stock_ws: AlpacaWebSocketClient | None = None
        self._crypto_ws: AlpacaWebSocketClient | None = None
        self._option_ws: AlpacaWebSocketClient | None = None
        self._stock_reconnect_task: asyncio.Task | None = None
        self._crypto_reconnect_task: asyncio.Task | None = None
        self._option_reconnect_task: asyncio.Task | None = None
        self._is_disconnecting = False
        self._stock_subscriptions: dict[str, set[str]] = {
            "quotes": set(),
            "trades": set(),
            "bars": set(),
        }
        self._crypto_subscriptions: dict[str, set[str]] = {
            "quotes": set(),
            "trades": set(),
            "bars": set(),
        }
        self._option_subscriptions: dict[str, set[str]] = {
            "quotes": set(),
            "trades": set(),
            "bars": set(),
        }
        self._bar_types: dict[str, BarType] = {}

        self._log.info(f"config.paper={config.paper}", LogColor.BLUE)
        self._log.info(f"config.stock_feed={config.stock_feed}", LogColor.BLUE)
        self._log.info(f"config.crypto_loc={config.crypto_loc}", LogColor.BLUE)
        self._log.info(f"config.option_feed={config.option_feed}", LogColor.BLUE)
        self._log.info(f"config.http_timeout_secs={config.http_timeout_secs}", LogColor.BLUE)

    @property
    def instrument_provider(self) -> AlpacaInstrumentProvider:
        return self._instrument_provider

    async def _connect(self) -> None:
        self._is_disconnecting = False
        await self._instrument_provider.initialize()
        self._send_all_instruments_to_data_engine()

    async def _disconnect(self) -> None:
        self._is_disconnecting = True
        await self._cancel_reconnect_task("_stock_reconnect_task")
        await self._cancel_reconnect_task("_crypto_reconnect_task")
        await self._cancel_reconnect_task("_option_reconnect_task")
        if self._stock_ws is not None:
            await self._stock_ws.close()
            self._stock_ws = None
        if self._crypto_ws is not None:
            await self._crypto_ws.close()
            self._crypto_ws = None
        if self._option_ws is not None:
            await self._option_ws.close()
            self._option_ws = None

    def _send_all_instruments_to_data_engine(self) -> None:
        for instrument in self._instrument_provider.get_all().values():
            self._handle_data(instrument)
        for currency in self._instrument_provider.currencies().values():
            self._cache.add_currency(currency)

    async def _ensure_instrument(self, instrument_id):
        instrument = self._cache.instrument(instrument_id)
        if instrument is not None:
            return instrument

        instrument = self._instrument_provider.find(instrument_id)
        if instrument is not None:
            return instrument

        await self._instrument_provider.load_async(instrument_id)
        return self._instrument_provider.find(instrument_id)

    async def _ensure_stock_ws_connected(self) -> None:
        if self._stock_ws is not None and not self._stock_ws.is_closed():
            return

        base_url = self._config.data_ws_base_url or ALPACA_DATA_WS_BASE_URL
        stock_feed = self._config.stock_feed or ALPACA_DEFAULT_STOCK_FEED
        url = f"{base_url.rstrip('/')}/v2/{stock_feed}"
        self._stock_ws = AlpacaWebSocketClient(url=url, headers=self._client.auth_headers)
        try:
            await self._stock_ws.connect(
                self._loop,
                self._handle_stock_msg,
                handler_disconnect=self._handle_stock_ws_disconnect,
            )
            await self._stock_ws.send_json(
                {
                    "action": "auth",
                    "key": self._client.api_key,
                    "secret": self._client.api_secret,
                },
            )
            await self._replay_subscriptions(self._stock_ws, self._stock_subscriptions)
        except Exception:
            await self._stock_ws.close()
            self._stock_ws = None
            raise

    async def _ensure_crypto_ws_connected(self) -> None:
        if self._crypto_ws is not None and not self._crypto_ws.is_closed():
            return

        base_url = self._config.data_ws_base_url or ALPACA_DATA_WS_BASE_URL
        crypto_loc = self._config.crypto_loc or ALPACA_DEFAULT_CRYPTO_LOC
        url = f"{base_url.rstrip('/')}/v1beta3/crypto/{crypto_loc}"
        self._crypto_ws = AlpacaWebSocketClient(url=url, headers=self._client.auth_headers)
        try:
            await self._crypto_ws.connect(
                self._loop,
                self._handle_crypto_msg,
                handler_disconnect=self._handle_crypto_ws_disconnect,
            )
            await self._crypto_ws.send_json(
                {
                    "action": "auth",
                    "key": self._client.api_key,
                    "secret": self._client.api_secret,
                },
            )
            await self._replay_subscriptions(self._crypto_ws, self._crypto_subscriptions)
        except Exception:
            await self._crypto_ws.close()
            self._crypto_ws = None
            raise

    async def _ensure_option_ws_connected(self) -> None:
        if self._option_ws is not None and not self._option_ws.is_closed():
            return

        base_url = self._config.data_ws_base_url or ALPACA_DATA_WS_BASE_URL
        url = f"{base_url.rstrip('/')}/v1beta1/{self._config.option_feed}"
        self._option_ws = AlpacaWebSocketClient(url=url, headers=self._client.auth_headers)
        try:
            await self._option_ws.connect(
                self._loop,
                self._handle_option_msg,
                handler_disconnect=self._handle_option_ws_disconnect,
            )
            await self._option_ws.send_json(
                {
                    "action": "auth",
                    "key": self._client.api_key,
                    "secret": self._client.api_secret,
                },
            )
            await self._replay_subscriptions(self._option_ws, self._option_subscriptions)
        except Exception:
            await self._option_ws.close()
            self._option_ws = None
            raise

    def _handle_stock_msg(self, msg: dict[str, Any]) -> None:
        self._handle_ws_msg(msg, asset_kind="stock")

    def _handle_crypto_msg(self, msg: dict[str, Any]) -> None:
        self._handle_ws_msg(msg, asset_kind="crypto")

    def _handle_option_msg(self, msg: dict[str, Any]) -> None:
        self._handle_ws_msg(msg, asset_kind="option")

    def _handle_ws_msg(self, msg: dict[str, Any], *, asset_kind: str) -> None:
        msg_type = msg.get("T")
        if msg_type in {"success", "subscription"}:
            return
        if msg_type == "error":
            self._log.error(f"Alpaca {asset_kind} stream error: {msg}")
            return

        symbol = msg.get("S")
        if not symbol:
            return

        instrument = self._instrument_provider.instrument_for_symbol(symbol)
        if instrument is None:
            self._log.warning(f"Ignoring Alpaca {asset_kind} message for unknown symbol {symbol}")
            return

        ts_init = self._clock.timestamp_ns()
        if msg_type == "q":
            self._handle_data(make_quote_tick(instrument, msg, ts_init))
        elif msg_type == "t":
            self._handle_data(make_trade_tick(instrument, msg, ts_init))
        elif msg_type == "b":
            bar_type = self._bar_types.get(
                data_symbol_for_instrument(instrument),
                BarType(
                    instrument.id,
                    BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
                    AggregationSource.EXTERNAL,
                ),
            )
            self._handle_data(make_bar(instrument, bar_type, msg, ts_init))

    async def _subscribe_instrument(self, command: SubscribeInstrument) -> None:
        self._log.info(f"Subscribed to instrument updates for {command.instrument_id}")

    async def _subscribe_instruments(self, command: SubscribeInstruments) -> None:
        self._log.info(f"Subscribed to instruments for {self.venue}")

    async def _subscribe_order_book_deltas(self, command: SubscribeOrderBook) -> None:
        self._log.warning("Alpaca order book deltas are not supported by this adapter")

    async def _subscribe_order_book_depth(self, command: SubscribeOrderBook) -> None:
        self._log.warning("Alpaca order book depth is not supported by this adapter")

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        instrument = await self._ensure_instrument(command.instrument_id)
        if instrument is None:
            self._log.error(f"Instrument not found: {command.instrument_id}")
            return
        await self._subscribe_symbol(instrument=instrument, channel="quotes")

    async def _subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None:
        instrument = await self._ensure_instrument(command.instrument_id)
        if instrument is None:
            self._log.error(f"Instrument not found: {command.instrument_id}")
            return
        await self._subscribe_symbol(instrument=instrument, channel="trades")

    async def _subscribe_mark_prices(self, command: SubscribeMarkPrices) -> None:
        self._log.warning("Alpaca mark prices are not supported")

    async def _subscribe_index_prices(self, command: SubscribeIndexPrices) -> None:
        self._log.warning("Alpaca index prices are not supported")

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        if (
            command.bar_type.aggregation_source != AggregationSource.EXTERNAL
            or command.bar_type.spec.price_type != PriceType.LAST
            or command.bar_type.spec.aggregation != BarAggregation.MINUTE
            or command.bar_type.spec.step != 1
        ):
            self._log.error(
                f"Cannot subscribe {command.bar_type}: Alpaca live bars are 1-minute LAST bars only",
            )
            return

        instrument = await self._ensure_instrument(command.bar_type.instrument_id)
        if instrument is None:
            self._log.error(f"Instrument not found: {command.bar_type.instrument_id}")
            return
        if is_option_instrument(instrument):
            self._log.error(
                f"Cannot subscribe {command.bar_type}: Alpaca live option bars are not supported",
            )
            return
        self._bar_types[data_symbol_for_instrument(instrument)] = command.bar_type
        await self._subscribe_symbol(instrument=instrument, channel="bars")

    async def _subscribe_funding_rates(self, command: SubscribeFundingRates) -> None:
        self._log.warning("Alpaca funding rates are not supported")

    async def _unsubscribe_instrument(self, command: UnsubscribeInstrument) -> None:
        self._log.info(f"Unsubscribed from instrument updates for {command.instrument_id}")

    async def _unsubscribe_instruments(self, command: UnsubscribeInstruments) -> None:
        self._log.info(f"Unsubscribed from instruments for {self.venue}")

    async def _unsubscribe_order_book_deltas(self, command: UnsubscribeOrderBook) -> None:
        self._log.warning("Alpaca order book deltas are not supported")

    async def _unsubscribe_order_book_depth(self, command: UnsubscribeOrderBook) -> None:
        self._log.warning("Alpaca order book depth is not supported")

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        instrument = self._instrument_provider.find(command.instrument_id)
        if instrument is None:
            return
        await self._unsubscribe_symbol(instrument=instrument, channel="quotes")

    async def _unsubscribe_trade_ticks(self, command: UnsubscribeTradeTicks) -> None:
        instrument = self._instrument_provider.find(command.instrument_id)
        if instrument is None:
            return
        await self._unsubscribe_symbol(instrument=instrument, channel="trades")

    async def _unsubscribe_mark_prices(self, command: UnsubscribeMarkPrices) -> None:
        self._log.warning("Alpaca mark prices are not supported")

    async def _unsubscribe_index_prices(self, command: UnsubscribeIndexPrices) -> None:
        self._log.warning("Alpaca index prices are not supported")

    async def _unsubscribe_bars(self, command: UnsubscribeBars) -> None:
        instrument = self._instrument_provider.find(command.bar_type.instrument_id)
        if instrument is None:
            return
        self._bar_types.pop(data_symbol_for_instrument(instrument), None)
        await self._unsubscribe_symbol(instrument=instrument, channel="bars")

    async def _unsubscribe_funding_rates(self, command: UnsubscribeFundingRates) -> None:
        self._log.warning("Alpaca funding rates are not supported")

    async def _request_instrument(self, request: RequestInstrument) -> None:
        instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is None:
            await self._instrument_provider.load_async(request.instrument_id)
            instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is None:
            self._log.error(f"Instrument not found: {request.instrument_id}")
            return
        self._handle_instrument(instrument, request.id, request.start, request.end, request.params)

    async def _request_instruments(self, request: RequestInstruments) -> None:
        instruments = [
            instrument
            for instrument in self._instrument_provider.get_all().values()
            if request.venue is None or instrument.venue == request.venue
        ]
        self._handle_instruments(
            request.venue,
            instruments,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_order_book_snapshot(self, request: RequestOrderBookSnapshot) -> None:
        self._log.error("Alpaca order book snapshots are not supported by this adapter")

    async def _request_quote_ticks(self, request: RequestQuoteTicks) -> None:
        instrument = await self._ensure_instrument(request.instrument_id)
        if instrument is None:
            self._log.error(f"Instrument not found: {request.instrument_id}")
            return

        start = self._timestamp_to_iso(request.start)
        end = self._timestamp_to_iso(request.end)
        limit = request.limit or 1000
        symbol = request.instrument_id.symbol.value
        request_symbol = data_symbol_for_instrument(instrument)
        quotes = [
            make_quote_tick(instrument, quote, self._clock.timestamp_ns())
            for quote in await self._collect_historical_items(
                instrument=instrument,
                request_symbol=request_symbol,
                symbol=symbol,
                key="quotes",
                limit=limit,
                request_fn=(
                    self._client.get_option_quotes
                    if is_option_instrument(instrument)
                    else self._client.get_crypto_quotes
                    if is_crypto_instrument(instrument)
                    else self._client.get_stock_quotes
                ),
                request_kwargs=(
                    {
                        "symbols": [request_symbol],
                        "start": start,
                        "end": end,
                        "feed": self._config.option_feed,
                    }
                    if is_option_instrument(instrument)
                    else
                    {
                        "loc": self._config.crypto_loc,
                        "symbols": [request_symbol],
                        "start": start,
                        "end": end,
                    }
                    if is_crypto_instrument(instrument)
                    else {
                        "symbols": [request_symbol],
                        "start": start,
                        "end": end,
                        "feed": self._config.stock_feed,
                    }
                ),
            )
        ]
        self._handle_quote_ticks(
            request.instrument_id,
            quotes,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_trade_ticks(self, request: RequestTradeTicks) -> None:
        instrument = await self._ensure_instrument(request.instrument_id)
        if instrument is None:
            self._log.error(f"Instrument not found: {request.instrument_id}")
            return

        start = self._timestamp_to_iso(request.start)
        end = self._timestamp_to_iso(request.end)
        limit = request.limit or 1000
        symbol = request.instrument_id.symbol.value
        request_symbol = data_symbol_for_instrument(instrument)
        trades = [
            make_trade_tick(instrument, trade, self._clock.timestamp_ns())
            for trade in await self._collect_historical_items(
                instrument=instrument,
                request_symbol=request_symbol,
                symbol=symbol,
                key="trades",
                limit=limit,
                request_fn=(
                    self._client.get_option_trades
                    if is_option_instrument(instrument)
                    else self._client.get_crypto_trades
                    if is_crypto_instrument(instrument)
                    else self._client.get_stock_trades
                ),
                request_kwargs=(
                    {
                        "symbols": [request_symbol],
                        "start": start,
                        "end": end,
                        "feed": self._config.option_feed,
                    }
                    if is_option_instrument(instrument)
                    else
                    {
                        "loc": self._config.crypto_loc,
                        "symbols": [request_symbol],
                        "start": start,
                        "end": end,
                    }
                    if is_crypto_instrument(instrument)
                    else {
                        "symbols": [request_symbol],
                        "start": start,
                        "end": end,
                        "feed": self._config.stock_feed,
                    }
                ),
            )
        ]
        self._handle_trade_ticks(
            request.instrument_id,
            trades,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_bars(self, request: RequestBars) -> None:
        instrument = await self._ensure_instrument(request.bar_type.instrument_id)
        if instrument is None:
            self._log.error(f"Instrument not found: {request.bar_type.instrument_id}")
            return

        timeframe = bar_type_to_timeframe(request.bar_type)
        start = self._timestamp_to_iso(request.start)
        end = self._timestamp_to_iso(request.end)
        limit = request.limit or 1000
        symbol = request.bar_type.instrument_id.symbol.value
        request_symbol = data_symbol_for_instrument(instrument)
        bars = [
            make_bar(instrument, request.bar_type, bar, self._clock.timestamp_ns())
            for bar in await self._collect_historical_items(
                instrument=instrument,
                request_symbol=request_symbol,
                symbol=symbol,
                key="bars",
                limit=limit,
                request_fn=(
                    self._client.get_option_bars
                    if is_option_instrument(instrument)
                    else self._client.get_crypto_bars
                    if is_crypto_instrument(instrument)
                    else self._client.get_stock_bars
                ),
                request_kwargs=(
                    {
                        "symbols": [request_symbol],
                        "timeframe": timeframe,
                        "start": start,
                        "end": end,
                        "feed": self._config.option_feed,
                    }
                    if is_option_instrument(instrument)
                    else
                    {
                        "loc": self._config.crypto_loc,
                        "symbols": [request_symbol],
                        "timeframe": timeframe,
                        "start": start,
                        "end": end,
                    }
                    if is_crypto_instrument(instrument)
                    else {
                        "symbols": [request_symbol],
                        "timeframe": timeframe,
                        "start": start,
                        "end": end,
                        "feed": self._config.stock_feed,
                    }
                ),
            )
        ]
        self._handle_bars(
            request.bar_type,
            bars,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _subscribe_symbol(self, *, instrument, channel: str) -> None:
        symbol = data_symbol_for_instrument(instrument)
        subscriptions = self._subscriptions_for_instrument(instrument)
        if symbol in subscriptions[channel]:
            return

        if is_option_instrument(instrument):
            await self._ensure_option_ws_connected()
            assert self._option_ws is not None
            await self._option_ws.send_json({"action": "subscribe", channel: [symbol]})
        elif is_crypto_instrument(instrument):
            await self._ensure_crypto_ws_connected()
            assert self._crypto_ws is not None
            await self._crypto_ws.send_json({"action": "subscribe", channel: [symbol]})
        else:
            await self._ensure_stock_ws_connected()
            assert self._stock_ws is not None
            await self._stock_ws.send_json({"action": "subscribe", channel: [symbol]})

        subscriptions[channel].add(symbol)

    async def _unsubscribe_symbol(self, *, instrument, channel: str) -> None:
        symbol = data_symbol_for_instrument(instrument)
        subscriptions = self._subscriptions_for_instrument(instrument)
        if symbol not in subscriptions[channel]:
            return

        if is_option_instrument(instrument):
            if self._option_ws is None:
                return
            await self._option_ws.send_json({"action": "unsubscribe", channel: [symbol]})
        elif is_crypto_instrument(instrument):
            if self._crypto_ws is None:
                return
            await self._crypto_ws.send_json({"action": "unsubscribe", channel: [symbol]})
        else:
            if self._stock_ws is None:
                return
            await self._stock_ws.send_json({"action": "unsubscribe", channel: [symbol]})

        subscriptions[channel].discard(symbol)

    async def _replay_subscriptions(
        self,
        ws_client: AlpacaWebSocketClient,
        subscriptions: dict[str, set[str]],
    ) -> None:
        for channel, symbols in subscriptions.items():
            if symbols:
                await ws_client.send_json(
                    {"action": "subscribe", channel: sorted(symbols)},
                )

    async def _handle_stock_ws_disconnect(self, error: Exception | None) -> None:
        self._stock_ws = None
        if self._is_disconnecting or not self._has_active_subscriptions(self._stock_subscriptions):
            return
        self._log.warning(f"Alpaca stock websocket disconnected, reconnecting ({error or 'closed'})")
        if self._stock_reconnect_task is None or self._stock_reconnect_task.done():
            self._stock_reconnect_task = self._loop.create_task(
                self._reconnect_market_ws(kind="stock"),
            )

    async def _handle_crypto_ws_disconnect(self, error: Exception | None) -> None:
        self._crypto_ws = None
        if self._is_disconnecting or not self._has_active_subscriptions(self._crypto_subscriptions):
            return
        self._log.warning(f"Alpaca crypto websocket disconnected, reconnecting ({error or 'closed'})")
        if self._crypto_reconnect_task is None or self._crypto_reconnect_task.done():
            self._crypto_reconnect_task = self._loop.create_task(
                self._reconnect_market_ws(kind="crypto"),
            )

    async def _handle_option_ws_disconnect(self, error: Exception | None) -> None:
        self._option_ws = None
        if self._is_disconnecting or not self._has_active_subscriptions(self._option_subscriptions):
            return
        self._log.warning(f"Alpaca option websocket disconnected, reconnecting ({error or 'closed'})")
        if self._option_reconnect_task is None or self._option_reconnect_task.done():
            self._option_reconnect_task = self._loop.create_task(
                self._reconnect_market_ws(kind="option"),
            )

    async def _reconnect_market_ws(self, *, kind: str) -> None:
        while not self._is_disconnecting:
            try:
                if kind == "stock":
                    await self._ensure_stock_ws_connected()
                elif kind == "option":
                    await self._ensure_option_ws_connected()
                else:
                    await self._ensure_crypto_ws_connected()
                return
            except Exception as exc:
                self._log.warning(f"Alpaca {kind} websocket reconnect failed: {exc}")
                await asyncio.sleep(1.0)

    def _subscriptions_for_instrument(self, instrument) -> dict[str, set[str]]:
        if is_option_instrument(instrument):
            return self._option_subscriptions
        if is_crypto_instrument(instrument):
            return self._crypto_subscriptions
        return self._stock_subscriptions

    async def _cancel_reconnect_task(self, attr_name: str) -> None:
        task = getattr(self, attr_name)
        if task is None:
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        setattr(self, attr_name, None)

    @staticmethod
    def _has_active_subscriptions(subscriptions: dict[str, set[str]]) -> bool:
        return any(symbols for symbols in subscriptions.values())

    async def _collect_historical_items(
        self,
        *,
        instrument,
        request_symbol: str,
        symbol: str,
        key: str,
        limit: int,
        request_fn,
        request_kwargs: dict[str, Any],
    ) -> list[dict[str, Any]]:
        del instrument  # Kept in signature for symmetry with call sites.

        items: list[dict[str, Any]] = []
        page_token: str | None = None

        while True:
            remaining = max(limit - len(items), 0)
            if remaining == 0:
                break

            payload = await request_fn(
                **request_kwargs,
                limit=remaining,
                page_token=page_token,
            )
            items.extend(extract_items_for_symbol(payload, key, symbol))
            page_token = self._next_page_token(payload)
            if page_token is None:
                break

        return items[:limit]

    @staticmethod
    def _next_page_token(payload: dict[str, Any]) -> str | None:
        token = payload.get("next_page_token")
        return str(token) if token else None

    @staticmethod
    def _timestamp_to_iso(value: Any) -> str | None:
        if value is None:
            return None
        return pd.Timestamp(value).isoformat()
