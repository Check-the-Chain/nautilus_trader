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
import json
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import suppress
from inspect import isawaitable
from typing import Any


try:
    import aiohttp
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "The Alpaca adapter requires aiohttp. Install with `nautilus_trader[alpaca]`.",
    ) from exc

import msgspec


class AlpacaWebSocketClient:
    def __init__(self, url: str, headers: dict[str, str]) -> None:
        self.url = url
        self._headers = headers
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._reader_task: asyncio.Task | None = None
        self._handler_disconnect: Callable[[Exception | None], Awaitable[None] | None] | None = None
        self._closing = False

    def is_closed(self) -> bool:
        return self._ws is None or self._ws.closed

    async def connect(
        self,
        loop: asyncio.AbstractEventLoop,
        handler: Callable[[dict[str, Any]], None],
        handler_disconnect: Callable[[Exception | None], Awaitable[None] | None] | None = None,
    ) -> None:
        if not self.is_closed():
            return

        self._closing = False
        self._handler_disconnect = handler_disconnect
        self._session = aiohttp.ClientSession(headers=self._headers)
        self._ws = await self._session.ws_connect(self.url, heartbeat=20)
        self._reader_task = loop.create_task(self._reader(handler))

    async def send_json(self, payload: dict[str, Any]) -> None:
        if self._ws is None:
            raise RuntimeError("WebSocket is not connected")
        await self._ws.send_str(json.dumps(payload))

    async def close(self) -> None:
        self._closing = True
        if self._reader_task is not None:
            self._reader_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._reader_task
            self._reader_task = None

        await self._cleanup_transport()

    async def _reader(self, handler: Callable[[dict[str, Any]], None]) -> None:
        assert self._ws is not None
        disconnect_error: Exception | None = None

        try:
            async for message in self._ws:
                if message.type in {aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY}:
                    payload = self._decode_payload(message.data)
                    if isinstance(payload, list):
                        for item in payload:
                            handler(item)
                    else:
                        handler(payload)
                elif message.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED}:
                    return
                elif message.type == aiohttp.WSMsgType.ERROR:
                    disconnect_error = RuntimeError(f"Alpaca websocket error: {self._ws.exception()}")
                    return
        finally:
            if not self._closing:
                await self._cleanup_transport()
                await self._notify_disconnect(disconnect_error)

    async def _notify_disconnect(self, error: Exception | None) -> None:
        if self._handler_disconnect is None:
            return
        result = self._handler_disconnect(error)
        if isawaitable(result):
            await result

    async def _cleanup_transport(self) -> None:
        if self._ws is not None and not getattr(self._ws, "closed", False):
            close = getattr(self._ws, "close", None)
            if close is not None:
                result = close()
                if isawaitable(result):
                    await result
        self._ws = None

        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    @staticmethod
    def _decode_payload(data: str | bytes | bytearray | memoryview) -> Any:
        if isinstance(data, str):
            return json.loads(data)

        raw = bytes(data)
        with suppress(UnicodeDecodeError, json.JSONDecodeError):
            return json.loads(raw.decode("utf-8"))
        with suppress(msgspec.DecodeError):
            return msgspec.msgpack.decode(raw)

        raise RuntimeError("Unsupported Alpaca websocket payload encoding")
