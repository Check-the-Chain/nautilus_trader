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
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import aiohttp
import pytest

from nautilus_trader.adapters.alpaca.websocket import AlpacaWebSocketClient


class _AsyncMessages:
    def __init__(self, messages):
        self._messages = iter(messages)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._messages)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


@pytest.mark.asyncio
async def test_reader_decodes_binary_json_payloads():
    client = AlpacaWebSocketClient(url="wss://example", headers={})
    client._ws = _AsyncMessages(
        [
            SimpleNamespace(
                type=aiohttp.WSMsgType.BINARY,
                data=json.dumps(
                    [
                        {"stream": "trade_updates", "data": {"event": "new"}},
                        {"stream": "trade_updates", "data": {"event": "fill"}},
                    ],
                ).encode(),
            ),
        ],
    )
    messages = []

    await client._reader(messages.append)

    assert [message["data"]["event"] for message in messages] == ["new", "fill"]


@pytest.mark.asyncio
async def test_reader_notifies_disconnect_handler_on_close():
    client = AlpacaWebSocketClient(url="wss://example", headers={})
    disconnect_handler = AsyncMock()
    client._handler_disconnect = disconnect_handler
    session = MagicMock()
    session.closed = False
    session.close = AsyncMock()
    client._session = session
    client._ws = _AsyncMessages([SimpleNamespace(type=aiohttp.WSMsgType.CLOSED, data=None)])

    await client._reader(lambda _: None)

    disconnect_handler.assert_awaited_once_with(None)
    session.close.assert_awaited_once()
    assert client._session is None
