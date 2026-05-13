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

import pytest

from nautilus_trader.core import nautilus_pyo3
from tests.integration_tests.adapters.lighter.sandbox.common import build_public_client
from tests.integration_tests.adapters.lighter.sandbox.common import canonical_channel
from tests.integration_tests.adapters.lighter.sandbox.common import decode_payload
from tests.integration_tests.adapters.lighter.sandbox.common import first_market_id
from tests.integration_tests.adapters.lighter.sandbox.common import first_spot_market_id
from tests.integration_tests.adapters.lighter.sandbox.common import lighter_testnet
from tests.integration_tests.adapters.lighter.sandbox.common import load_provider
from tests.integration_tests.adapters.lighter.sandbox.common import wait_for_channels


@pytest.mark.asyncio
async def test_lighter_public_websocket_smoke() -> None:
    http_client = build_public_client()
    provider = await load_provider(http_client)
    market_id = first_spot_market_id(provider) or first_market_id(provider)

    queue: asyncio.Queue[dict] = asyncio.Queue()

    def handler(raw: str) -> None:
        payload = decode_payload(raw)
        if payload is not None:
            queue.put_nowait(payload)

    url = f"{nautilus_pyo3.get_lighter_ws_base_url(lighter_testnet())}?readonly=true"
    client = nautilus_pyo3.LighterWebSocketClient(url=url, testnet=lighter_testnet())
    await client.connect(asyncio.get_running_loop(), handler)

    try:
        expected_channels = {
            f"order_book/{market_id}",
            f"ticker/{market_id}",
            f"trade/{market_id}",
            "market_stats/all",
            "spot_market_stats/all",
        }

        await client.subscribe_book(market_id)
        await client.subscribe_quotes(market_id)
        await client.subscribe_trades(market_id)
        await client.subscribe_market_stats()
        await client.subscribe_spot_market_stats()

        seen = await wait_for_channels(queue, expected_channels)
        assert set(seen) == {canonical_channel(channel) for channel in expected_channels}
    finally:
        await client.close()
