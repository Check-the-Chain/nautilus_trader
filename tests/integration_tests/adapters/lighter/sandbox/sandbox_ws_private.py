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
from tests.integration_tests.adapters.lighter.sandbox.common import build_private_client
from tests.integration_tests.adapters.lighter.sandbox.common import canonical_channel
from tests.integration_tests.adapters.lighter.sandbox.common import decode_payload
from tests.integration_tests.adapters.lighter.sandbox.common import lighter_testnet
from tests.integration_tests.adapters.lighter.sandbox.common import wait_for_channels


@pytest.mark.asyncio
async def test_lighter_private_websocket_smoke() -> None:
    http_client, account_index, api_key_index = build_private_client()
    auth_token = await http_client.create_auth_token(deadline_secs=120, api_key_index=api_key_index)

    queue: asyncio.Queue[dict] = asyncio.Queue()

    def handler(raw: str) -> None:
        payload = decode_payload(raw)
        if payload is not None:
            queue.put_nowait(payload)

    client = nautilus_pyo3.LighterWebSocketClient(
        testnet=lighter_testnet(),
        auth_token=auth_token,
    )
    await client.connect(asyncio.get_running_loop(), handler)

    try:
        expected_channels = {
            f"account_all_assets/{account_index}",
            f"user_stats/{account_index}",
        }

        await client.subscribe_account_all_assets(account_index)
        await client.subscribe_user_stats(account_index)

        seen = await wait_for_channels(queue, expected_channels)
        assert set(seen) == {canonical_channel(channel) for channel in expected_channels}
    finally:
        await client.close()
