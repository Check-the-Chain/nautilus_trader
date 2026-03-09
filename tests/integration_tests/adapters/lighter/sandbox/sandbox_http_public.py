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

import pytest

from tests.integration_tests.adapters.lighter.sandbox.common import build_public_client
from tests.integration_tests.adapters.lighter.sandbox.common import first_market_id
from tests.integration_tests.adapters.lighter.sandbox.common import first_perp_market_id
from tests.integration_tests.adapters.lighter.sandbox.common import load_provider


@pytest.mark.asyncio
async def test_lighter_public_http_smoke() -> None:
    client = build_public_client()
    provider = await load_provider(client)

    assert provider.count > 0

    status = json.loads(await client.request_status())
    system_config = json.loads(await client.request_system_config())
    order_books = json.loads(await client.request_order_books())

    assert status
    assert system_config["code"] == 200
    assert order_books["code"] == 200
    assert order_books["order_books"]

    market_id = first_market_id(provider)
    depth = json.loads(await client.request_order_book_snapshot(market_id, limit=5))
    trades = json.loads(await client.request_recent_trades(market_id, limit=5))

    assert depth["code"] == 200
    assert depth["bids"] or depth["asks"]
    assert trades["code"] == 200
    assert trades["trades"]

    perp_market_id = first_perp_market_id(provider)
    if perp_market_id is not None:
        order_book_details = json.loads(await client.request_order_book_details(perp_market_id))
        assert order_book_details["code"] == 200
        assert order_book_details["order_book_details"]
