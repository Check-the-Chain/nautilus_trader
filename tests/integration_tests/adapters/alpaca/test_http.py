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

from unittest.mock import AsyncMock

import pytest

from nautilus_trader.adapters.alpaca.http import AlpacaHttpClient


def test_auth_headers_can_source_environment(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "env-key")
    monkeypatch.setenv("ALPACA_API_SECRET", "env-secret")

    client = AlpacaHttpClient(api_key=None, api_secret=None, paper=True)

    assert client.auth_headers == {
        "APCA-API-KEY-ID": "env-key",
        "APCA-API-SECRET-KEY": "env-secret",
    }


@pytest.mark.asyncio
async def test_get_asset_encodes_slash_symbols(monkeypatch):
    client = AlpacaHttpClient(api_key="key", api_secret="secret", paper=True)
    request = AsyncMock(return_value={})
    monkeypatch.setattr(client, "_request", request)

    await client.get_asset("BTC/USD")

    request.assert_awaited_once_with("GET", "/v2/assets/BTC%2FUSD", base="trading")


@pytest.mark.asyncio
async def test_get_option_contract_encodes_option_symbols(monkeypatch):
    client = AlpacaHttpClient(api_key="key", api_secret="secret", paper=True)
    request = AsyncMock(return_value={})
    monkeypatch.setattr(client, "_request", request)

    await client.get_option_contract("AAPL260320C00150000")

    request.assert_awaited_once_with(
        "GET",
        "/v2/options/contracts/AAPL260320C00150000",
        base="trading",
    )


@pytest.mark.asyncio
async def test_list_orders_passes_symbols_direction_and_page_token(monkeypatch):
    client = AlpacaHttpClient(api_key="key", api_secret="secret", paper=True)
    request = AsyncMock(return_value=[])
    monkeypatch.setattr(client, "_request", request)

    await client.list_orders(
        status="all",
        limit=100,
        symbols=["AAPL", "BTC/USD"],
        nested=True,
        direction="desc",
        page_token="next-page",
    )

    params = request.await_args.kwargs["params"]
    assert params["symbols"] == "AAPL,BTC/USD"
    assert params["nested"] == "true"
    assert params["direction"] == "desc"
    assert params["page_token"] == "next-page"


@pytest.mark.asyncio
async def test_get_activities_passes_direction_and_page_token(monkeypatch):
    client = AlpacaHttpClient(api_key="key", api_secret="secret", paper=True)
    request = AsyncMock(return_value=[])
    monkeypatch.setattr(client, "_request", request)

    await client.get_activities(
        activity_type="FILL",
        page_size=100,
        direction="desc",
        page_token="fill-099",
    )

    params = request.await_args.kwargs["params"]
    assert params["page_size"] == 100
    assert params["direction"] == "desc"
    assert params["page_token"] == "fill-099"


@pytest.mark.asyncio
async def test_get_option_contracts_passes_filters(monkeypatch):
    client = AlpacaHttpClient(api_key="key", api_secret="secret", paper=True)
    request = AsyncMock(return_value={})
    monkeypatch.setattr(client, "_request", request)

    await client.get_option_contracts(
        underlying_symbols=["AAPL"],
        status="active",
        expiration_date_gte="2026-03-01",
        expiration_date_lte="2026-03-31",
        option_type="call",
        style="american",
        limit=250,
        page_token="page-2",
    )

    params = request.await_args.kwargs["params"]
    assert params["underlying_symbols"] == "AAPL"
    assert params["status"] == "active"
    assert params["expiration_date_gte"] == "2026-03-01"
    assert params["expiration_date_lte"] == "2026-03-31"
    assert params["type"] == "call"
    assert params["style"] == "american"
    assert params["limit"] == 250
    assert params["page_token"] == "page-2"


@pytest.mark.asyncio
async def test_get_option_quotes_passes_feed(monkeypatch):
    client = AlpacaHttpClient(api_key="key", api_secret="secret", paper=True)
    request = AsyncMock(return_value={})
    monkeypatch.setattr(client, "_request", request)

    await client.get_option_quotes(
        symbols=["AAPL260320C00150000"],
        start="2026-03-09T10:00:00Z",
        end="2026-03-09T10:01:00Z",
        limit=100,
        feed="opra",
        page_token="page-1",
    )

    params = request.await_args.kwargs["params"]
    assert params["symbols"] == "AAPL260320C00150000"
    assert params["feed"] == "opra"
    assert params["page_token"] == "page-1"
