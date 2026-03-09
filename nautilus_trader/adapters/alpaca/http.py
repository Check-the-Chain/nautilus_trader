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

import os
import urllib.parse
from typing import Any

from nautilus_trader.adapters.alpaca.constants import ALPACA_DATA_BASE_URL
from nautilus_trader.adapters.alpaca.constants import ALPACA_LIVE_TRADING_BASE_URL
from nautilus_trader.adapters.alpaca.constants import ALPACA_PAPER_TRADING_BASE_URL


try:
    import aiohttp
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "The Alpaca adapter requires aiohttp. Install with `nautilus_trader[alpaca]`.",
    ) from exc


class AlpacaHttpClient:
    def __init__(
        self,
        *,
        api_key: str | None,
        api_secret: str | None,
        paper: bool,
        trading_base_url: str | None = None,
        data_base_url: str | None = None,
        timeout_secs: int = 10,
    ) -> None:
        self.api_key = api_key or os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
        self.api_secret = (
            api_secret
            or os.getenv("ALPACA_API_SECRET")
            or os.getenv("APCA_API_SECRET_KEY")
        )
        if not self.api_key or not self.api_secret:
            raise ValueError(
                "Alpaca credentials not configured. Set config.api_key/config.api_secret "
                "or ALPACA_API_KEY/ALPACA_API_SECRET (or APCA_API_KEY_ID/APCA_API_SECRET_KEY).",
            )

        self.paper = paper
        self.trading_base_url = trading_base_url or (
            ALPACA_PAPER_TRADING_BASE_URL if paper else ALPACA_LIVE_TRADING_BASE_URL
        )
        self.data_base_url = data_base_url or ALPACA_DATA_BASE_URL
        self.timeout_secs = timeout_secs
        self._session: aiohttp.ClientSession | None = None

    @property
    def auth_headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout_secs)
            self._session = aiohttp.ClientSession(timeout=timeout, headers=self.auth_headers)
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        base: str,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        session = await self._get_session()
        base_url = self.trading_base_url if base == "trading" else self.data_base_url
        url = f"{base_url}{path}"
        params = {k: v for k, v in (params or {}).items() if v is not None}

        async with session.request(method, url, params=params, json=payload) as response:
            if response.status == 204:
                return None

            content_type = response.headers.get("Content-Type", "")
            if response.status >= 400:
                body = await response.json() if "application/json" in content_type else await response.text()
                raise RuntimeError(f"Alpaca request failed [{response.status}] {body}")

            if "application/json" in content_type:
                return await response.json()
            return await response.text()

    async def get_account(self) -> dict[str, Any]:
        return await self._request("GET", "/v2/account", base="trading")

    async def get_assets(
        self,
        *,
        status: str | None = None,
        asset_class: str | None = None,
    ) -> list[dict[str, Any]]:
        return await self._request(
            "GET",
            "/v2/assets",
            base="trading",
            params={"status": status, "asset_class": asset_class},
        )

    async def get_asset(self, symbol_or_asset_id: str) -> dict[str, Any]:
        encoded = urllib.parse.quote(symbol_or_asset_id, safe="")
        return await self._request("GET", f"/v2/assets/{encoded}", base="trading")

    async def get_option_contracts(
        self,
        *,
        underlying_symbols: list[str] | None = None,
        status: str | None = None,
        expiration_date_gte: str | None = None,
        expiration_date_lte: str | None = None,
        option_type: str | None = None,
        style: str | None = None,
        limit: int | None = None,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        params = {
            "status": status,
            "expiration_date_gte": expiration_date_gte,
            "expiration_date_lte": expiration_date_lte,
            "type": option_type,
            "style": style,
            "limit": limit,
            "page_token": page_token,
        }
        if underlying_symbols:
            params["underlying_symbols"] = ",".join(underlying_symbols)

        return await self._request(
            "GET",
            "/v2/options/contracts",
            base="trading",
            params=params,
        )

    async def get_option_contract(self, symbol_or_contract_id: str) -> dict[str, Any]:
        encoded = urllib.parse.quote(symbol_or_contract_id, safe="")
        return await self._request(
            "GET",
            f"/v2/options/contracts/{encoded}",
            base="trading",
        )

    async def get_positions(self) -> list[dict[str, Any]]:
        return await self._request("GET", "/v2/positions", base="trading")

    async def list_orders(
        self,
        *,
        status: str = "all",
        limit: int | None = None,
        after: str | None = None,
        until: str | None = None,
        symbols: list[str] | None = None,
        nested: bool = False,
        direction: str | None = None,
        page_token: str | None = None,
    ) -> list[dict[str, Any]]:
        params = {
            "status": status,
            "limit": limit,
            "after": after,
            "until": until,
            "nested": str(nested).lower(),
            "direction": direction,
            "page_token": page_token,
        }
        if symbols:
            params["symbols"] = ",".join(symbols)
        return await self._request("GET", "/v2/orders", base="trading", params=params)

    async def get_order(self, order_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/v2/orders/{order_id}", base="trading")

    async def get_order_by_client_order_id(self, client_order_id: str) -> dict[str, Any]:
        return await self._request(
            "GET",
            "/v2/orders:by_client_order_id",
            base="trading",
            params={"client_order_id": client_order_id},
        )

    async def submit_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._request("POST", "/v2/orders", base="trading", payload=payload)

    async def replace_order(self, order_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._request(
            "PATCH",
            f"/v2/orders/{order_id}",
            base="trading",
            payload=payload,
        )

    async def cancel_order(self, order_id: str) -> None:
        await self._request("DELETE", f"/v2/orders/{order_id}", base="trading")

    async def cancel_all_orders(self) -> list[dict[str, Any]]:
        response = await self._request("DELETE", "/v2/orders", base="trading")
        return response or []

    async def get_activities(
        self,
        *,
        activity_type: str,
        after: str | None = None,
        until: str | None = None,
        page_size: int | None = None,
        direction: str | None = None,
        page_token: str | None = None,
    ) -> list[dict[str, Any]]:
        return await self._request(
            "GET",
            f"/v2/account/activities/{activity_type}",
            base="trading",
            params={
                "after": after,
                "until": until,
                "page_size": page_size,
                "direction": direction,
                "page_token": page_token,
            },
        )

    async def get_stock_quotes(
        self,
        *,
        symbols: list[str],
        start: str,
        end: str,
        limit: int | None,
        feed: str,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "GET",
            "/v2/stocks/quotes",
            base="data",
            params={
                "symbols": ",".join(symbols),
                "start": start,
                "end": end,
                "limit": limit,
                "sort": "asc",
                "feed": feed,
                "page_token": page_token,
            },
        )

    async def get_stock_trades(
        self,
        *,
        symbols: list[str],
        start: str,
        end: str,
        limit: int | None,
        feed: str,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "GET",
            "/v2/stocks/trades",
            base="data",
            params={
                "symbols": ",".join(symbols),
                "start": start,
                "end": end,
                "limit": limit,
                "sort": "asc",
                "feed": feed,
                "page_token": page_token,
            },
        )

    async def get_stock_bars(
        self,
        *,
        symbols: list[str],
        timeframe: str,
        start: str,
        end: str,
        limit: int | None,
        feed: str,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "GET",
            "/v2/stocks/bars",
            base="data",
            params={
                "symbols": ",".join(symbols),
                "timeframe": timeframe,
                "start": start,
                "end": end,
                "limit": limit,
                "sort": "asc",
                "feed": feed,
                "page_token": page_token,
            },
        )

    async def get_option_quotes(
        self,
        *,
        symbols: list[str],
        start: str,
        end: str,
        limit: int | None,
        feed: str,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "GET",
            "/v1beta1/options/quotes",
            base="data",
            params={
                "symbols": ",".join(symbols),
                "start": start,
                "end": end,
                "limit": limit,
                "sort": "asc",
                "feed": feed,
                "page_token": page_token,
            },
        )

    async def get_option_trades(
        self,
        *,
        symbols: list[str],
        start: str,
        end: str,
        limit: int | None,
        feed: str,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "GET",
            "/v1beta1/options/trades",
            base="data",
            params={
                "symbols": ",".join(symbols),
                "start": start,
                "end": end,
                "limit": limit,
                "sort": "asc",
                "feed": feed,
                "page_token": page_token,
            },
        )

    async def get_option_bars(
        self,
        *,
        symbols: list[str],
        timeframe: str,
        start: str,
        end: str,
        limit: int | None,
        feed: str,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "GET",
            "/v1beta1/options/bars",
            base="data",
            params={
                "symbols": ",".join(symbols),
                "timeframe": timeframe,
                "start": start,
                "end": end,
                "limit": limit,
                "sort": "asc",
                "feed": feed,
                "page_token": page_token,
            },
        )

    async def get_crypto_quotes(
        self,
        *,
        loc: str,
        symbols: list[str],
        start: str,
        end: str,
        limit: int | None,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/v1beta3/crypto/{loc}/quotes",
            base="data",
            params={
                "symbols": ",".join(symbols),
                "start": start,
                "end": end,
                "limit": limit,
                "sort": "asc",
                "page_token": page_token,
            },
        )

    async def get_crypto_trades(
        self,
        *,
        loc: str,
        symbols: list[str],
        start: str,
        end: str,
        limit: int | None,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/v1beta3/crypto/{loc}/trades",
            base="data",
            params={
                "symbols": ",".join(symbols),
                "start": start,
                "end": end,
                "limit": limit,
                "sort": "asc",
                "page_token": page_token,
            },
        )

    async def get_crypto_bars(
        self,
        *,
        loc: str,
        symbols: list[str],
        timeframe: str,
        start: str,
        end: str,
        limit: int | None,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/v1beta3/crypto/{loc}/bars",
            base="data",
            params={
                "symbols": ",".join(symbols),
                "timeframe": timeframe,
                "start": start,
                "end": end,
                "limit": limit,
                "sort": "asc",
                "page_token": page_token,
            },
        )
