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
import os
from collections.abc import Iterable
from typing import Any

import pytest

from nautilus_trader.adapters.lighter.constants import LIGHTER_MARKET_TYPE_PERP
from nautilus_trader.adapters.lighter.factories import get_cached_lighter_http_client
from nautilus_trader.adapters.lighter.providers import LighterInstrumentProvider


def lighter_testnet() -> bool:
    return os.environ.get("LIGHTER_TESTNET", "0") == "1"


def build_public_client():
    return get_cached_lighter_http_client(testnet=lighter_testnet())


def build_private_client():
    missing = [
        name
        for name in (
            "LIGHTER_ACCOUNT_INDEX",
            "LIGHTER_API_KEY_INDEX",
            "LIGHTER_PRIVATE_KEY",
        )
        if not os.environ.get(name)
    ]
    if missing:
        pytest.skip(f"Missing required Lighter private env vars: {', '.join(missing)}")

    return (
        get_cached_lighter_http_client(
            testnet=lighter_testnet(),
            account_index=int(os.environ["LIGHTER_ACCOUNT_INDEX"]),
            api_key_index=int(os.environ["LIGHTER_API_KEY_INDEX"]),
            private_key=os.environ["LIGHTER_PRIVATE_KEY"],
            signer_lib_path=os.environ.get("LIGHTER_SIGNER_LIB_PATH"),
        ),
        int(os.environ["LIGHTER_ACCOUNT_INDEX"]),
        int(os.environ["LIGHTER_API_KEY_INDEX"]),
    )


async def load_provider(client) -> LighterInstrumentProvider:
    provider = LighterInstrumentProvider(client=client)
    await provider.load_all_async()
    return provider


def canonical_channel(channel: str) -> str:
    if "/" not in channel or ":" in channel:
        return channel
    head, tail = channel.split("/", 1)
    return f"{head}:{tail}"


def first_market_id(provider: LighterInstrumentProvider, market_type: str | None = None) -> int:
    for market_id in provider.market_ids():
        metadata = provider.metadata_for_market_id(market_id) or {}
        if market_type is None or metadata.get("market_type") == market_type:
            return market_id

    if market_type is None:
        raise AssertionError("No Lighter markets were loaded")
    raise AssertionError(f"No Lighter markets were loaded for market_type={market_type!r}")


def first_perp_market_id(provider: LighterInstrumentProvider) -> int | None:
    for market_id in provider.market_ids():
        metadata = provider.metadata_for_market_id(market_id) or {}
        if metadata.get("market_type") == LIGHTER_MARKET_TYPE_PERP:
            return market_id
    return None


def first_spot_market_id(provider: LighterInstrumentProvider) -> int | None:
    for market_id in provider.market_ids():
        metadata = provider.metadata_for_market_id(market_id) or {}
        if metadata.get("market_type") != LIGHTER_MARKET_TYPE_PERP:
            return market_id
    return None


def decode_payload(raw: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None

    return payload if isinstance(payload, dict) else None


async def wait_for_channels(
    queue: asyncio.Queue[dict[str, Any]],
    expected_channels: Iterable[str],
    timeout_secs: float = 15.0,
) -> dict[str, dict[str, Any]]:
    expected = {canonical_channel(channel) for channel in expected_channels}
    seen: dict[str, dict[str, Any]] = {}
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_secs

    while expected.difference(seen):
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise AssertionError(
                f"Timed out waiting for websocket channels: {sorted(expected.difference(seen))}",
            )

        payload = await asyncio.wait_for(queue.get(), timeout=remaining)
        if payload.get("type") == "error":
            raise AssertionError(f"Received websocket error payload: {payload}")

        channel = payload.get("channel")
        if isinstance(channel, str):
            channel = canonical_channel(channel)
            if channel in expected:
                seen[channel] = payload

    return seen
