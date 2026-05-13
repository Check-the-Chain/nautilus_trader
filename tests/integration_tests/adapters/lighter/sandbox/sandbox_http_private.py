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

from tests.integration_tests.adapters.lighter.sandbox.common import build_private_client


@pytest.mark.asyncio
async def test_lighter_private_http_smoke() -> None:
    client, account_index, api_key_index = build_private_client()

    auth_token = await client.create_auth_token(deadline_secs=120, api_key_index=api_key_index)
    assert auth_token

    account = json.loads(await client.request_account(account_index, auth_token))
    account_metadata = json.loads(await client.request_account_metadata(account_index, auth_token))
    account_limits = json.loads(await client.request_account_limits(account_index, auth_token))
    account_api_keys = json.loads(await client.request_account_api_keys(account_index, auth_token))

    assert account["code"] == 200
    assert account["accounts"]
    assert account_metadata["code"] == 200
    assert account_metadata["account_metadatas"] is not None
    assert account_limits["code"] == 200
    assert account_api_keys["code"] == 200

    l1_address = account["accounts"][0].get("l1_address")
    if l1_address:
        sub_accounts = json.loads(await client.request_sub_accounts(l1_address))
        assert sub_accounts["code"] == 200
