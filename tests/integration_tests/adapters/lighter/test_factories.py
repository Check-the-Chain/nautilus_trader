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

import pytest

from nautilus_trader.adapters.lighter.config import LighterDataClientConfig
from nautilus_trader.adapters.lighter.config import LighterExecClientConfig


class TestLighterDataClientConfig:
    def test_default_config(self):
        config = LighterDataClientConfig()

        assert config.base_url_http is None
        assert config.base_url_ws is None
        assert config.testnet is False
        assert config.http_timeout_secs == 30

    def test_custom_urls_and_proxy(self):
        config = LighterDataClientConfig(
            base_url_http="https://lighter.local",
            base_url_ws="wss://lighter.local/stream",
            http_proxy_url="http://proxy:8080",
            ws_proxy_url="http://proxy:8081",
        )

        assert config.base_url_http == "https://lighter.local"
        assert config.base_url_ws == "wss://lighter.local/stream"
        assert config.http_proxy_url == "http://proxy:8080"
        assert config.ws_proxy_url == "http://proxy:8081"


class TestLighterExecClientConfig:
    def test_default_config(self):
        config = LighterExecClientConfig()

        assert config.account_index is None
        assert config.private_key is None
        assert config.api_key_index is None
        assert config.testnet is False
        assert config.http_timeout_secs == 30
        assert config.nonce_mode == "optimistic"
        assert config.default_auth_token_ttl_secs == 300
        assert config.cancel_all_gtt_secs == 300

    def test_signer_fields(self):
        config = LighterExecClientConfig(
            account_index=7,
            private_key="0xdeadbeef",
            api_key_index=3,
            api_private_keys={3: "0xdeadbeef"},
            signer_lib_path="/usr/local/lib/liblighter_signer.so",
            testnet=True,
        )

        assert config.account_index == 7
        assert config.api_key_index == 3
        assert config.api_private_keys == {3: "0xdeadbeef"}
        assert config.signer_lib_path == "/usr/local/lib/liblighter_signer.so"
        assert config.testnet is True


class TestConfigValidation:
    @pytest.mark.parametrize(("testnet", "expected"), [(False, False), (True, True)])
    def test_data_client_testnet_setting(self, testnet, expected):
        assert LighterDataClientConfig(testnet=testnet).testnet == expected

    @pytest.mark.parametrize(("testnet", "expected"), [(False, False), (True, True)])
    def test_exec_client_testnet_setting(self, testnet, expected):
        assert LighterExecClientConfig(testnet=testnet).testnet == expected
