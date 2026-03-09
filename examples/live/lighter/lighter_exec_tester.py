#!/usr/bin/env python3
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

import os
from decimal import Decimal

from nautilus_trader.adapters.lighter import LIGHTER
from nautilus_trader.adapters.lighter import LighterDataClientConfig
from nautilus_trader.adapters.lighter import LighterExecClientConfig
from nautilus_trader.adapters.lighter import LighterLiveDataClientFactory
from nautilus_trader.adapters.lighter import LighterLiveExecClientFactory
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.test_kit.strategies.tester_exec import ExecTester
from nautilus_trader.test_kit.strategies.tester_exec import ExecTesterConfig


# *** THIS IS A TEST STRATEGY WITH NO ALPHA ADVANTAGE WHATSOEVER. ***
# *** IT IS NOT INTENDED TO BE USED TO TRADE LIVE WITH REAL MONEY. ***


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable {name}")
    return value


testnet = os.environ.get("LIGHTER_TESTNET", "0") == "1"
account_index = int(_required_env("LIGHTER_ACCOUNT_INDEX"))
api_key_index = int(_required_env("LIGHTER_API_KEY_INDEX"))
private_key = _required_env("LIGHTER_PRIVATE_KEY")
signer_lib_path = os.environ.get("LIGHTER_SIGNER_LIB_PATH")

instrument_id = InstrumentId.from_str("BTC-USDC-PERP.LIGHTER")

config_node = TradingNodeConfig(
    trader_id=TraderId("TESTER-001"),
    logging=LoggingConfig(
        log_level="INFO",
        use_pyo3=True,
    ),
    exec_engine=LiveExecEngineConfig(
        reconciliation=True,
        reconciliation_lookback_mins=1440,
        open_check_interval_secs=15.0,
        open_check_threshold_ms=10_000,
        open_check_open_only=False,
        open_check_lookback_mins=60,
        purge_closed_orders_interval_mins=15,
        purge_closed_orders_buffer_mins=60,
        purge_closed_positions_interval_mins=15,
        purge_closed_positions_buffer_mins=60,
        purge_account_events_interval_mins=15,
        purge_account_events_lookback_mins=60,
        graceful_shutdown_on_exception=True,
    ),
    data_clients={
        LIGHTER: LighterDataClientConfig(
            instrument_provider=InstrumentProviderConfig(load_all=True),
            testnet=testnet,
        ),
    },
    exec_clients={
        LIGHTER: LighterExecClientConfig(
            account_index=account_index,
            private_key=private_key,
            api_key_index=api_key_index,
            signer_lib_path=signer_lib_path,
            instrument_provider=InstrumentProviderConfig(load_all=True),
            testnet=testnet,
        ),
    },
    timeout_connection=30.0,
    timeout_reconciliation=10.0,
    timeout_portfolio=10.0,
    timeout_disconnection=10.0,
    timeout_post_stop=10.0,
)

node = TradingNode(config=config_node)

strategy = ExecTester(
    config=ExecTesterConfig(
        instrument_id=instrument_id,
        external_order_claims=[instrument_id],
        order_qty=Decimal("0.001"),
        open_position_on_start_qty=Decimal("0.001"),
        open_position_time_in_force=TimeInForce.IOC,
        enable_limit_buys=True,
        enable_limit_sells=True,
        use_post_only=True,
        manage_stop=True,
        reduce_only_on_stop=True,
        market_exit_reduce_only=True,
        log_data=False,
    ),
)

node.trader.add_strategy(strategy)
node.add_data_client_factory(LIGHTER, LighterLiveDataClientFactory)
node.add_exec_client_factory(LIGHTER, LighterLiveExecClientFactory)
node.build()


if __name__ == "__main__":
    try:
        node.run()
    finally:
        node.dispose()
