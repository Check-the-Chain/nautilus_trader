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

from nautilus_trader.adapters.lighter import LIGHTER
from nautilus_trader.adapters.lighter import LighterDataClientConfig
from nautilus_trader.adapters.lighter import LighterLiveDataClientFactory
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.test_kit.strategies.tester_data import DataTester
from nautilus_trader.test_kit.strategies.tester_data import DataTesterConfig


# *** THIS IS A TEST STRATEGY WITH NO ALPHA ADVANTAGE WHATSOEVER. ***
# *** IT IS NOT INTENDED TO BE USED TO TRADE LIVE WITH REAL MONEY. ***

# Adjust these symbols to match the markets available on the target Lighter environment.
instrument_ids = [
    InstrumentId.from_str("BTC-USDC-PERP.LIGHTER"),
    InstrumentId.from_str("ETH-USDC-SPOT.LIGHTER"),
]

bar_types = [
    BarType.from_str("BTC-USDC-PERP.LIGHTER-1-MINUTE-LAST-EXTERNAL"),
]


if __name__ == "__main__":
    config_node = TradingNodeConfig(
        trader_id=TraderId("TESTER-001"),
        logging=LoggingConfig(
            log_level="INFO",
            use_pyo3=True,
        ),
        exec_engine=LiveExecEngineConfig(
            reconciliation=False,
        ),
        data_clients={
            LIGHTER: LighterDataClientConfig(
                instrument_provider=InstrumentProviderConfig(load_all=True),
                testnet=False,
            ),
        },
        timeout_connection=20.0,
        timeout_reconciliation=10.0,
        timeout_portfolio=10.0,
        timeout_disconnection=10.0,
        timeout_post_stop=2.0,
    )

    node = TradingNode(config=config_node)

    config_strat = DataTesterConfig(
        instrument_ids=instrument_ids,
        bar_types=bar_types,
        subscribe_book=True,
        subscribe_quotes=True,
        subscribe_trades=True,
        subscribe_mark_prices=True,
        subscribe_index_prices=True,
        subscribe_funding_rates=True,
        subscribe_bars=False,
        request_bars=True,
    )
    strategy = DataTester(config=config_strat)

    node.trader.add_actor(strategy)
    node.add_data_client_factory(LIGHTER, LighterLiveDataClientFactory)
    node.build()

    try:
        node.run()
    finally:
        node.dispose()
