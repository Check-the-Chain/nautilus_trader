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

from nautilus_trader.adapters.alpaca import ALPACA
from nautilus_trader.adapters.alpaca import AlpacaDataClientConfig
from nautilus_trader.adapters.alpaca import AlpacaLiveDataClientFactory
from nautilus_trader.config import InstrumentProviderConfig
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


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable {name}")
    return value


paper = os.environ.get("ALPACA_PAPER", "1") == "1"
api_key = _required_env("ALPACA_API_KEY")
api_secret = _required_env("ALPACA_API_SECRET")

instrument_ids = [
    InstrumentId.from_str("AAPL.ALPACA"),
    InstrumentId.from_str("BTC/USD.ALPACA"),
]

bar_types = [
    BarType.from_str("AAPL.ALPACA-1-MINUTE-LAST-EXTERNAL"),
]

config_node = TradingNodeConfig(
    trader_id=TraderId("TESTER-001"),
    logging=LoggingConfig(
        log_level="INFO",
        use_pyo3=True,
    ),
    data_clients={
        ALPACA: AlpacaDataClientConfig(
            api_key=api_key,
            api_secret=api_secret,
            paper=paper,
            instrument_provider=InstrumentProviderConfig(
                load_ids=frozenset(instrument_ids),
            ),
        ),
    },
    timeout_connection=20.0,
    timeout_disconnection=10.0,
    timeout_post_stop=2.0,
)

node = TradingNode(config=config_node)

strategy = DataTester(
    config=DataTesterConfig(
        instrument_ids=instrument_ids,
        bar_types=bar_types,
        subscribe_quotes=True,
        subscribe_trades=True,
        subscribe_bars=False,
        request_bars=True,
    ),
)

node.trader.add_actor(strategy)
node.add_data_client_factory(ALPACA, AlpacaLiveDataClientFactory)
node.build()


if __name__ == "__main__":
    try:
        node.run()
    finally:
        node.dispose()
