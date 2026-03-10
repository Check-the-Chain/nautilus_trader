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

from pathlib import Path
import subprocess
import sys
import textwrap

from nautilus_trader.backtest.native import BacktestRunConfig
from nautilus_trader.backtest.native import BacktestVenueConfig
from nautilus_trader.execution.native import ExecutionAlgorithm
from nautilus_trader.execution.native import ExecutionAlgorithmConfig
from nautilus_trader.execution.native import LimitChaserAlgorithm
from nautilus_trader.execution.native import LimitChaserAlgorithmConfig
from nautilus_trader.execution.native import TwapAlgorithm
from nautilus_trader.model import AccountType
from nautilus_trader.model import BookType
from nautilus_trader.model import OmsType


def _make_run_config() -> BacktestRunConfig:
    venue = BacktestVenueConfig(
        name="SIM",
        oms_type=OmsType.HEDGING,
        account_type=AccountType.MARGIN,
        book_type=BookType.L1_MBP,
        starting_balances=["1_000_000 USD"],
    )
    return BacktestRunConfig(
        id="native-exec-algos",
        venues=[venue],
        data=[],
    )


def test_native_execution_algorithms_construct():
    twap = TwapAlgorithm(ExecutionAlgorithmConfig())
    limit_chaser = LimitChaserAlgorithm(
        LimitChaserAlgorithmConfig(
            follow_offset_ticks=1,
            aggressive_offset_ticks=1,
            reprice_interval_ms=100,
        ),
    )

    assert isinstance(twap, ExecutionAlgorithm)
    assert isinstance(limit_chaser, ExecutionAlgorithm)
    assert twap.id is not None
    assert limit_chaser.id is not None


def test_backtest_node_add_native_exec_algorithms_smoke():
    script = textwrap.dedent("""
        from nautilus_trader.backtest.native import BacktestNode, BacktestRunConfig, BacktestVenueConfig
        from nautilus_trader.execution.native import ExecutionAlgorithmConfig, ImportableExecAlgorithmConfig
        from nautilus_trader.execution.native import LimitChaserAlgorithm, LimitChaserAlgorithmConfig
        from nautilus_trader.model import AccountType, BookType, OmsType

        venue = BacktestVenueConfig(
            name="SIM",
            oms_type=OmsType.HEDGING,
            account_type=AccountType.MARGIN,
            book_type=BookType.L1_MBP,
            starting_balances=["1_000_000 USD"],
        )
        config = BacktestRunConfig(
            id="native-exec-algos",
            venues=[venue],
            data=[],
        )
        node = BacktestNode([config])
        node.build()
        node.add_exec_algorithm(
            config.id,
            LimitChaserAlgorithm(LimitChaserAlgorithmConfig(follow_offset_ticks=1)),
        )
        node.add_exec_algorithm_from_config(
            config.id,
            ImportableExecAlgorithmConfig(
                exec_algorithm_path="nautilus_trader.execution.native:TwapAlgorithm",
                config_path="nautilus_trader.execution.native:ExecutionAlgorithmConfig",
                config={},
            ),
        )
        print("ok")
    """)

    result = subprocess.run(
        [sys.executable, "-u", "-c", script],
        capture_output=True,
        check=False,
        cwd=Path(__file__).resolve().parents[3],
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "ok" in result.stdout
