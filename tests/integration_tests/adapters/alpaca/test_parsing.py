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

from decimal import Decimal

import pytest

from nautilus_trader.adapters.alpaca.common import account_balance_from_account
from nautilus_trader.adapters.alpaca.common import account_type_from_account
from nautilus_trader.adapters.alpaca.common import asset_to_instrument
from nautilus_trader.adapters.alpaca.common import bar_type_to_timeframe
from nautilus_trader.adapters.alpaca.common import extract_items_for_symbol
from nautilus_trader.adapters.alpaca.common import order_to_report
from nautilus_trader.adapters.alpaca.common import position_to_report
from nautilus_trader.adapters.alpaca.common import symbol_to_instrument_id
from nautilus_trader.model.data import BarSpecification
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import ContingencyType
from nautilus_trader.model.enums import OptionKind
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.enums import TrailingOffsetType
from tests.integration_tests.adapters.alpaca.conftest import make_alpaca_order
from tests.integration_tests.adapters.alpaca.conftest import make_option_contract_asset


def test_symbol_to_instrument_id_normalizes_crypto_symbols():
    assert symbol_to_instrument_id("BTCUSD").value == "BTC/USD.ALPACA"


def test_asset_to_instrument_stores_data_and_trade_symbols():
    instrument = asset_to_instrument(
        {
            "symbol": "BTCUSD",
            "asset_class": "crypto",
            "status": "active",
            "tradable": True,
            "min_order_size": "0.0001",
            "min_trade_increment": "0.0001",
            "price_increment": "0.01",
        },
    )

    assert instrument is not None
    assert instrument.id.value == "BTC/USD.ALPACA"
    assert instrument.info["data_symbol"] == "BTC/USD"
    assert instrument.info["trade_symbol"] == "BTCUSD"


def test_asset_to_instrument_parses_option_contract():
    instrument = asset_to_instrument(
        make_option_contract_asset(
            symbol="AAPL260320P00145000",
            option_type="put",
            strike_price="145",
        ),
    )

    assert instrument is not None
    assert instrument.id.value == "AAPL260320P00145000.ALPACA"
    assert instrument.underlying == "AAPL"
    assert instrument.option_kind == OptionKind.PUT
    assert str(instrument.strike_price) == "145.00"
    assert str(instrument.multiplier) == "100"
    assert str(instrument.lot_size) == "1"
    assert instrument.info["style"] == "american"


def test_extract_items_for_symbol_handles_slashless_payload_keys():
    payload = {"trades": {"BTCUSD": [{"id": "trade-1"}]}}

    items = extract_items_for_symbol(payload, "trades", "BTC/USD")

    assert items == [{"id": "trade-1"}]


def test_account_type_from_account_detects_margin():
    account_type = account_type_from_account({"multiplier": "2"})

    assert account_type == AccountType.MARGIN


def test_account_balance_from_account_uses_cash_field():
    balance = account_balance_from_account({"cash": "1234.56"})

    assert str(balance.total) == "1234.56 USD"
    assert str(balance.free) == "1234.56 USD"


def test_bar_type_to_timeframe_supports_hour_bars():
    bar_type = BarType(
        symbol_to_instrument_id("AAPL"),
        BarSpecification(1, BarAggregation.HOUR, PriceType.LAST),
        AggregationSource.EXTERNAL,
    )

    assert bar_type_to_timeframe(bar_type) == "1Hour"


def test_bar_type_to_timeframe_rejects_non_last_price_type():
    bar_type = BarType(
        symbol_to_instrument_id("AAPL"),
        BarSpecification(1, BarAggregation.MINUTE, PriceType.BID),
        AggregationSource.EXTERNAL,
    )

    with pytest.raises(ValueError, match="Only LAST price bars are supported"):
        bar_type_to_timeframe(bar_type)


def test_order_to_report_maps_stop_limit_and_contingency(account_id, equity_instrument):
    order = make_alpaca_order(
        symbol="AAPL",
        type_="stop_limit",
        qty="10",
        limit_price="149.50",
        stop_price="149.75",
    )
    order["order_class"] = "oco"
    order["expires_at"] = "2026-03-10T10:00:00Z"

    report = order_to_report(account_id, equity_instrument, order)

    assert report.order_type == OrderType.STOP_LIMIT
    assert report.contingency_type == ContingencyType.OCO
    assert str(report.trigger_price) == "149.75"
    assert report.expire_time is not None


def test_order_to_report_maps_trailing_percent_to_basis_points(account_id, equity_instrument):
    order = make_alpaca_order(
        symbol="AAPL",
        type_="trailing_stop",
        qty="10",
        filled_qty="0",
        limit_price=None,
    )
    order["time_in_force"] = "gtc"
    order["trail_percent"] = "1.25"

    report = order_to_report(account_id, equity_instrument, order)

    assert report.trailing_offset == Decimal(125)
    assert report.trailing_offset_type == TrailingOffsetType.BASIS_POINTS


def test_position_to_report_maps_short_side_and_abs_quantity(account_id, crypto_instrument):
    report = position_to_report(
        account_id,
        crypto_instrument,
        {
            "asset_id": "asset-1",
            "side": "short",
            "qty": "-0.5000",
            "avg_entry_price": "50000.00",
            "updated_at": "2026-03-09T10:00:00Z",
        },
    )

    assert report.position_side == PositionSide.SHORT
    assert str(report.quantity) == "0.5000"
