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

from decimal import Decimal

from nautilus_trader.adapters.lighter.constants import LIGHTER_FEE_SCALE
from nautilus_trader.adapters.lighter.parsing import fill_report_from_lighter_trade
from nautilus_trader.adapters.lighter.parsing import market_id_from_channel
from nautilus_trader.adapters.lighter.parsing import market_stats_to_updates
from nautilus_trader.adapters.lighter.parsing import normalize_market_type
from nautilus_trader.adapters.lighter.parsing import order_report_from_lighter
from nautilus_trader.adapters.lighter.parsing import position_report_from_lighter
from nautilus_trader.model.identifiers import ClientOrderId


def test_normalize_market_type_detects_spot_by_market_id():
    assert normalize_market_type({"market_id": 2048}) == "spot"
    assert normalize_market_type({"market_id": 1}) == "perp"


def test_market_id_from_channel_supports_slash_and_colon_formats():
    assert market_id_from_channel("order_book/2048") == 2048
    assert market_id_from_channel("order_book:2048") == 2048
    assert market_id_from_channel("market_stats:all") is None


def test_order_report_from_lighter_computes_avg_price_and_gtd(account_id, instrument):
    report = order_report_from_lighter(
        {
            "order_index": 101,
            "status": "partially_filled",
            "type": 0,
            "time_in_force": "gtt",
            "client_order_index": 777,
            "price": "100000.00",
            "trigger_price": "0",
            "created_at": 1704067200000,
            "updated_at": 1704067260000,
            "is_ask": False,
            "initial_base_amount": "0.5000",
            "filled_base_amount": "0.1000",
            "filled_quote_amount": "10000.00",
            "order_expiry": 1704153600000,
            "reduce_only": False,
        },
        account_id,
        instrument,
        lambda value: ClientOrderId(f"O-{value}"),
    )

    assert report.client_order_id == ClientOrderId("O-777")
    assert report.avg_px == Decimal(100000)
    assert report.time_in_force.name == "GTD"
    assert report.order_status.name == "PARTIALLY_FILLED"


def test_fill_report_from_lighter_trade_scales_fees(account_id, instrument):
    report = fill_report_from_lighter_trade(
        {
            "trade_id": "fill-1",
            "ask_account_id": 7,
            "bid_account_id": 8,
            "ask_client_id": 777,
            "bid_client_id": 0,
            "ask_id": 101,
            "bid_id": 202,
            "size": "0.1000",
            "price": "100010.00",
            "timestamp": 1704067260000,
            "is_maker_ask": True,
            "maker_fee": "100",
            "taker_fee": "200",
            "position_id": "5001",
        },
        7,
        account_id,
        instrument,
        lambda value: ClientOrderId(f"O-{value}"),
    )

    assert report is not None
    assert report.client_order_id == ClientOrderId("O-777")
    assert report.venue_order_id.value == "101"
    assert report.commission.as_decimal() == Decimal(100) / LIGHTER_FEE_SCALE
    assert report.liquidity_side.name == "MAKER"


def test_position_report_from_lighter_maps_short_position(account_id, instrument):
    report = position_report_from_lighter(
        {
            "position": "0.2500",
            "sign": -1,
            "avg_entry_price": "99999.00",
        },
        account_id,
        instrument,
        ts_init=1704067260000000000,
    )

    assert report.position_side.name == "SHORT"
    assert str(report.quantity) == "0.2500"
    assert report.avg_px_open == Decimal("99999.00")


def test_market_stats_to_updates_emits_mark_index_and_funding(instrument):
    updates = market_stats_to_updates(
        instrument,
        {
            "mark_price": "100001.00",
            "index_price": "100000.00",
            "current_funding_rate": "0.0001",
            "next_funding_time": 1704067800000,
        },
        ts_event=1704067260000000000,
        ts_init=1704067260000000000,
    )

    assert len(updates) == 3
    assert updates[0].__class__.__name__ == "MarkPriceUpdate"
    assert updates[1].__class__.__name__ == "IndexPriceUpdate"
    assert updates[2].__class__.__name__ == "FundingRateUpdate"
    assert updates[2].next_funding_ns == 1704067800000000000
