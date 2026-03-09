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
from decimal import Decimal
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.lighter.constants import LIGHTER_VENUE
from nautilus_trader.adapters.lighter.providers import LighterInstrumentProvider
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import Logger
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.events import AccountState
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


def _market_metadata_payload() -> str:
    return json.dumps(
        {
            "assets": [
                {"asset_id": 1, "symbol": "BTC"},
                {"asset_id": 2, "symbol": "USDC"},
                {"asset_id": 3, "symbol": "ETH"},
            ],
            "details": [
                {
                    "market_id": 1,
                    "symbol": "BTC-USDC",
                    "base_asset_id": 1,
                    "quote_asset_id": 2,
                    "market_type": "perp",
                    "price_decimals": 2,
                    "size_decimals": 4,
                    "min_base_amount": "0.0001",
                    "maker_fee": "0.0002",
                    "taker_fee": "0.0005",
                    "default_initial_margin_fraction": 500,
                    "maintenance_margin_fraction": 250,
                },
                {
                    "market_id": 2048,
                    "symbol": "ETH-USDC",
                    "base_asset_id": 3,
                    "quote_asset_id": 2,
                    "market_type": "spot",
                    "price_decimals": 2,
                    "size_decimals": 4,
                    "min_base_amount": "0.001",
                    "maker_fee": "0.0001",
                    "taker_fee": "0.0004",
                },
            ],
        },
    )


@pytest.fixture(scope="session")
def live_clock():
    return LiveClock()


@pytest.fixture(scope="session")
def live_logger():
    return Logger("TEST_LOGGER")


@pytest.fixture
def venue() -> Venue:
    return LIGHTER_VENUE


@pytest.fixture
def account_id(venue) -> AccountId:
    return AccountId(f"{venue.value}-7")


@pytest.fixture
def instrument() -> CryptoPerpetual:
    usdc = Currency.from_str("USDC")
    return CryptoPerpetual(
        instrument_id=InstrumentId(Symbol("BTC-USDC-PERP"), LIGHTER_VENUE),
        raw_symbol=Symbol("BTC-USDC"),
        base_currency=Currency.from_str("BTC"),
        quote_currency=usdc,
        settlement_currency=usdc,
        is_inverse=False,
        price_precision=2,
        size_precision=4,
        price_increment=Price.from_str("0.01"),
        size_increment=Quantity.from_str("0.0001"),
        lot_size=Quantity.from_str("0.0001"),
        max_quantity=None,
        min_quantity=Quantity.from_str("0.0001"),
        max_notional=None,
        min_notional=None,
        max_price=None,
        min_price=None,
        margin_init=Decimal("0.05"),
        margin_maint=Decimal("0.025"),
        maker_fee=Decimal("0.0002"),
        taker_fee=Decimal("0.0005"),
        ts_event=0,
        ts_init=0,
        info={
            "market_id": 1,
            "market_type": "perp",
            "raw_symbol": "BTC-USDC",
            "price_decimals": 2,
            "size_decimals": 4,
        },
    )


@pytest.fixture
def spot_instrument() -> CurrencyPair:
    usdc = Currency.from_str("USDC")
    return CurrencyPair(
        instrument_id=InstrumentId(Symbol("ETH-USDC-SPOT"), LIGHTER_VENUE),
        raw_symbol=Symbol("ETH-USDC"),
        base_currency=Currency.from_str("ETH"),
        quote_currency=usdc,
        price_precision=2,
        size_precision=4,
        price_increment=Price.from_str("0.01"),
        size_increment=Quantity.from_str("0.0001"),
        lot_size=Quantity.from_str("0.0001"),
        max_quantity=None,
        min_quantity=Quantity.from_str("0.0010"),
        max_notional=None,
        min_notional=None,
        max_price=None,
        min_price=None,
        margin_init=Decimal(0),
        margin_maint=Decimal(0),
        maker_fee=Decimal("0.0001"),
        taker_fee=Decimal("0.0004"),
        ts_event=0,
        ts_init=0,
        info={
            "market_id": 2048,
            "market_type": "spot",
            "raw_symbol": "ETH-USDC",
            "price_decimals": 2,
            "size_decimals": 4,
        },
    )


@pytest.fixture
def account_state(account_id) -> AccountState:
    usdc = Currency.from_str("USDC")
    return AccountState(
        account_id=account_id,
        account_type=AccountType.MARGIN,
        base_currency=usdc,
        reported=True,
        balances=[
            AccountBalance(
                total=Money(100_000, usdc),
                locked=Money(0, usdc),
                free=Money(100_000, usdc),
            ),
        ],
        margins=[],
        info={},
        event_id=TestIdStubs.uuid(),
        ts_event=0,
        ts_init=0,
    )


@pytest.fixture
def mock_http_client():
    mock = MagicMock(spec=nautilus_pyo3.LighterHttpClient)

    mock.load_market_metadata = AsyncMock(return_value=_market_metadata_payload())
    mock.request_order_book_snapshot = AsyncMock(
        return_value=json.dumps(
            {
                "bids": [{"price": "100000.00", "size": "1.2000"}],
                "asks": [{"price": "100001.00", "size": "1.3000"}],
                "offset": 42,
                "total_bids": 1,
                "total_asks": 1,
            },
        ),
    )
    mock.request_recent_trades = AsyncMock(
        return_value=json.dumps(
            {
                "trades": [
                    {
                        "trade_id": "trade-1",
                        "price": "100000.00",
                        "size": "0.1000",
                        "timestamp": 1704067200000,
                        "is_maker_ask": False,
                    },
                    {
                        "trade_id": "trade-2",
                        "price": "100010.00",
                        "size": "0.2000",
                        "timestamp": 1704067260000,
                        "is_maker_ask": True,
                    },
                ],
            },
        ),
    )
    mock.request_candles = AsyncMock(
        return_value=json.dumps(
            {
                "candles": [
                    {
                        "timestamp": 1704067200000,
                        "open": "100000.00",
                        "high": "100020.00",
                        "low": "99990.00",
                        "close": "100010.00",
                        "volume": "12.3456",
                    },
                    {
                        "timestamp": 1704067260000,
                        "open": "100010.00",
                        "high": "100030.00",
                        "low": "100000.00",
                        "close": "100025.00",
                        "volume": "10.0000",
                    },
                ],
            },
        ),
    )
    mock.request_funding_rates = AsyncMock(
        return_value=json.dumps(
            {
                "funding_rates": [
                    {
                        "funding_rate": "0.0001",
                        "settlement_time": 1704067200000,
                        "index_price": "100000.00",
                        "mark_price": "100002.00",
                    },
                    {
                        "funding_rate": "0.0002",
                        "settlement_time": 1704067800000,
                        "index_price": "100100.00",
                        "mark_price": "100101.00",
                    },
                ],
            },
        ),
    )
    mock.request_announcements = AsyncMock(
        return_value=json.dumps({"code": 200, "announcements": [{"title": "listing"}]}),
    )
    mock.request_status = AsyncMock(
        return_value=json.dumps({"status": 1, "network_id": 1, "timestamp": 1704067200}),
    )
    mock.request_system_config = AsyncMock(
        return_value=json.dumps({"code": 200, "liquidity_pool_index": 1}),
    )
    mock.request_exchange_metrics = AsyncMock(
        return_value=json.dumps({"code": 200, "metrics": [{"value": "123.4"}]}),
    )
    mock.request_execute_stats = AsyncMock(
        return_value=json.dumps({"code": 200, "period": "d", "result": {"success": 99.9}}),
    )
    mock.request_layer1_basic_info = AsyncMock(
        return_value=json.dumps({"code": 200, "validator_info": {"status": "ok"}}),
    )
    mock.request_zk_lighter_info = AsyncMock(
        return_value=json.dumps({"contract_address": "0xcontract"}),
    )
    mock.create_auth_token = AsyncMock(return_value="lighter-auth-token")
    mock.request_account = AsyncMock(
        return_value=json.dumps(
            {
                "accounts": [
                    {
                        "assets": [
                            {
                                "symbol": "USDC",
                                "balance": "100000",
                                "locked_balance": "10",
                            },
                        ],
                        "positions": [
                            {
                                "market_id": 1,
                                "position": "0.5",
                                "sign": 1,
                                "allocated_margin": "1000",
                                "maintenance_margin": "500",
                                "avg_entry_price": "100000.00",
                            },
                        ],
                    },
                ],
            },
        ),
    )
    mock.request_account_api_keys = AsyncMock(
        return_value=json.dumps({"code": 200, "api_keys": [{"api_key_index": 3}]}),
    )
    mock.request_account_limits = AsyncMock(
        return_value=json.dumps({"code": 200, "user_tier": 1}),
    )
    mock.request_account_metadata = AsyncMock(
        return_value=json.dumps(
            {
                "code": 200,
                "account_metadatas": [
                    {"account_index": 7, "name": "main", "description": "Primary trading account"},
                ],
            },
        ),
    )
    mock.request_l1_metadata = AsyncMock(
        return_value=json.dumps(
            {
                "code": 200,
                "l1_address": "0xabc",
                "nickname": "primary",
            },
        ),
    )
    mock.request_sub_accounts = AsyncMock(
        return_value=json.dumps(
            {
                "code": 200,
                "l1_address": "0xabc",
                "sub_accounts": [{"account_index": 7}],
            },
        ),
    )
    mock.request_public_pools_metadata = AsyncMock(
        return_value=json.dumps(
            {
                "code": 200,
                "pools": [
                    {
                        "public_pool_index": 11,
                        "account_index": 7,
                        "info": {"operator_fee": "10"},
                    },
                ],
            },
        ),
    )
    mock.request_account_pnl = AsyncMock(return_value=json.dumps({"code": 200, "pnl": []}))
    mock.request_liquidations = AsyncMock(
        return_value=json.dumps(
            {
                "code": 200,
                "liquidations": [{"id": 1, "market_id": 1, "type": "partial"}],
                "next_cursor": "cursor-2",
            },
        ),
    )
    mock.request_position_fundings = AsyncMock(
        return_value=json.dumps({"code": 200, "fundings": []}),
    )
    mock.request_deposit_history = AsyncMock(
        return_value=json.dumps({"code": 200, "deposits": []}),
    )
    mock.request_withdraw_history = AsyncMock(
        return_value=json.dumps({"code": 200, "withdraws": []}),
    )
    mock.request_transfer_history = AsyncMock(
        return_value=json.dumps({"code": 200, "transfers": []}),
    )
    mock.request_next_nonce = AsyncMock(return_value=json.dumps({"code": 200, "nonce": 12345}))
    mock.request_enriched_tx = AsyncMock(return_value=json.dumps({"code": 200, "tx_hash": "0xabc"}))
    mock.request_tx_from_l1_tx_hash = AsyncMock(
        return_value=json.dumps({"code": 200, "hash": "0xl1"}),
    )
    mock.request_txs = AsyncMock(
        return_value=json.dumps({"code": 200, "txs": [{"hash": "0xabc"}]}),
    )
    mock.request_export = AsyncMock(
        return_value=json.dumps({"code": 200, "data_url": "https://example.com/export.csv"}),
    )
    mock.request_transfer_fee_info = AsyncMock(
        return_value=json.dumps({"code": 200, "transfer_fee_usdc": 15}),
    )
    mock.request_withdrawal_delay = AsyncMock(
        return_value=json.dumps({"seconds": 86400}),
    )
    mock.create_intent_address = AsyncMock(
        return_value=json.dumps({"code": 200, "intent_address": "0xintent"}),
    )
    mock.request_fast_bridge_info = AsyncMock(
        return_value=json.dumps({"code": 200, "fast_bridge_limit": "50000"}),
    )
    mock.request_deposit_latest = AsyncMock(
        return_value=json.dumps({"code": 200, "l1_address": "0xabc", "status": "settled"}),
    )
    mock.request_deposit_networks = AsyncMock(
        return_value=json.dumps({"code": 200, "networks": [{"chain_id": 1, "name": "Ethereum"}]}),
    )
    mock.request_fast_withdraw_info = AsyncMock(
        return_value=json.dumps(
            {
                "code": 200,
                "to_account_index": 17,
                "withdraw_limit": "1000",
                "max_withdrawal_amount": "800",
            },
        ),
    )
    mock.request_lease_options = AsyncMock(
        return_value=json.dumps(
            {
                "code": 200,
                "options": [{"duration_days": 30, "apr_bps": 100}],
                "lit_incentives_account_index": 99,
            },
        ),
    )
    mock.request_leases = AsyncMock(
        return_value=json.dumps({"code": 200, "leases": [{"lease_id": 1}], "next_cursor": None}),
    )
    mock.request_api_tokens = AsyncMock(
        return_value=json.dumps(
            {
                "code": 200,
                "tokens": [{"token_id": 11, "name": "reporting"}],
            },
        ),
    )
    mock.request_user_referrals = AsyncMock(
        return_value=json.dumps(
            {
                "code": 200,
                "cursor": 2,
                "referrals": [
                    {
                        "l1_address": "0xabc",
                        "referral_code": "LIGHTER7",
                        "used_at": 1704067200000,
                    },
                ],
            },
        ),
    )
    mock.request_referral_code = AsyncMock(
        return_value=json.dumps(
            {
                "code": 200,
                "referral_code": "LIGHTER7",
                "remaining_usage": 3,
            },
        ),
    )
    mock.create_referral_code = AsyncMock(
        return_value=json.dumps(
            {
                "code": 200,
                "referral_code": "LIGHTER7",
                "remaining_usage": 3,
            },
        ),
    )
    mock.update_referral_code = AsyncMock(return_value=json.dumps({"code": 200, "success": True}))
    mock.update_referral_kickback = AsyncMock(
        return_value=json.dumps({"code": 200, "success": True}),
    )
    mock.use_referral_code = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.create_api_token = AsyncMock(
        return_value=json.dumps(
            {
                "code": 200,
                "token_id": 11,
                "api_token": "ro:7:all:1767139200:deadbeef",
            },
        ),
    )
    mock.revoke_api_token = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.request_account_active_orders = AsyncMock(
        return_value=json.dumps(
            {
                "orders": [
                    {
                        "order_index": 101,
                        "status": "open",
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
                ],
            },
        ),
    )
    mock.request_account_inactive_orders = AsyncMock(return_value=json.dumps({"orders": []}))
    mock.request_account_trades = AsyncMock(
        return_value=json.dumps(
            {
                "trades": [
                    {
                        "trade_id": "fill-1",
                        "market_id": 1,
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
                ],
            },
        ),
    )
    mock.submit_order = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.submit_order_batch = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.modify_order = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.cancel_order = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.cancel_order_batch = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.cancel_all_orders = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.update_leverage = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.update_margin = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.change_account_tier = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.acknowledge_notification = AsyncMock(
        return_value=json.dumps({"code": 200, "message": "ok"}),
    )
    mock.fast_withdraw = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.change_pub_key = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.create_sub_account = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.lit_lease = AsyncMock(return_value=json.dumps({"code": 200, "tx_hash": "0xlease"}))
    mock.create_public_pool = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.update_public_pool = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.mint_pool_shares = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.burn_pool_shares = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.withdraw = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))
    mock.transfer = AsyncMock(return_value=json.dumps({"code": 200, "message": "ok"}))

    return mock


def _create_ws_mock() -> MagicMock:
    mock = MagicMock(spec=nautilus_pyo3.LighterWebSocketClient)
    mock.url = "wss://mainnet.zklighter.elliot.ai/stream"
    mock.is_closed = MagicMock(return_value=False)
    mock.is_active = MagicMock(return_value=True)
    mock.connect = AsyncMock()
    mock.close = AsyncMock()
    mock.set_auth_token = AsyncMock()
    mock.subscribe_book = AsyncMock()
    mock.unsubscribe_book = AsyncMock()
    mock.subscribe_quotes = AsyncMock()
    mock.unsubscribe_quotes = AsyncMock()
    mock.subscribe_trades = AsyncMock()
    mock.unsubscribe_trades = AsyncMock()
    mock.subscribe_market_stats = AsyncMock()
    mock.unsubscribe_market_stats = AsyncMock()
    mock.subscribe_account_all = AsyncMock()
    mock.subscribe_account_all_orders = AsyncMock()
    mock.subscribe_account_all_positions = AsyncMock()
    mock.subscribe_account_all_trades = AsyncMock()
    mock.subscribe_account_all_assets = AsyncMock()
    mock.subscribe_user_stats = AsyncMock()
    return mock


@pytest.fixture
def mock_ws_client():
    return _create_ws_mock()


@pytest.fixture
def mock_instrument_provider(instrument, spot_instrument):
    instruments = {
        instrument.id: instrument,
        spot_instrument.id: spot_instrument,
    }
    market_to_instrument = {
        1: instrument,
        2048: spot_instrument,
    }
    metadata_by_instrument_id = {
        instrument.id: instrument.info,
        spot_instrument.id: spot_instrument.info,
    }

    provider = MagicMock(spec=LighterInstrumentProvider)
    provider.initialize = AsyncMock()
    provider.load_all_async = AsyncMock()
    provider.load_ids_async = AsyncMock()
    provider.load_async = AsyncMock()
    provider.get_all = MagicMock(return_value=instruments)
    provider.currencies = MagicMock(return_value={})
    provider.find = MagicMock(side_effect=lambda instrument_id: instruments.get(instrument_id))
    provider.list_all = MagicMock(return_value=list(instruments.values()))
    provider.market_ids = MagicMock(return_value=[1, 2048])
    provider.market_id_for_instrument = MagicMock(
        side_effect=lambda instrument_id: (
            metadata_by_instrument_id[instrument_id]["market_id"]
            if instrument_id in metadata_by_instrument_id
            else None
        ),
    )
    provider.instrument_for_market_id = MagicMock(
        side_effect=lambda market_id: market_to_instrument.get(market_id),
    )
    provider.metadata_for_instrument = MagicMock(
        side_effect=lambda instrument_id: metadata_by_instrument_id.get(instrument_id),
    )
    provider.instrument_metadata = MagicMock(
        side_effect=lambda instrument_id: metadata_by_instrument_id.get(instrument_id),
    )
    provider.metadata_for_market_id = MagicMock(
        side_effect=lambda market_id: (
            market_to_instrument[market_id].info if market_id in market_to_instrument else None
        ),
    )
    return provider


@pytest.fixture
def data_client():
    return None


@pytest.fixture
def exec_client():
    return None
