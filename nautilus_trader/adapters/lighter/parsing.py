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
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from decimal import ROUND_HALF_UP
from decimal import Decimal
from typing import Any

from nautilus_trader.adapters.lighter.constants import LIGHTER_FEE_SCALE
from nautilus_trader.adapters.lighter.constants import LIGHTER_LIMIT_ORDER
from nautilus_trader.adapters.lighter.constants import LIGHTER_MARKET_ORDER
from nautilus_trader.adapters.lighter.constants import LIGHTER_MARKET_TYPE_SPOT
from nautilus_trader.adapters.lighter.constants import LIGHTER_STOP_LOSS_LIMIT_ORDER
from nautilus_trader.adapters.lighter.constants import LIGHTER_STOP_LOSS_ORDER
from nautilus_trader.adapters.lighter.constants import LIGHTER_TAKE_PROFIT_LIMIT_ORDER
from nautilus_trader.adapters.lighter.constants import LIGHTER_TAKE_PROFIT_ORDER
from nautilus_trader.adapters.lighter.constants import LIGHTER_TIF_IOC
from nautilus_trader.adapters.lighter.constants import LIGHTER_TIF_POST_ONLY
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BookOrder
from nautilus_trader.model.data import FundingRateUpdate
from nautilus_trader.model.data import IndexPriceUpdate
from nautilus_trader.model.data import MarkPriceUpdate
from nautilus_trader.model.data import OrderBookDelta
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import BookAction
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import TriggerType
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import MarginBalance
from nautilus_trader.model.objects import Money


def loads(payload: str | bytes | bytearray | dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, (bytes, bytearray)):
        return json.loads(payload.decode("utf-8"))
    return json.loads(payload)


def epoch_to_nanos(value: Any) -> int:
    if value in (None, "", 0, "0"):
        return 0
    parsed = int(Decimal(str(value)))
    digits = len(str(abs(parsed)))
    if digits <= 10:
        return parsed * 1_000_000_000
    if digits <= 13:
        return parsed * 1_000_000
    if digits <= 16:
        return parsed * 1_000
    return parsed


def datetime_to_nanos(value: Any) -> int | None:
    if value is None:
        return None
    if hasattr(value, "value"):
        return int(value.value)
    timestamp = value.timestamp()
    return int(timestamp * 1_000_000_000)


def epoch_to_datetime(value: Any) -> datetime | None:
    nanos = epoch_to_nanos(value)
    if nanos <= 0:
        return None
    return datetime.fromtimestamp(nanos / 1_000_000_000, tz=UTC)


def decimal_increment(precision: int) -> str:
    if precision <= 0:
        return "1"
    return "0." + ("0" * (precision - 1)) + "1"


def normalize_market_type(detail: dict[str, Any]) -> str:
    market_type = str(detail.get("market_type") or "").lower()
    market_id = int(detail.get("market_id") or 0)
    if "spot" in market_type or market_id >= 2048:
        return LIGHTER_MARKET_TYPE_SPOT
    return "perp"


def market_id_from_channel(channel: str | None) -> int | None:
    if not channel:
        return None
    parts = str(channel).replace(":", "/").split("/")
    for part in reversed(parts):
        if part.isdigit():
            return int(part)
    return None


def account_balances_from_assets(assets: list[dict[str, Any]]) -> list[AccountBalance]:
    balances: list[AccountBalance] = []
    for asset in assets:
        symbol = asset.get("symbol")
        if not symbol:
            continue
        currency = Currency.from_str(str(symbol))
        total = Decimal(str(asset.get("balance") or 0))
        locked = Decimal(str(asset.get("locked_balance") or 0))
        free = total - locked
        balances.append(
            AccountBalance(
                total=Money(total, currency),
                locked=Money(locked, currency),
                free=Money(free, currency),
            ),
        )
    return balances


def margin_balances_from_positions(
    positions: list[dict[str, Any]],
    instrument_lookup: Callable[[int], Instrument | None],
) -> list[MarginBalance]:
    margins: list[MarginBalance] = []
    for position in positions:
        market_id = position.get("market_id")
        if market_id is None:
            continue
        instrument = instrument_lookup(int(market_id))
        if instrument is None:
            continue
        allocated_margin = Decimal(str(position.get("allocated_margin") or 0))
        maintenance = Decimal(str(position.get("maintenance_margin") or 0))
        margins.append(
            MarginBalance(
                initial=Money(allocated_margin, instrument.quote_currency),
                maintenance=Money(maintenance, instrument.quote_currency),
                instrument_id=instrument.id,
            ),
        )
    return margins


def order_book_snapshot(
    instrument: Instrument,
    bids: list[dict[str, Any]],
    asks: list[dict[str, Any]],
    sequence: int,
    ts_event: int,
    ts_init: int,
) -> OrderBookDeltas:
    deltas: list[OrderBookDelta] = [
        OrderBookDelta.clear(instrument.id, sequence, ts_event, ts_init),
    ]
    deltas.extend(
        _book_side_deltas(instrument, bids, OrderSide.BUY, sequence, ts_event, ts_init),
    )
    deltas.extend(
        _book_side_deltas(instrument, asks, OrderSide.SELL, sequence, ts_event, ts_init),
    )
    return OrderBookDeltas(instrument_id=instrument.id, deltas=deltas)


def order_book_deltas(
    instrument: Instrument,
    bids: list[dict[str, Any]],
    asks: list[dict[str, Any]],
    sequence: int,
    ts_event: int,
    ts_init: int,
) -> OrderBookDeltas:
    deltas = _book_side_delta_updates(
        instrument,
        bids,
        OrderSide.BUY,
        sequence,
        ts_event,
        ts_init,
    )
    deltas.extend(
        _book_side_delta_updates(
            instrument,
            asks,
            OrderSide.SELL,
            sequence,
            ts_event,
            ts_init,
        ),
    )
    return OrderBookDeltas(instrument_id=instrument.id, deltas=deltas)


def _book_side_deltas(
    instrument: Instrument,
    levels: list[dict[str, Any]],
    side: OrderSide,
    sequence: int,
    ts_event: int,
    ts_init: int,
) -> list[OrderBookDelta]:
    deltas: list[OrderBookDelta] = []
    for level in levels:
        price = instrument.make_price(float(level["price"]))
        size = instrument.make_qty(float(level["size"]))
        deltas.append(
            OrderBookDelta(
                instrument.id,
                BookAction.ADD,
                BookOrder(side, price, size, 0),
                flags=0,
                sequence=sequence,
                ts_event=ts_event,
                ts_init=ts_init,
            ),
        )
    return deltas


def _book_side_delta_updates(
    instrument: Instrument,
    levels: list[dict[str, Any]],
    side: OrderSide,
    sequence: int,
    ts_event: int,
    ts_init: int,
) -> list[OrderBookDelta]:
    deltas: list[OrderBookDelta] = []
    for level in levels:
        price = instrument.make_price(float(level["price"]))
        size = instrument.make_qty(float(level["size"]))
        deltas.append(
            OrderBookDelta(
                instrument.id,
                BookAction.UPDATE if size > 0 else BookAction.DELETE,
                BookOrder(side, price, size, 0),
                flags=0,
                sequence=sequence,
                ts_event=ts_event,
                ts_init=ts_init,
            ),
        )
    return deltas


def quote_tick_from_ticker(
    instrument: Instrument,
    ticker: dict[str, Any],
    ts_event: int,
    ts_init: int,
) -> QuoteTick | None:
    ask = ticker.get("a") or {}
    bid = ticker.get("b") or {}
    bid_price = bid.get("price")
    ask_price = ask.get("price")
    bid_size = bid.get("size")
    ask_size = ask.get("size")
    if None in (bid_price, ask_price, bid_size, ask_size):
        return None
    return QuoteTick(
        instrument_id=instrument.id,
        bid_price=instrument.make_price(float(bid_price)),
        ask_price=instrument.make_price(float(ask_price)),
        bid_size=instrument.make_qty(float(bid_size)),
        ask_size=instrument.make_qty(float(ask_size)),
        ts_event=ts_event,
        ts_init=ts_init,
    )


def trade_tick_from_trade(
    instrument: Instrument,
    trade: dict[str, Any],
    ts_init: int,
) -> TradeTick:
    ts_event = epoch_to_nanos(trade.get("timestamp")) or ts_init
    aggressor_side = (
        AggressorSide.BUYER if bool(trade.get("is_maker_ask")) else AggressorSide.SELLER
    )
    return TradeTick(
        instrument_id=instrument.id,
        price=instrument.make_price(float(trade["price"])),
        size=instrument.make_qty(float(trade["size"])),
        aggressor_side=aggressor_side,
        trade_id=TradeId(str(trade["trade_id"])),
        ts_event=ts_event,
        ts_init=ts_init,
    )


def market_stats_to_updates(
    instrument: Instrument,
    market_stats: dict[str, Any],
    ts_event: int,
    ts_init: int,
) -> list[MarkPriceUpdate | IndexPriceUpdate | FundingRateUpdate]:
    updates: list[MarkPriceUpdate | IndexPriceUpdate | FundingRateUpdate] = []
    mark_price = market_stats.get("mark_price")
    if mark_price is not None:
        updates.append(
            MarkPriceUpdate(
                instrument_id=instrument.id,
                value=instrument.make_price(float(mark_price)),
                ts_event=ts_event,
                ts_init=ts_init,
            ),
        )
    index_price = market_stats.get("index_price")
    if index_price is not None:
        updates.append(
            IndexPriceUpdate(
                instrument_id=instrument.id,
                value=instrument.make_price(float(index_price)),
                ts_event=ts_event,
                ts_init=ts_init,
            ),
        )
    funding_rate = market_stats.get("current_funding_rate")
    if funding_rate is None:
        funding_rate = market_stats.get("funding_rate")
    if funding_rate is not None:
        next_funding_ns = epoch_to_nanos(
            market_stats.get("next_funding_time") or market_stats.get("settlement_time"),
        )
        updates.append(
            FundingRateUpdate(
                instrument_id=instrument.id,
                rate=Decimal(str(funding_rate)),
                next_funding_ns=next_funding_ns or None,
                ts_event=ts_event,
                ts_init=ts_init,
            ),
        )
    return updates


def candles_to_bars(
    instrument: Instrument,
    bar_type,
    candles: list[dict[str, Any]],
) -> list[Bar]:
    bars: list[Bar] = []
    for candle in sorted(candles, key=lambda item: int(item.get("timestamp") or 0)):
        ts_event = epoch_to_nanos(candle.get("timestamp"))
        bars.append(
            Bar(
                bar_type=bar_type,
                open=instrument.make_price(float(candle["open"])),
                high=instrument.make_price(float(candle["high"])),
                low=instrument.make_price(float(candle["low"])),
                close=instrument.make_price(float(candle["close"])),
                volume=instrument.make_qty(float(candle.get("volume") or 0)),
                ts_event=ts_event,
                ts_init=ts_event,
            ),
        )
    return bars


def order_type_from_lighter(value: Any) -> OrderType:
    if isinstance(value, int):
        mapping = {
            LIGHTER_LIMIT_ORDER: OrderType.LIMIT,
            LIGHTER_MARKET_ORDER: OrderType.MARKET,
            LIGHTER_STOP_LOSS_ORDER: OrderType.STOP_MARKET,
            LIGHTER_STOP_LOSS_LIMIT_ORDER: OrderType.STOP_LIMIT,
            LIGHTER_TAKE_PROFIT_ORDER: OrderType.MARKET_IF_TOUCHED,
            LIGHTER_TAKE_PROFIT_LIMIT_ORDER: OrderType.LIMIT_IF_TOUCHED,
        }
        return mapping.get(value, OrderType.LIMIT)

    raw = str(value or "").lower()
    mapping = {
        "limit": OrderType.LIMIT,
        "market": OrderType.MARKET,
        "stop-loss": OrderType.STOP_MARKET,
        "stop_loss": OrderType.STOP_MARKET,
        "stop-loss-limit": OrderType.STOP_LIMIT,
        "stop_loss_limit": OrderType.STOP_LIMIT,
        "take-profit": OrderType.MARKET_IF_TOUCHED,
        "take_profit": OrderType.MARKET_IF_TOUCHED,
        "take-profit-limit": OrderType.LIMIT_IF_TOUCHED,
        "take_profit_limit": OrderType.LIMIT_IF_TOUCHED,
    }
    return mapping.get(raw, OrderType.LIMIT)


def time_in_force_from_lighter(value: Any, post_only: bool = False) -> TimeInForce:
    if post_only:
        return TimeInForce.GTC
    if isinstance(value, int):
        if value == LIGHTER_TIF_IOC:
            return TimeInForce.IOC
        if value == LIGHTER_TIF_POST_ONLY:
            return TimeInForce.GTC
        return TimeInForce.GTD
    raw = str(value or "").lower()
    if raw in {"ioc", "immediate_or_cancel"}:
        return TimeInForce.IOC
    if raw in {"gtt", "gtd", "good_till_time"}:
        return TimeInForce.GTD
    return TimeInForce.GTC


def order_status_from_lighter(value: str) -> OrderStatus:
    raw = str(value or "").lower()
    if raw in {"in-progress", "pending"}:
        return OrderStatus.SUBMITTED
    if raw in {"new", "open"}:
        return OrderStatus.ACCEPTED
    if raw in {"partially_filled", "partially-filled"}:
        return OrderStatus.PARTIALLY_FILLED
    if raw == "filled":
        return OrderStatus.FILLED
    if raw == "rejected":
        return OrderStatus.REJECTED
    if raw.startswith(("canceled", "cancelled")):
        return OrderStatus.CANCELED
    if raw == "expired":
        return OrderStatus.EXPIRED
    return OrderStatus.SUBMITTED


def client_order_id_from_value(
    value: Any,
    resolver: Callable[[int], ClientOrderId | None],
) -> ClientOrderId | None:
    if value in (None, "", 0, "0"):
        return None
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return ClientOrderId(str(value))
    return resolver(numeric) or ClientOrderId(str(numeric))


def order_report_from_lighter(
    order: dict[str, Any],
    account_id: AccountId,
    instrument: Instrument,
    ts_init: int,
    resolver: Callable[[int], ClientOrderId | None],
) -> OrderStatusReport:
    order_status = order_status_from_lighter(str(order.get("status") or ""))
    order_type = order_type_from_lighter(order.get("type"))
    post_only = str(order.get("time_in_force") or "").lower() == "post_only"
    client_order_id = client_order_id_from_value(order.get("client_order_index"), resolver)
    if client_order_id is None:
        client_order_id = client_order_id_from_value(order.get("client_order_id"), resolver)
    price = order.get("price")
    trigger_price = order.get("trigger_price")
    ts_accepted = epoch_to_nanos(order.get("created_at") or order.get("timestamp"))
    ts_last = epoch_to_nanos(
        order.get("updated_at") or order.get("transaction_time") or order.get("timestamp"),
    )
    time_in_force = time_in_force_from_lighter(order.get("time_in_force"), post_only)
    cancel_reason = None
    status_text = str(order.get("status") or "")
    if status_text.startswith("canceled"):
        cancel_reason = status_text
    avg_px = None
    filled_base = Decimal(str(order.get("filled_base_amount") or 0))
    filled_quote = Decimal(str(order.get("filled_quote_amount") or 0))
    if filled_base > 0 and filled_quote > 0:
        avg_px = filled_quote / filled_base
    return OrderStatusReport(
        account_id=account_id,
        instrument_id=instrument.id,
        venue_order_id=VenueOrderId(str(order.get("order_index") or order.get("order_id"))),
        order_side=OrderSide.SELL if bool(order.get("is_ask")) else OrderSide.BUY,
        order_type=order_type,
        time_in_force=time_in_force,
        order_status=order_status,
        quantity=instrument.make_qty(float(order.get("initial_base_amount") or 0)),
        filled_qty=instrument.make_qty(float(order.get("filled_base_amount") or 0)),
        report_id=UUID4(),
        client_order_id=client_order_id,
        expire_time=(
            epoch_to_datetime(order.get("order_expiry"))
            if time_in_force == TimeInForce.GTD
            else None
        ),
        price=instrument.make_price(float(price)) if price not in (None, "", "0") else None,
        trigger_price=(
            instrument.make_price(float(trigger_price))
            if trigger_price not in (None, "", "0")
            else None
        ),
        trigger_type=(
            TriggerType.DEFAULT if trigger_price not in (None, "", "0") else TriggerType.NO_TRIGGER
        ),
        avg_px=avg_px,
        post_only=post_only,
        reduce_only=bool(order.get("reduce_only")),
        cancel_reason=cancel_reason,
        ts_accepted=ts_accepted,
        ts_last=ts_last,
        ts_init=ts_init,
    )


def fill_report_from_lighter_trade(
    trade: dict[str, Any],
    account_index: int,
    account_id: AccountId,
    instrument: Instrument,
    ts_init: int,
    resolver: Callable[[int], ClientOrderId | None],
) -> FillReport | None:
    ask_account_id = int(trade.get("ask_account_id") or 0)
    bid_account_id = int(trade.get("bid_account_id") or 0)
    if account_index not in (ask_account_id, bid_account_id):
        return None

    is_ask = account_index == ask_account_id
    is_maker = bool(trade.get("is_maker_ask")) if is_ask else not bool(trade.get("is_maker_ask"))
    fee_raw = trade.get("maker_fee") if is_maker else trade.get("taker_fee")
    commission = Money(
        Decimal(str(fee_raw or 0)) / LIGHTER_FEE_SCALE,
        instrument.quote_currency,
    )
    client_order_id = client_order_id_from_value(
        trade.get("ask_client_id") if is_ask else trade.get("bid_client_id"),
        resolver,
    )
    venue_order_id = VenueOrderId(str(trade.get("ask_id") if is_ask else trade.get("bid_id")))
    ts_event = epoch_to_nanos(trade.get("timestamp"))
    venue_position_id = None
    position_id = trade.get("position_id")
    if position_id not in (None, "", "0"):
        venue_position_id = PositionId(str(position_id))
    return FillReport(
        account_id=account_id,
        instrument_id=instrument.id,
        venue_order_id=venue_order_id,
        client_order_id=client_order_id,
        trade_id=TradeId(str(trade["trade_id"])),
        order_side=OrderSide.SELL if is_ask else OrderSide.BUY,
        last_qty=instrument.make_qty(float(trade["size"])),
        last_px=instrument.make_price(float(trade["price"])),
        commission=commission,
        liquidity_side=LiquiditySide.MAKER if is_maker else LiquiditySide.TAKER,
        report_id=UUID4(),
        ts_event=ts_event,
        ts_init=ts_init,
        venue_position_id=venue_position_id,
    )


def position_report_from_lighter(
    position: dict[str, Any],
    account_id: AccountId,
    instrument: Instrument,
    ts_init: int,
) -> PositionStatusReport:
    quantity_decimal = Decimal(str(position.get("position") or 0))
    sign = int(position.get("sign") or 0)
    quantity = abs(quantity_decimal)
    if quantity == 0:
        side = PositionSide.FLAT
    elif sign < 0:
        side = PositionSide.SHORT
    else:
        side = PositionSide.LONG

    avg_entry_price = position.get("avg_entry_price")
    return PositionStatusReport(
        account_id=account_id,
        instrument_id=instrument.id,
        position_side=side,
        quantity=instrument.make_qty(float(quantity)),
        report_id=UUID4(),
        avg_px_open=Decimal(str(avg_entry_price))
        if avg_entry_price not in (None, "", "0")
        else None,
        ts_last=ts_init,
        ts_init=ts_init,
    )


def to_lighter_price(value: Decimal, precision: int) -> int:
    factor = Decimal(10) ** precision
    return int((value * factor).to_integral_value(rounding=ROUND_HALF_UP))


def to_lighter_size(value: Decimal, precision: int) -> int:
    factor = Decimal(10) ** precision
    return int((value * factor).to_integral_value(rounding=ROUND_HALF_UP))
