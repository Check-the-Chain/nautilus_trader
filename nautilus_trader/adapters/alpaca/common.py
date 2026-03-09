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

import time
from decimal import Decimal
from typing import Any

import pandas as pd

from nautilus_trader.adapters.alpaca.constants import ALPACA_VENUE
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import ContingencyType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OptionKind
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import TrailingOffsetType
from nautilus_trader.model.enums import TriggerType
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.instruments import OptionContract
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


ALPACA_ORDER_STATUS: dict[str, OrderStatus] = {
    "submitted": OrderStatus.SUBMITTED,
    "accepted": OrderStatus.ACCEPTED,
    "accepted_for_bidding": OrderStatus.ACCEPTED,
    "pending_new": OrderStatus.ACCEPTED,
    "new": OrderStatus.ACCEPTED,
    "partially_filled": OrderStatus.PARTIALLY_FILLED,
    "filled": OrderStatus.FILLED,
    "done_for_day": OrderStatus.EXPIRED,
    "canceled": OrderStatus.CANCELED,
    "expired": OrderStatus.EXPIRED,
    "replaced": OrderStatus.ACCEPTED,
    "pending_cancel": OrderStatus.PENDING_CANCEL,
    "pending_replace": OrderStatus.PENDING_UPDATE,
    "rejected": OrderStatus.REJECTED,
    "stopped": OrderStatus.TRIGGERED,
    "suspended": OrderStatus.ACCEPTED,
    "calculated": OrderStatus.ACCEPTED,
}

ALPACA_ORDER_SIDE: dict[str, OrderSide] = {
    "buy": OrderSide.BUY,
    "sell": OrderSide.SELL,
}

ALPACA_ORDER_TYPE: dict[str, OrderType] = {
    "market": OrderType.MARKET,
    "limit": OrderType.LIMIT,
    "stop": OrderType.STOP_MARKET,
    "stop_limit": OrderType.STOP_LIMIT,
    "trailing_stop": OrderType.TRAILING_STOP_MARKET,
}

ALPACA_TIME_IN_FORCE: dict[str, TimeInForce] = {
    "day": TimeInForce.DAY,
    "gtc": TimeInForce.GTC,
    "opg": TimeInForce.AT_THE_OPEN,
    "cls": TimeInForce.AT_THE_CLOSE,
    "ioc": TimeInForce.IOC,
    "fok": TimeInForce.FOK,
}

ALPACA_OPTION_ASSET_CLASSES = frozenset({"option", "us_option"})


def get_timestamp_ns(value: str | None, default: int | None = None) -> int:
    if value:
        return pd.Timestamp(value).value
    return default if default is not None else time.time_ns()


def precision_from_value(value: str | Decimal | float | None, default: int) -> int:
    if value is None:
        return default

    decimal = Decimal(str(value)).normalize()
    exponent = decimal.as_tuple().exponent
    precision = max(-exponent, 0)
    return precision or default


def normalize_symbol(symbol: str | None) -> str:
    return str(symbol or "").strip().upper()


def is_crypto_symbol(symbol: str) -> bool:
    normalized = normalize_symbol(symbol)
    return "/" in normalized or (normalized.endswith("USD") and len(normalized) > 3)


def data_symbol_from_symbol(symbol: str) -> str:
    normalized = normalize_symbol(symbol)
    if "/" in normalized:
        return normalized
    if normalized.endswith("USD") and len(normalized) > 3:
        return f"{normalized[:-3]}/USD"
    return normalized


def trade_symbol_from_symbol(symbol: str) -> str:
    return data_symbol_from_symbol(symbol).replace("/", "")


def symbol_to_instrument_id(symbol: str) -> InstrumentId:
    return InstrumentId(Symbol(data_symbol_from_symbol(symbol)), ALPACA_VENUE)


def asset_class_from_asset(asset: dict[str, Any]) -> str:
    asset_class = str(asset.get("class") or asset.get("asset_class") or "").lower()
    if asset_class in ALPACA_OPTION_ASSET_CLASSES:
        return "option"
    if _is_option_asset(asset):
        return "option"
    return asset_class


def is_crypto_instrument(instrument: Instrument) -> bool:
    return isinstance(instrument, CurrencyPair)


def is_equity_instrument(instrument: Instrument) -> bool:
    return isinstance(instrument, Equity)


def is_option_instrument(instrument: Instrument) -> bool:
    return isinstance(instrument, OptionContract)


def quote_currency_for_instrument(instrument: Instrument) -> Currency:
    if hasattr(instrument, "quote_currency"):
        return instrument.quote_currency
    return instrument.currency


def data_symbol_for_instrument(instrument: Instrument) -> str:
    info = instrument.info or {}
    return normalize_symbol(str(info.get("data_symbol") or instrument.id.symbol.value))


def trade_symbol_for_instrument(instrument: Instrument) -> str:
    info = instrument.info or {}
    value = str(info.get("trade_symbol") or instrument.id.symbol.value)
    return normalize_symbol(value).replace("/", "")


def extract_items_for_symbol(
    payload: dict[str, Any],
    key: str,
    symbol: str,
) -> list[dict[str, Any]]:
    items = payload.get(key) or {}
    if isinstance(items, list):
        return items
    if not isinstance(items, dict):
        return []

    candidates = [
        normalize_symbol(symbol),
        data_symbol_from_symbol(symbol),
        trade_symbol_from_symbol(symbol),
    ]
    for candidate in candidates:
        value = items.get(candidate)
        if isinstance(value, list):
            return value
    return []


def _is_option_asset(asset: dict[str, Any]) -> bool:
    option_type = str(asset.get("type") or asset.get("option_type") or "").lower()
    return (
        bool(asset.get("underlying_symbol") or asset.get("root_symbol"))
        and bool(asset.get("expiration_date"))
        and option_type in {"call", "put"}
    )


def _option_expiration_ns(value: str) -> int:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        if len(value) <= 10:
            ts = pd.Timestamp(f"{value} 16:00:00", tz="America/New_York")
        else:
            ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")

    return ts.tz_convert("UTC").value


def _option_activation_ns(expiration_ns: int) -> int:
    activation = pd.Timestamp(expiration_ns, unit="ns", tz="UTC") - pd.Timedelta(days=90)
    return min(time.time_ns(), activation.value)


def asset_to_instrument(asset: dict[str, Any]) -> Instrument | None:
    raw_symbol = normalize_symbol(asset["symbol"])
    asset_class = asset_class_from_asset(asset)
    data_symbol = data_symbol_from_symbol(raw_symbol)
    trade_symbol = trade_symbol_from_symbol(raw_symbol)
    instrument_id = symbol_to_instrument_id(data_symbol)
    ts_now = time.time_ns()
    info = {
        **asset,
        "asset_class": asset_class,
        "data_symbol": data_symbol,
        "trade_symbol": trade_symbol,
    }

    if asset_class == "us_equity":
        price_increment_str = str(asset.get("price_increment") or "0.01")
        price_precision = precision_from_value(price_increment_str, 2)
        return Equity(
            instrument_id=instrument_id,
            raw_symbol=Symbol(data_symbol),
            currency=USD,
            price_precision=price_precision,
            price_increment=Price.from_str(price_increment_str),
            lot_size=Quantity.from_int(1),
            max_quantity=None,
            min_quantity=Quantity.from_int(1),
            ts_event=ts_now,
            ts_init=ts_now,
            info=info,
        )

    if asset_class == "crypto":
        base, quote = data_symbol.split("/", maxsplit=1)
        size_increment_str = str(asset.get("min_trade_increment") or "0.00000001")
        price_increment_str = str(asset.get("price_increment") or "0.01")
        min_order_size = asset.get("min_order_size")
        size_precision = precision_from_value(size_increment_str, 8)
        price_precision = precision_from_value(price_increment_str, 2)
        return CurrencyPair(
            instrument_id=instrument_id,
            raw_symbol=Symbol(data_symbol),
            base_currency=Currency.from_str(base),
            quote_currency=Currency.from_str(quote),
            price_precision=price_precision,
            size_precision=size_precision,
            price_increment=Price.from_str(price_increment_str),
            size_increment=Quantity.from_str(size_increment_str),
            lot_size=None,
            max_quantity=None,
            min_quantity=(
                Quantity.from_str(str(min_order_size))
                if min_order_size is not None
                else Quantity.from_str(size_increment_str)
            ),
            max_notional=None,
            min_notional=None,
            max_price=None,
            min_price=None,
            margin_init=Decimal(0),
            margin_maint=Decimal(0),
            maker_fee=Decimal(0),
            taker_fee=Decimal(0),
            ts_event=ts_now,
            ts_init=ts_now,
            info=info,
        )

    if asset_class == "option":
        option_type = str(asset.get("type") or asset.get("option_type") or "").lower()
        option_kind = {
            "call": OptionKind.CALL,
            "put": OptionKind.PUT,
        }.get(option_type)
        expiration_date = asset.get("expiration_date")
        strike_price = asset.get("strike_price")
        underlying = normalize_symbol(
            str(asset.get("underlying_symbol") or asset.get("root_symbol") or ""),
        )
        if option_kind is None or expiration_date is None or strike_price is None or not underlying:
            return None

        price_increment_str = str(asset.get("price_increment") or "0.01")
        price_precision = precision_from_value(price_increment_str, 2)
        strike_precision = max(price_precision, precision_from_value(strike_price, 2))
        expiration_ns = _option_expiration_ns(str(expiration_date))
        multiplier_value = asset.get("multiplier") or asset.get("size") or "100"
        exchange = asset.get("exchange")
        return OptionContract(
            instrument_id=instrument_id,
            raw_symbol=Symbol(raw_symbol),
            asset_class=AssetClass.EQUITY,
            currency=USD,
            price_precision=price_precision,
            price_increment=Price.from_str(price_increment_str),
            multiplier=Quantity.from_str(str(multiplier_value)),
            lot_size=Quantity.from_int(1),
            underlying=underlying,
            option_kind=option_kind,
            strike_price=Price(float(strike_price), strike_precision),
            activation_ns=_option_activation_ns(expiration_ns),
            expiration_ns=expiration_ns,
            ts_event=ts_now,
            ts_init=ts_now,
            exchange=str(exchange) if exchange is not None else None,
            info=info,
        )

    return None


def make_quote_tick(instrument: Instrument, payload: dict[str, Any], ts_init: int) -> QuoteTick:
    ts_event = get_timestamp_ns(payload.get("t"), default=ts_init)
    return QuoteTick(
        instrument_id=instrument.id,
        bid_price=instrument.make_price(payload["bp"]),
        ask_price=instrument.make_price(payload["ap"]),
        bid_size=instrument.make_qty(payload["bs"]),
        ask_size=instrument.make_qty(payload["as"]),
        ts_event=ts_event,
        ts_init=max(ts_init, ts_event),
    )


def make_trade_tick(instrument: Instrument, payload: dict[str, Any], ts_init: int) -> TradeTick:
    ts_event = get_timestamp_ns(payload.get("t"), default=ts_init)
    trade_id = str(
        payload.get("i")
        or payload.get("id")
        or payload.get("trade_id")
        or f"{data_symbol_for_instrument(instrument)}-{ts_event}"
    )
    return TradeTick(
        instrument_id=instrument.id,
        price=instrument.make_price(payload["p"]),
        size=instrument.make_qty(payload["s"]),
        aggressor_side=AggressorSide.NO_AGGRESSOR,
        trade_id=TradeId(trade_id),
        ts_event=ts_event,
        ts_init=max(ts_init, ts_event),
    )


def make_bar(instrument: Instrument, bar_type: BarType, payload: dict[str, Any], ts_init: int) -> Bar:
    ts_event = get_timestamp_ns(payload.get("t"), default=ts_init)
    return Bar(
        bar_type=bar_type,
        open=instrument.make_price(payload["o"]),
        high=instrument.make_price(payload["h"]),
        low=instrument.make_price(payload["l"]),
        close=instrument.make_price(payload["c"]),
        volume=instrument.make_qty(payload.get("v", 0)),
        ts_event=ts_event,
        ts_init=max(ts_init, ts_event),
        is_revision=False,
    )


def bar_type_to_timeframe(bar_type: BarType) -> str:
    if not bar_type.spec.is_time_aggregated():
        raise ValueError("Only time-aggregated bars are supported by Alpaca")
    if bar_type.spec.price_type != PriceType.LAST:
        raise ValueError("Only LAST price bars are supported by Alpaca")

    aggregation = bar_type.spec.aggregation
    step = bar_type.spec.step

    if aggregation == BarAggregation.MINUTE:
        return f"{step}Min"
    if aggregation == BarAggregation.HOUR:
        return f"{step}Hour"
    if aggregation == BarAggregation.DAY:
        return f"{step}Day"
    if aggregation == BarAggregation.WEEK:
        return f"{step}Week"
    if aggregation == BarAggregation.MONTH:
        return f"{step}Month"
    raise ValueError(f"Unsupported Alpaca timeframe for bar type {bar_type}")


def account_type_from_account(account: dict[str, Any]) -> AccountType:
    multiplier = Decimal(str(account.get("multiplier") or "1"))
    return AccountType.MARGIN if multiplier > 1 else AccountType.CASH


def account_balance_from_account(account: dict[str, Any]) -> AccountBalance:
    cash = Decimal(str(account.get("cash") or "0"))
    money = Money(cash, USD)
    return AccountBalance(total=money, locked=Money(0, USD), free=money)


def order_to_report(
    account_id: AccountId,
    instrument: Instrument,
    order: dict[str, Any],
    *,
    client_order_id: ClientOrderId | None = None,
    venue_position_id: PositionId | None = None,
) -> OrderStatusReport:
    order_type = ALPACA_ORDER_TYPE.get(order["type"])
    if order_type is None:
        raise ValueError(f"Unsupported Alpaca order type {order['type']!r}")

    tif = ALPACA_TIME_IN_FORCE.get(order["time_in_force"])
    if tif is None:
        raise ValueError(f"Unsupported Alpaca time in force {order['time_in_force']!r}")

    order_status = ALPACA_ORDER_STATUS.get(order["status"])
    if order_status is None:
        raise ValueError(f"Unsupported Alpaca order status {order['status']!r}")

    trigger_price = order.get("stop_price")
    trigger_type = TriggerType.LAST_PRICE if trigger_price is not None else TriggerType.NO_TRIGGER

    trailing_offset = None
    trailing_offset_type = None
    if order_type == OrderType.TRAILING_STOP_MARKET:
        if order.get("trail_price") is not None:
            trailing_offset = Decimal(str(order["trail_price"]))
            trailing_offset_type = TrailingOffsetType.PRICE
        elif order.get("trail_percent") is not None:
            trailing_offset = Decimal(str(order["trail_percent"])) * 100
            trailing_offset_type = TrailingOffsetType.BASIS_POINTS

    contingency_type = ContingencyType.NO_CONTINGENCY
    order_class = order.get("order_class")
    if order_class == "oco":
        contingency_type = ContingencyType.OCO
    elif order_class in {"oto", "bracket"}:
        contingency_type = ContingencyType.OTO

    return OrderStatusReport(
        account_id=account_id,
        instrument_id=instrument.id,
        venue_order_id=VenueOrderId(order["id"]),
        order_side=ALPACA_ORDER_SIDE[order["side"]],
        order_type=order_type,
        time_in_force=tif,
        order_status=order_status,
        quantity=instrument.make_qty(order["qty"]),
        filled_qty=instrument.make_qty(order.get("filled_qty") or "0"),
        report_id=UUID4(),
        ts_accepted=get_timestamp_ns(order.get("submitted_at") or order.get("created_at")),
        ts_last=get_timestamp_ns(
            order.get("updated_at")
            or order.get("filled_at")
            or order.get("canceled_at")
            or order.get("expired_at"),
        ),
        ts_init=time.time_ns(),
        client_order_id=client_order_id or ClientOrderId(order["client_order_id"]),
        venue_position_id=venue_position_id,
        contingency_type=contingency_type,
        expire_time=(
            pd.Timestamp(order["expires_at"]).to_pydatetime()
            if order.get("expires_at") is not None
            else None
        ),
        price=instrument.make_price(order["limit_price"]) if order.get("limit_price") else None,
        trigger_price=instrument.make_price(trigger_price) if trigger_price else None,
        trigger_type=trigger_type,
        trailing_offset=trailing_offset,
        trailing_offset_type=trailing_offset_type,
        avg_px=Decimal(str(order["filled_avg_price"])) if order.get("filled_avg_price") else None,
        cancel_reason=(
            str(order.get("status"))
            if order_status in {OrderStatus.CANCELED, OrderStatus.REJECTED}
            else None
        ),
        ts_triggered=get_timestamp_ns(order.get("filled_at"), default=0),
    )


def activity_to_fill_report(
    account_id: AccountId,
    instrument: Instrument,
    activity: dict[str, Any],
    *,
    client_order_id: ClientOrderId | None = None,
    venue_position_id: PositionId | None = None,
) -> FillReport:
    return FillReport(
        account_id=account_id,
        instrument_id=instrument.id,
        venue_order_id=VenueOrderId(activity["order_id"]),
        trade_id=TradeId(str(activity["id"])),
        order_side=ALPACA_ORDER_SIDE[activity["side"]],
        last_qty=instrument.make_qty(activity["qty"]),
        last_px=instrument.make_price(activity["price"]),
        commission=Money(0, quote_currency_for_instrument(instrument)),
        liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
        report_id=UUID4(),
        ts_event=get_timestamp_ns(activity.get("transaction_time")),
        ts_init=time.time_ns(),
        client_order_id=client_order_id,
        venue_position_id=venue_position_id,
    )


def position_to_report(
    account_id: AccountId,
    instrument: Instrument,
    position: dict[str, Any],
) -> PositionStatusReport:
    side_text = str(position.get("side", "")).lower()
    qty_decimal = Decimal(str(position.get("qty") or "0"))
    quantity = abs(qty_decimal)
    if quantity == 0:
        side = PositionSide.FLAT
    elif side_text == "short" or qty_decimal < 0:
        side = PositionSide.SHORT
    else:
        side = PositionSide.LONG

    return PositionStatusReport(
        account_id=account_id,
        instrument_id=instrument.id,
        position_side=side,
        quantity=instrument.make_qty(str(quantity)),
        report_id=UUID4(),
        ts_last=get_timestamp_ns(position.get("updated_at")),
        ts_init=time.time_ns(),
        venue_position_id=PositionId(str(position["asset_id"])),
        avg_px_open=Decimal(str(position["avg_entry_price"]))
        if position.get("avg_entry_price") is not None
        else None,
    )
