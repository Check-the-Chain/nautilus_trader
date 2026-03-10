from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import timedelta
from decimal import Decimal
from typing import Any

from nautilus_trader.common.enums import LogColor
from nautilus_trader.common.events import TimeEvent
from nautilus_trader.config import ExecAlgorithmConfig
from nautilus_trader.config import NonNegativeInt
from nautilus_trader.config import PositiveFloat
from nautilus_trader.config import PositiveInt
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.execution.algorithm import ExecAlgorithm
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import ExecAlgorithmId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import LimitOrder
from nautilus_trader.model.orders import Order
from nautilus_trader.model.orders import OrderList


def _parse_decimal(value: Any, name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception as exc:  # pragma: no cover - defensive parsing
        raise ValueError(f"Invalid decimal for `{name}`: {value!r}") from exc


def _parse_int(value: Any, name: str) -> int:
    try:
        return int(value)
    except Exception as exc:  # pragma: no cover - defensive parsing
        raise ValueError(f"Invalid integer for `{name}`: {value!r}") from exc


def _parse_float(value: Any, name: str) -> float:
    try:
        return float(value)
    except Exception as exc:  # pragma: no cover - defensive parsing
        raise ValueError(f"Invalid float for `{name}`: {value!r}") from exc


class LimitChaserExecAlgorithmConfig(ExecAlgorithmConfig, frozen=True):
    """
    Configuration for ``LimitChaserExecAlgorithm`` instances.

    This algorithm follows the top of book for primary ``LIMIT`` orders. It starts
    passively from the same-side touch and can optionally turn aggressive after a
    configured amount of time, while still respecting the primary order limit.

    Parameters
    ----------
    exec_algorithm_id : ExecAlgorithmId, optional
        The execution algorithm ID (will override default which is the class name).
    follow_offset_ticks : int, default 0
        The passive offset from the same-side touch.
        For BUY orders, 0 joins best bid, 1 works one tick below best bid.
        For SELL orders, 0 joins best ask, 1 works one tick above best ask.
    aggressive_offset_ticks : int, default 0
        The aggressive offset from the opposite-side touch once the sequence has
        reached ``aggressive_after_secs``.
    aggressive_after_secs : float, optional
        If provided, switches the sequence from passive chasing to aggressive
        chasing after this many seconds.
    max_child_quantity : Decimal, optional
        If provided, the maximum quantity for each interim spawned child slice.
        The final slice is submitted as the primary order itself.
    reprice_interval_ms : int, default 250
        The minimum interval between repricing attempts for a sequence.
    min_reprice_delta_ticks : int, default 1
        The minimum target price change (in ticks) required before a reprice is
        sent.

    """

    exec_algorithm_id: ExecAlgorithmId | None = ExecAlgorithmId("LIMIT_CHASER")
    follow_offset_ticks: NonNegativeInt = 0
    aggressive_offset_ticks: NonNegativeInt = 0
    aggressive_after_secs: PositiveFloat | None = None
    max_child_quantity: Decimal | None = None
    reprice_interval_ms: PositiveInt = 250
    min_reprice_delta_ticks: NonNegativeInt = 1


@dataclass
class LimitChaserSettings:
    follow_offset_ticks: int
    aggressive_offset_ticks: int
    aggressive_after_secs: float | None
    max_child_quantity: Quantity | None
    reprice_interval_ms: int
    min_reprice_delta_ticks: int


@dataclass
class LimitChaserSequence:
    primary_order_id: ClientOrderId
    instrument_id: InstrumentId
    started_at_ns: int
    limit_price: Price
    settings: LimitChaserSettings
    working_order_id: ClientOrderId | None = None
    working_is_primary: bool = False
    last_reprice_ns: int = 0
    cancel_requested: bool = False
    pending_quantity: Quantity | None = None
    pending_reduce_primary: bool = True
    tracked_order_ids: set[ClientOrderId] = field(default_factory=set)


class LimitChaserExecAlgorithm(ExecAlgorithm):
    """
    A quote-driven limit chaser execution algorithm.

    The algorithm expects primary ``LIMIT`` orders. It uses the primary order price
    as the hard cap/floor, reprices toward the top of book, optionally slices
    larger orders into interim child orders, and submits the final slice as the
    primary order itself.

    Per-order overrides can be supplied via ``exec_algorithm_params`` using these
    keys:

    - ``follow_offset_ticks``
    - ``aggressive_offset_ticks``
    - ``aggressive_after_secs``
    - ``max_child_quantity``
    - ``reprice_interval_ms``
    - ``min_reprice_delta_ticks``

    """

    def __init__(self, config: LimitChaserExecAlgorithmConfig | None = None) -> None:
        if config is None:
            config = LimitChaserExecAlgorithmConfig()
        super().__init__(config)

        self._sequences: dict[ClientOrderId, LimitChaserSequence] = {}
        self._instrument_sequences: dict[InstrumentId, set[ClientOrderId]] = {}
        self._subscribed_instruments: set[InstrumentId] = set()

    def on_start(self) -> None:
        """
        Actions to be performed when the algorithm component is started.
        """

    def on_stop(self) -> None:
        """
        Actions to be performed when the algorithm component is stopped.
        """
        self.clock.cancel_timers()

    def on_reset(self) -> None:
        """
        Actions to be performed when the algorithm component is reset.
        """
        self._sequences.clear()
        self._instrument_sequences.clear()
        self._subscribed_instruments.clear()

    def on_save(self) -> dict[str, bytes]:
        """
        Actions to be performed when the algorithm component is saved.

        Returns
        -------
        dict[str, bytes]

        """
        return {}

    def on_load(self, state: dict[str, bytes]) -> None:
        """
        Actions to be performed when the algorithm component is loaded.

        Parameters
        ----------
        state : dict[str, bytes]
            The algorithm component state dictionary.

        """

    def on_order(self, order: Order) -> None:
        """
        Actions to be performed when running and receives an order.

        Parameters
        ----------
        order : Order
            The order to be handled.

        """
        self.log.info(repr(order), LogColor.CYAN)

        if order.order_type != OrderType.LIMIT:
            self.log.error(
                f"Cannot execute order: only implemented for limit orders, {order.order_type=}",
            )
            return

        if order.time_in_force in (TimeInForce.FOK, TimeInForce.IOC):
            self.log.error(
                f"Cannot execute order: unsupported time in force for chasing, {order.time_in_force=}",
            )
            return

        instrument = self.cache.instrument(order.instrument_id)
        if instrument is None:
            self.log.error(
                f"Cannot execute order: instrument {order.instrument_id} not found",
            )
            return

        settings = self._resolve_settings(order=order, instrument=instrument)
        sequence = LimitChaserSequence(
            primary_order_id=order.client_order_id,
            instrument_id=order.instrument_id,
            started_at_ns=self.clock.timestamp_ns(),
            limit_price=order.price,
            settings=settings,
            tracked_order_ids={order.client_order_id},
        )
        self._sequences[order.client_order_id] = sequence
        self._instrument_sequences.setdefault(order.instrument_id, set()).add(order.client_order_id)

        if order.instrument_id not in self._subscribed_instruments:
            self.subscribe_quote_ticks(order.instrument_id)
            self._subscribed_instruments.add(order.instrument_id)

        self.clock.set_timer(
            name=order.client_order_id.value,
            interval=timedelta(milliseconds=settings.reprice_interval_ms),
            callback=self.on_time_event,
        )

        self._refresh_sequence(order.client_order_id, force=True)

    def on_order_list(self, order_list: OrderList) -> None:
        """
        Actions to be performed when running and receives an order list.

        Parameters
        ----------
        order_list : OrderList
            The order list to be handled.

        """
        self.log.info(repr(order_list), LogColor.CYAN)
        for order in order_list.orders:
            self.on_order(order)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        """
        Actions to be performed when running and receives a quote tick.

        Parameters
        ----------
        tick : QuoteTick
            The quote tick received.

        """
        for primary_order_id in list(self._instrument_sequences.get(tick.instrument_id, set())):
            self._refresh_sequence(primary_order_id)

    def on_time_event(self, event: TimeEvent) -> None:
        """
        Actions to be performed when the algorithm receives a time event.

        Parameters
        ----------
        event : TimeEvent
            The time event received.

        """
        self._refresh_sequence(ClientOrderId(event.name))

    def on_order_accepted(self, event) -> None:  # type: ignore[override]
        self._refresh_for_order(event.client_order_id)

    def on_order_filled(self, event) -> None:  # type: ignore[override]
        self._refresh_for_order(event.client_order_id, force=True)

    def on_order_rejected(self, event) -> None:  # type: ignore[override]
        self._refresh_for_order(event.client_order_id, force=True)

    def on_order_denied(self, event) -> None:  # type: ignore[override]
        self._refresh_for_order(event.client_order_id, force=True)

    def on_order_expired(self, event) -> None:  # type: ignore[override]
        self._refresh_for_order(event.client_order_id, force=True)

    def on_order_canceled(self, event) -> None:  # type: ignore[override]
        order = self.cache.order(event.client_order_id)
        if order is None:
            return

        primary_order_id = order.exec_spawn_id
        sequence = self._sequences.get(primary_order_id)
        if sequence is None:
            return

        if order.is_primary:
            sequence.cancel_requested = True

        if order.is_primary and sequence.working_order_id not in (None, primary_order_id):
            working_order = self.cache.order(sequence.working_order_id)
            if (
                working_order is not None
                and not working_order.is_closed
                and not working_order.is_pending_cancel
            ):
                self.cancel_order(working_order)
                return

        self._refresh_sequence(primary_order_id, force=True)

    def on_order_modify_rejected(self, event) -> None:  # type: ignore[override]
        self._refresh_for_order(event.client_order_id, force=True)

    def on_order_cancel_rejected(self, event) -> None:  # type: ignore[override]
        self._refresh_for_order(event.client_order_id, force=True)

    def _refresh_for_order(self, client_order_id: ClientOrderId, force: bool = False) -> None:
        order = self.cache.order(client_order_id)
        if order is None:
            return

        self._refresh_sequence(order.exec_spawn_id, force=force)

    def _refresh_sequence(self, primary_order_id: ClientOrderId, force: bool = False) -> None:
        sequence = self._sequences.get(primary_order_id)
        if sequence is None:
            return

        primary_order = self.cache.order(primary_order_id)
        if primary_order is None:
            self._cleanup_sequence(primary_order_id)
            return

        quote = self.cache.quote_tick(sequence.instrument_id)

        if sequence.cancel_requested:
            working_order = self._working_order(sequence)
            if working_order is None or working_order.is_closed:
                self._cleanup_sequence(primary_order_id)
            return

        working_order = self._working_order(sequence)
        if working_order is not None and working_order.is_closed:
            self._handle_closed_working_order(sequence, primary_order, working_order)
            return

        if working_order is None:
            self._submit_pending_or_next(sequence, primary_order, quote)
            return

        if quote is None:
            return

        if (
            not force
            and self.clock.timestamp_ns()
            < sequence.last_reprice_ns + (sequence.settings.reprice_interval_ms * 1_000_000)
        ):
            return

        if working_order.is_inflight or working_order.is_pending_update or working_order.is_pending_cancel:
            return

        if working_order.price is None:
            return

        target_price = self._target_price(primary_order=primary_order, quote=quote, sequence=sequence)
        if target_price is None:
            return

        instrument = self.cache.instrument(sequence.instrument_id)
        if instrument is None:
            self.log.error(
                f"Cannot refresh order: instrument {sequence.instrument_id} not found",
            )
            return

        if not self._should_reprice(
            instrument=instrument,
            current_price=working_order.price,
            target_price=target_price,
            min_delta_ticks=sequence.settings.min_reprice_delta_ticks,
        ):
            return

        self.modify_order(working_order, price=target_price)
        sequence.last_reprice_ns = self.clock.timestamp_ns()

    def _handle_closed_working_order(
        self,
        sequence: LimitChaserSequence,
        primary_order: Order,
        working_order: Order,
    ) -> None:
        sequence.working_order_id = None
        sequence.working_is_primary = False

        if sequence.cancel_requested:
            self._cleanup_sequence(sequence.primary_order_id)
            return

        if working_order.is_primary:
            self._cleanup_sequence(sequence.primary_order_id)
            return

        if (
            working_order.status in (OrderStatus.CANCELED, OrderStatus.EXPIRED)
            and working_order.leaves_qty > 0
        ):
            sequence.pending_quantity = working_order.leaves_qty
            sequence.pending_reduce_primary = False

        quote = self.cache.quote_tick(sequence.instrument_id)
        if quote is None:
            return

        self._submit_pending_or_next(sequence, primary_order, quote)

    def _submit_pending_or_next(
        self,
        sequence: LimitChaserSequence,
        primary_order: Order,
        quote: QuoteTick | None,
    ) -> None:
        if quote is None:
            return

        if sequence.pending_quantity is not None:
            quantity = sequence.pending_quantity
            reduce_primary = sequence.pending_reduce_primary
            sequence.pending_quantity = None
            sequence.pending_reduce_primary = True
            self._submit_spawned_order(
                sequence=sequence,
                primary_order=primary_order,
                quantity=quantity,
                quote=quote,
                reduce_primary=reduce_primary,
            )
            return

        next_quantity = self._next_slice_quantity(
            instrument=self.cache.instrument(sequence.instrument_id),
            primary_order=primary_order,
            settings=sequence.settings,
        )
        if next_quantity is None:
            return

        if next_quantity == primary_order.quantity:
            self._submit_primary_order(sequence, primary_order, quote)
            return

        self._submit_spawned_order(
            sequence=sequence,
            primary_order=primary_order,
            quantity=next_quantity,
            quote=quote,
            reduce_primary=True,
        )

    def _submit_primary_order(
        self,
        sequence: LimitChaserSequence,
        primary_order: Order,
        quote: QuoteTick,
    ) -> None:
        target_price = self._target_price(primary_order=primary_order, quote=quote, sequence=sequence)
        if target_price is None:
            return

        self.modify_order_in_place(primary_order, price=target_price)
        self.submit_order(primary_order)
        sequence.working_order_id = primary_order.client_order_id
        sequence.working_is_primary = True
        sequence.last_reprice_ns = self.clock.timestamp_ns()

    def _submit_spawned_order(
        self,
        sequence: LimitChaserSequence,
        primary_order: Order,
        quantity: Quantity,
        quote: QuoteTick,
        reduce_primary: bool,
    ) -> None:
        target_price = self._target_price(primary_order=primary_order, quote=quote, sequence=sequence)
        if target_price is None:
            sequence.pending_quantity = quantity
            sequence.pending_reduce_primary = reduce_primary
            return

        spawned_order: LimitOrder = self.spawn_limit(
            primary=primary_order,
            quantity=quantity,
            price=target_price,
            time_in_force=primary_order.time_in_force,
            expire_time=primary_order.expire_time,
            post_only=primary_order.is_post_only,
            reduce_only=primary_order.is_reduce_only,
            display_qty=self._child_display_qty(primary_order=primary_order, quantity=quantity),
            tags=primary_order.tags,
            reduce_primary=reduce_primary,
        )

        self.submit_order(spawned_order)
        sequence.working_order_id = spawned_order.client_order_id
        sequence.working_is_primary = False
        sequence.last_reprice_ns = self.clock.timestamp_ns()
        sequence.tracked_order_ids.add(spawned_order.client_order_id)

    def _next_slice_quantity(
        self,
        instrument: Instrument | None,
        primary_order: Order,
        settings: LimitChaserSettings,
    ) -> Quantity | None:
        if instrument is None:
            self.log.error(
                f"Cannot determine next slice quantity: instrument {primary_order.instrument_id} not found",
            )
            return None

        if settings.max_child_quantity is None or settings.max_child_quantity >= primary_order.quantity:
            return primary_order.quantity

        return settings.max_child_quantity

    def _target_price(
        self,
        primary_order: Order,
        quote: QuoteTick,
        sequence: LimitChaserSequence,
    ) -> Price | None:
        instrument = self.cache.instrument(primary_order.instrument_id)
        if instrument is None:
            self.log.error(
                f"Cannot determine target price: instrument {primary_order.instrument_id} not found",
            )
            return None

        best_bid = quote.bid_price
        best_ask = quote.ask_price
        if best_bid is None or best_ask is None:
            return None

        tick_size = instrument.price_increment.as_decimal()
        limit_price = sequence.limit_price.as_decimal()
        aggressive = self._is_aggressive(sequence)

        if primary_order.is_buy:
            if aggressive and not primary_order.is_post_only:
                target = best_ask.as_decimal() + (tick_size * sequence.settings.aggressive_offset_ticks)
            elif aggressive:
                target = best_bid.as_decimal()
            else:
                target = best_bid.as_decimal() - (tick_size * sequence.settings.follow_offset_ticks)
            target = min(target, limit_price)
        else:
            if aggressive and not primary_order.is_post_only:
                target = best_bid.as_decimal() - (tick_size * sequence.settings.aggressive_offset_ticks)
            elif aggressive:
                target = best_ask.as_decimal()
            else:
                target = best_ask.as_decimal() + (tick_size * sequence.settings.follow_offset_ticks)
            target = max(target, limit_price)

        if target <= 0:
            self.log.error(f"Cannot determine target price: computed non-positive price {target}")
            return None

        return instrument.make_price(target)

    def _is_aggressive(self, sequence: LimitChaserSequence) -> bool:
        if sequence.settings.aggressive_after_secs is None:
            return False

        return self.clock.timestamp_ns() >= sequence.started_at_ns + int(
            sequence.settings.aggressive_after_secs * 1_000_000_000,
        )

    def _should_reprice(
        self,
        instrument: Instrument,
        current_price: Price,
        target_price: Price,
        min_delta_ticks: int,
    ) -> bool:
        if current_price == target_price:
            return False

        if min_delta_ticks == 0:
            return True

        tick_size = instrument.price_increment.as_decimal()
        min_delta = tick_size * min_delta_ticks
        price_delta = abs(target_price.as_decimal() - current_price.as_decimal())
        return price_delta >= min_delta

    def _child_display_qty(self, primary_order: Order, quantity: Quantity) -> Quantity | None:
        display_qty = getattr(primary_order, "display_qty", None)
        if display_qty is None:
            return None
        if display_qty <= quantity:
            return display_qty
        return quantity

    def _working_order(self, sequence: LimitChaserSequence) -> Order | None:
        if sequence.working_order_id is None:
            return None
        return self.cache.order(sequence.working_order_id)

    def _cleanup_sequence(self, primary_order_id: ClientOrderId) -> None:
        sequence = self._sequences.pop(primary_order_id, None)
        if sequence is None:
            return

        if primary_order_id.value in self.clock.timer_names:
            self.clock.cancel_timer(primary_order_id.value)

        instrument_sequences = self._instrument_sequences.get(sequence.instrument_id)
        if instrument_sequences is not None:
            instrument_sequences.discard(primary_order_id)
            if not instrument_sequences:
                self._instrument_sequences.pop(sequence.instrument_id, None)
                if sequence.instrument_id in self._subscribed_instruments:
                    self.unsubscribe_quote_ticks(sequence.instrument_id)
                    self._subscribed_instruments.discard(sequence.instrument_id)

        self.log.info(
            f"Completed limit-chaser execution for {primary_order_id}",
            LogColor.BLUE,
        )

    def _resolve_settings(
        self,
        order: Order,
        instrument: Instrument,
    ) -> LimitChaserSettings:
        exec_params = order.exec_algorithm_params or {}

        follow_offset_ticks = self.config.follow_offset_ticks
        if "follow_offset_ticks" in exec_params:
            follow_offset_ticks = _parse_int(exec_params["follow_offset_ticks"], "follow_offset_ticks")

        aggressive_offset_ticks = self.config.aggressive_offset_ticks
        if "aggressive_offset_ticks" in exec_params:
            aggressive_offset_ticks = _parse_int(
                exec_params["aggressive_offset_ticks"],
                "aggressive_offset_ticks",
            )

        aggressive_after_secs = self.config.aggressive_after_secs
        if "aggressive_after_secs" in exec_params:
            aggressive_after_secs = _parse_float(
                exec_params["aggressive_after_secs"],
                "aggressive_after_secs",
            )

        reprice_interval_ms = self.config.reprice_interval_ms
        if "reprice_interval_ms" in exec_params:
            reprice_interval_ms = _parse_int(exec_params["reprice_interval_ms"], "reprice_interval_ms")

        min_reprice_delta_ticks = self.config.min_reprice_delta_ticks
        if "min_reprice_delta_ticks" in exec_params:
            min_reprice_delta_ticks = _parse_int(
                exec_params["min_reprice_delta_ticks"],
                "min_reprice_delta_ticks",
            )

        max_child_quantity = self.config.max_child_quantity
        if "max_child_quantity" in exec_params:
            max_child_quantity = _parse_decimal(
                exec_params["max_child_quantity"],
                "max_child_quantity",
            )

        PyCondition.is_true(follow_offset_ticks >= 0, "follow_offset_ticks must be >= 0")
        PyCondition.is_true(aggressive_offset_ticks >= 0, "aggressive_offset_ticks must be >= 0")
        PyCondition.is_true(reprice_interval_ms > 0, "reprice_interval_ms must be > 0")
        PyCondition.is_true(
            min_reprice_delta_ticks >= 0,
            "min_reprice_delta_ticks must be >= 0",
        )
        if aggressive_after_secs is not None:
            PyCondition.is_true(aggressive_after_secs > 0, "aggressive_after_secs must be > 0")

        max_child_qty = None
        if max_child_quantity is not None:
            PyCondition.is_true(max_child_quantity > 0, "max_child_quantity must be > 0")
            max_child_qty = instrument.make_qty(max_child_quantity)
            if max_child_qty < instrument.size_increment:
                raise ValueError(
                    f"max_child_quantity {max_child_qty} was smaller than size increment {instrument.size_increment}",
                )
            if instrument.min_quantity and max_child_qty < instrument.min_quantity:
                raise ValueError(
                    f"max_child_quantity {max_child_qty} was smaller than min quantity {instrument.min_quantity}",
                )

        return LimitChaserSettings(
            follow_offset_ticks=follow_offset_ticks,
            aggressive_offset_ticks=aggressive_offset_ticks,
            aggressive_after_secs=aggressive_after_secs,
            max_child_quantity=max_child_qty,
            reprice_interval_ms=reprice_interval_ms,
            min_reprice_delta_ticks=min_reprice_delta_ticks,
        )
