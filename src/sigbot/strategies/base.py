"""Common strategy types. Every order is a BUY of YES or NO (the engine nets opposite holdings)."""

from __future__ import annotations

from typing import NamedTuple, Protocol

from sigbot.core.reasons import TradeReason
from sigbot.core.util import Mils
from sigbot.market.fair import Fair
from sigbot.market.mapping import Race
from sigbot.market.state import Book, Holding
from sigbot.sigapi.models import Side


class Leg(NamedTuple):
    exchange_id: str
    side: Side
    """Side we BUY."""
    price: Mils
    """Limit in the bought side's price terms (valid SIG tick); marketable limits fill better prices first."""
    quantity: int


class Intent(NamedTuple):
    strategy: str
    race: str
    legs: tuple[Leg, ...]
    edge: float
    """Expected profit per share (single leg) or per set (multi-leg), in mils."""
    reason: TradeReason
    detail: str
    created: float
    reduces_risk: bool = False
    """Exits: allowed even when caps are full or the daily loss limit is hit; still blocked by /halt, paused
    strategies, manual flags, stale books and the rate/error breakers."""

    @property
    def quantity(self) -> int:
        return self.legs[0].quantity

    @property
    def capital(self) -> float:
        return sum(leg.price * leg.quantity for leg in self.legs) / 1000

    @property
    def expected_profit(self) -> float:
        return self.edge * self.quantity / 1000

    def with_quantity(self, quantity: int) -> Intent:
        return self._replace(legs=tuple(leg._replace(quantity=quantity) for leg in self.legs))


class View(Protocol):
    """What strategies may read. Implemented by the engine."""

    def book(self, exchange_id: str) -> Book: ...

    """SIG's book without our own resting orders: what a strategy may take as liquidity."""

    def fair(self, exchange_id: str) -> Fair | None: ...
    def holding(self, exchange_id: str) -> Holding | None: ...
    def quoted(self, exchange_id: str) -> float: ...

    """Shares the flow quoter bought net on this exchange (YES positive, NO negative): its inventory, which
    it cycles itself; 0 without a quoter."""

    def is_quoting(self, exchange_id: str) -> bool: ...

    """Whether the flow quoter has a quote resting on this exchange right now: it is working that market's
    inventory at the venue's own band, so nothing else should dump the position under cost there."""

    def realized(self, exchange_id: str) -> float: ...

    """P&L booked so far on this exchange (SUSQies): a sale below cost may use it up but must not turn the
    market's total negative (the Smart Score counts every market closed at a net loss)."""

    def days_left(self) -> float: ...

    """Days until the tournament's trading ends (at least 1): what a hold still has to earn its return in."""

    @property
    def equity(self) -> float: ...


class Strategy(Protocol):
    name: str

    def evaluate(self, race: Race, view: View, now: float) -> list[Intent]: ...
