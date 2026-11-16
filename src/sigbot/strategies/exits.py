"""Exit positions back into the market to lock profit and free capital.

Exits never realize a loss (bar the bounded ``hurdle`` rule below): the price we get must be at or above our
average cost. Beyond that, two rules:
* at/above fair (within ``slack``): the market pays the whole expected edge, take it anywhere;
* in competitive races (fair inside ``competitive``) the exit also fires once the market pays at least
  ``lock_fraction`` of the expected edge (fair - cost). A ~55/45 race held to settlement is a coin flip for
  the resolved-only win rate and locks capital for weeks; closing it books the market as a win and recycles
  cash. Lopsided races are held toward settlement (likely win, and settlement adds no volume for ROI) unless
  ``recycle_lopsided`` is on: then they too go at cost + ``recycle_margin``, to put the capital to work.

The ``hurdle`` rule (off at 0) overrides the cost floor: a share whose remaining return to settlement, at the
price the market pays now, is at most ``hurdle`` of the cash its sale frees is sold, even a little below cost
(``hurdle_max_loss``). What it cost is sunk; the final balance only sees what the share is worth and what the
freed cash earns next. Only on a confirmed, fully trusted fair value. ``hurdle_per_day`` is the same rule per
day of trading left: a 13% return with a month to go is 0.4% a day, which the quoter and the spike rungs
beat in an hour, so capital is cycled instead of parked (2026-10-03: 84% of the book sat in such holds).
A sale below cost never turns a market's booked P&L negative (``View.realized``): the Smart Score's win rate
counts every market closed at a net loss, so a losing sale is cut to what the market's earlier gains cover.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from sigbot.core.reasons import TradeReason
from sigbot.core.util import Mils, sig_floor
from sigbot.market.fair import Fair, fair_confidence
from sigbot.market.mapping import Race
from sigbot.market.state import Book, Holding, Level
from sigbot.sigapi.models import Side

from .base import Intent, Leg, View

log = logging.getLogger(__name__)

RECYCLE = "recycle lopsided"
"""Detail prefix of an exit that sells a settlement hold at cost or better (``recycle_lopsided``)."""
HURDLE = "low return"
"""Detail prefix of an exit that frees capital earning less than the hurdle to settlement (``hurdle``)."""
CARRY_SIDE_FAIR = 800
"""A held side worth at least this (mils) is a settlement hold ("carry"): the capital hurdle is about such
holds. A 70c share worth 72c is not one: its 2.8% to settlement comes with a coin flip's variance and its
value is the flow it was bought from (2026-10-03: the hurdle sold the quoter's SEN-MI D inventory under cost
as "low return")."""
HURDLE_ENTRY_MARGIN = 0.03
"""New positions must return at least the exit hurdle plus this on their capital: one bought at the hurdle
would be sold straight back across the spread by the hurdle exit."""


def entry_floor(hurdle: float) -> float:
    """Least return on capital a new position needs so that the hurdle exit leaves it alone."""
    return hurdle + HURDLE_ENTRY_MARGIN if hurdle > 0 else 0.0


def remaining_return(exit_px: float, side_fair: float) -> float:
    """What a held share still earns to settlement per unit of the cash selling it now would free (prices
    in the held side's terms, mils)."""
    return (side_fair - exit_px) / exit_px if exit_px > 0 else float("inf")


@dataclass(frozen=True, slots=True)
class ExitConfig:
    slack: float = 10.0
    """Accept up to this many mils away from fair (covers the spread). Profitability is checked separately."""
    lock_fraction: float = 0.5
    """Competitive races: exit once the market pays this share of the expected edge (fair - cost); the rest
    of the edge is given up for a booked win and freed capital. With the DMM 8c wide, 0.5 fires on its own
    quotes once it is centred on fair."""
    competitive: tuple[float, float] = (300.0, 700.0)
    """Fair-value band (mils, exclusive) where the lock exit applies."""
    recycle_lopsided: bool = False
    """Outside the competitive band, exit whenever the market pays at least cost + ``recycle_margin``: the
    3-5c of settlement edge is given up to free capital that the quoter turns over daily (2026-10-02: 66k
    of the book was lopsided NOs earning 5.8% to Nov 4, exitable at cost, and not cash on election night)."""
    recycle_margin: float = 0.0
    """Mils above cost a recycle exit must realize."""
    hurdle: float = 0.0
    """Sell a share once its remaining return to settlement is at most this share of the cash its sale frees
    (0 = off). 2026-10-02: 84% of the book sat in lopsided positions with ~4.7% left to Nov 4 (75k earning
    ~100 a day) while the deployed cap turned away 8-15% snipes that SIG let us sell again within hours."""
    hurdle_max_loss: float = 0.0
    """Mils below cost a hurdle exit may sell at. A market closed at a loss counts against the Smart Score win
    rate, and a fair value that is briefly wrong must not dump a position far below what it cost."""
    hurdle_per_day: float = 0.0
    """Sell a share once its remaining return to settlement per day of trading left is at most this (0 =
    off); e.g. 0.01: a hold must still earn 1% a day of the cash its sale frees to be kept."""
    unwind_exchanges: tuple[str, ...] = ()
    """A deliberate liquidation (user-approved): positions on these exchanges (or, with ``carry``, every
    position the venue's band does not pay cost for, per ``is_carry``) are sold at the inside even under
    cost, down to ``unwind_max_loss``, once ``unwind_after`` (Unix time) has passed. Until then the ladder's
    flips rest at cost or better, so only what did not fill that way takes the loss."""
    unwind_after: float = 0.0
    unwind_max_loss: float = 0.0
    """Mils under cost an unwind exit may sell at."""


UNWIND = "unwind"
"""Detail prefix of an unwind exit (``ExitConfig.unwind_exchanges``)."""
CARRY = "carry"
"""``unwind_exchanges`` entry meaning every position the venue will not pay cost for."""
UNWIND_WINDOW_S = 120.0
"""Seconds after the unwind first applies in which positions can still join it (every race is evaluated
within a few seconds); anything opened after that is a new trade, not part of the liquidation."""


class ExitStrategy:
    name = "exits"

    def __init__(
        self,
        cfg: ExitConfig | None = None,
        *,
        is_carry: Callable[[str, bool, float], bool] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """``is_carry(exchange_id, held_yes, cost_mils)``: whether the venue's band pays less than cost for
        a position (``RiskGate.trapped``), for an unwind of ``carry``."""
        self.cfg = cfg or ExitConfig()
        self._is_carry = is_carry
        self._clock = clock
        self._unwind_armed: float | None = None
        """When the unwind first applied; positions qualify only within UNWIND_WINDOW_S of it."""
        self._unwind_targets: set[str] = set()
        """The positions the unwind took on: those held when it started. Nothing opened later."""

    def unwinding(self, exchange_id: str, holding: Holding) -> bool:
        """Whether this position is in the deliberate liquidation and its time has come. One-shot: the
        unwind takes on the positions it finds in its first ``UNWIND_WINDOW_S`` and never any opened later.
        On 2026-10-04 a standing ``carry`` rule sold every position the snipes opened at the band within
        seconds, ~3,000 times, for -15k: the unwind is a liquidation, not a strategy."""
        cfg = self.cfg
        if not cfg.unwind_exchanges:
            return False
        now = self._clock()
        if now < cfg.unwind_after:
            return False
        if exchange_id in self._unwind_targets:
            return True
        if self._unwind_armed is None:
            self._unwind_armed = now
        if now - self._unwind_armed > UNWIND_WINDOW_S:
            return False
        listed = exchange_id in cfg.unwind_exchanges or (
            CARRY in cfg.unwind_exchanges
            and self._is_carry is not None
            and self._is_carry(exchange_id, holding.qty > 0, holding.avg_cost * 1000)
        )
        if listed:
            self._unwind_targets.add(exchange_id)
        return listed

    def evaluate(self, race: Race, view: View, now: float) -> list[Intent]:
        out: list[Intent] = []
        held = {
            o.sig_exchange_id: h
            for o in race.outcomes
            if (h := view.holding(o.sig_exchange_id)) is not None and h.qty != 0
        }
        # Same-side holdings on two or more outcomes are a set (an arb): its value is locked across the legs,
        # and closing one leg at its own cost would give that up. Sets are closed only by the arb exit.
        hedged: set[str] = set()
        for sign in (1, -1):
            legs = [ex for ex, h in held.items() if h.qty * sign > 0]
            if len(legs) >= 2:
                hedged.update(legs)
        for ex, holding in held.items():
            book = view.book(ex)
            if book.settled:
                continue
            if self.unwinding(ex, holding):
                out.extend(self._unwind(race, ex, holding, book, now))
                continue
            if ex in hedged:
                continue
            fair = view.fair(ex)
            if fair is None:
                continue
            # the flow quoter's own inventory is left to its asks and bids (it cycles at the inside; an
            # exit at cost here would only take the position away for nothing)
            qty = holding.qty
            quoted = view.quoted(ex)
            if quoted * qty > 0:
                qty = (abs(qty) - min(abs(qty), abs(quoted))) * (1 if qty > 0 else -1)
                if qty == 0:
                    continue
            days = view.days_left()
            # the quoter is working this market's inventory out at the venue's band (its asks are the exit,
            # and it may be buying the very side we hold, under its worth): while it quotes, the capital
            # hurdle stays out and nothing is sold under cost; the profit-taking exits still fire
            quoting = view.is_quoting(ex)
            cushion = 0.0 if quoting else view.realized(ex)
            args = (fair, holding.avg_cost, now, days, cushion, not quoting)
            if qty > 0:
                out.extend(self._exit_yes(race, ex, qty, book.bids, *args))
            else:
                out.extend(self._exit_no(race, ex, qty, book.asks, *args))
        return out

    def _unwind(self, race: Race, ex: str, holding: Holding, book: Book, now: float) -> list[Intent]:
        """Sell at the inside whatever it pays down to cost - ``unwind_max_loss``: the cushion rule does not
        apply (the loss is the point, and approved), nor does any fair-value rule."""
        qty, cost = holding.qty, holding.avg_cost * 1000
        if qty > 0:
            if not book.bids:
                return []
            top = book.bids[0]
            pays, leg = top.price, Leg(ex, Side.NO, sig_floor(1000 - top.price), 0)
        else:
            if not book.asks:
                return []
            top = book.asks[0]
            pays, leg = 1000 - top.price, Leg(ex, Side.YES, sig_floor(top.price), 0)
        if pays < cost - self.cfg.unwind_max_loss:
            return []
        exit_qty = min(int(abs(qty)), int(top.qty) if top.qty >= 0 else int(abs(qty)))
        if exit_qty <= 0:
            return []
        held = "YES" if qty > 0 else "NO"
        return [
            Intent(
                self.name,
                race.key,
                (leg._replace(quantity=exit_qty),),
                pays - cost,
                TradeReason.INVENTORY_REBALANCE,
                f"{UNWIND} {held} {exit_qty} @ {pays} (cost {cost:.0f}, {(pays - cost) / 10:+.1f}c)",
                now,
                reduces_risk=True,
            ),
        ]

    def _should_exit(
        self,
        exit_px: Mils,
        cost: float,
        side_fair: float,
        fair: Fair,
        days_left: float,
        hurdle_ok: bool = True,
    ) -> str:
        """Which rule fires ("" = hold): ``exit_px`` (what the market pays per share), ``cost`` and
        ``side_fair`` are in the held side's price terms (mils). The capital hurdle applies to settlement
        holds only (``CARRY_SIDE_FAIR``), and only when ``hurdle_ok``."""
        cfg = self.cfg
        lo, hi = cfg.competitive
        competitive = lo < fair.value < hi
        if exit_px >= cost:
            if exit_px >= side_fair - cfg.slack:
                return "exit"  # at/above fair: the market pays the whole edge
            expected = side_fair - cost
            if competitive and expected > 0 and exit_px - cost >= cfg.lock_fraction * expected:
                return "exit"
        if (
            hurdle_ok
            and side_fair >= CARRY_SIDE_FAIR
            and self._below_hurdle(exit_px, cost, side_fair, fair, days_left)
        ):
            return HURDLE  # the cash earns more cycling than this share does waiting for settlement
        if exit_px < cost:
            return ""  # would realize a loss: keep holding
        if not competitive and cfg.recycle_lopsided and exit_px - cost >= cfg.recycle_margin:
            return RECYCLE  # a settlement hold sold at cost or better to put the capital to work
        return ""

    def _below_hurdle(
        self,
        exit_px: Mils,
        cost: float,
        side_fair: float,
        fair: Fair,
        days_left: float,
    ) -> bool:
        cfg = self.cfg
        if not (cfg.hurdle > 0 or cfg.hurdle_per_day > 0):
            return False
        if not (fair.confirmed and fair_confidence(fair) >= 1.0 and exit_px >= cost - cfg.hurdle_max_loss):
            return False
        left = remaining_return(exit_px, side_fair)
        return (cfg.hurdle > 0 and left <= cfg.hurdle) or (
            cfg.hurdle_per_day > 0 and left / max(1.0, days_left) <= cfg.hurdle_per_day
        )

    @staticmethod
    def _affordable(exit_qty: int, exit_px: Mils, cost: float, cushion: float) -> int:
        """Shares that may be sold at ``exit_px``: all of them at or above cost, else only as many as the
        market's booked P&L (``cushion``) keeps from turning its total negative."""
        if exit_px >= cost:
            return exit_qty
        return min(exit_qty, int(max(0.0, cushion) * 1000 / (cost - exit_px)))

    def _exit_yes(
        self,
        race: Race,
        ex: str,
        qty: float,
        bids: list[Level],
        fair: Fair,
        avg_cost: float,
        now: float,
        days_left: float = 31.0,
        cushion: float = 0.0,
        hurdle_ok: bool = True,
    ) -> list[Intent]:
        if not bids:
            return []
        bid = bids[0]
        cost = avg_cost * 1000
        rule = self._should_exit(bid.price, cost, fair.value, fair, days_left, hurdle_ok)
        if not rule:
            return []
        exit_qty = min(int(qty), int(bid.qty) if bid.qty >= 0 else int(qty))
        exit_qty = self._affordable(exit_qty, bid.price, cost, cushion)
        if exit_qty <= 0:
            return []
        return [
            Intent(
                self.name,
                race.key,
                (Leg(ex, Side.NO, sig_floor(1000 - bid.price), exit_qty),),
                bid.price - cost,
                TradeReason.EXIT_RISK if fair.value < bid.price else TradeReason.INVENTORY_REBALANCE,
                f"{rule} YES {exit_qty} @ {bid.price} (fair {fair.value}, cost {cost:.0f})",
                now,
                reduces_risk=True,
            ),
        ]

    def _exit_no(
        self,
        race: Race,
        ex: str,
        qty: float,
        asks: list[Level],
        fair: Fair,
        avg_cost: float,
        now: float,
        days_left: float = 31.0,
        cushion: float = 0.0,
        hurdle_ok: bool = True,
    ) -> list[Intent]:
        if not asks:
            return []
        ask = asks[0]
        # avg_cost is what a NO share cost us; buying YES at ``ask`` nets the pair for 1, so the exit pays
        # 1 - ask per NO share and realizes 1 - avg_cost - ask.
        cost, no_exit_price, no_fair = avg_cost * 1000, 1000 - ask.price, 1000 - fair.value
        rule = self._should_exit(no_exit_price, cost, no_fair, fair, days_left, hurdle_ok)
        if not rule:
            return []
        exit_qty = min(int(-qty), int(ask.qty) if ask.qty >= 0 else int(-qty))
        exit_qty = self._affordable(exit_qty, no_exit_price, cost, cushion)
        if exit_qty <= 0:
            return []
        return [
            Intent(
                self.name,
                race.key,
                (Leg(ex, Side.YES, sig_floor(ask.price), exit_qty),),
                no_exit_price - cost,
                TradeReason.EXIT_RISK if fair.value > ask.price else TradeReason.INVENTORY_REBALANCE,
                f"{rule} NO {exit_qty} @ {ask.price} (fair {fair.value}, cost {cost:.0f})",
                now,
                reduces_risk=True,
            ),
        ]
