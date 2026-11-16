"""Cross-market set arbitrage over the D/R(/I) markets of one race.

* Overpriced set (always safe): sum of YES bids over any >=2 mutually exclusive outcomes > 1
  -> buy NO on each. Profit per set >= sum(bids) - 1 (at most one outcome can win).
* Underpriced set (exhaustive races only): sum of YES asks over ALL outcomes < 1 -> buy YES on each.
* Exits: holding NO on every leg of a set (an old overpriced arb) and the YES asks now sum <= 1 - margin
  -> buy YES back on every leg (pairs net and pay 1) to realize early and free capital.
  Symmetric for YES sets.
* Capital: a set must return ``min_return_on_capital`` on what it costs, and with ``exit_hurdle`` a held set
  is also closed once its remaining expected return (fair-value payout over what closing pays now) is at
  most the hurdle, even a little below its cost: a NO set over three outcomes costs ~1.97 for a 2-3c profit
  locked until settlement.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from itertools import combinations

from sigbot.core.reasons import TradeReason
from sigbot.core.util import ONE, Mils, sig_floor
from sigbot.market.fair import fair_confidence
from sigbot.market.mapping import Race
from sigbot.market.state import UNKNOWN_QTY, Level
from sigbot.sigapi.models import Side

from .base import Intent, Leg, View
from .exits import HURDLE


@dataclass(frozen=True, slots=True)
class ArbConfig:
    min_edge: float = 20.0
    """Minimum profit per set, mils."""
    max_sets: int = 5_000
    depth_fraction: float = 0.5
    """Take at most this share of each level: at the open, arbs sized to the whole visible level kept losing
    the mispriced leg to faster traders and filling only the fairly priced one."""
    max_book_age_s: float = 5.0
    """Every leg's book must be this fresh to open a set (the risk gate's 10 s is too stale for two legs)."""
    exit_margin: float = 0.0
    """Exits trigger when the reverse sum beats 1 by this much (mils)."""
    non_exhaustive_exit_margin: float = 15.0
    min_return_on_capital: float = 0.0
    """Profit per set over what the set costs, at least (0 = ``min_edge`` alone). 2026-10-02: arb held 15k in
    sets earning 1.9% to settlement, 11k of it one GOV-RI NO set for ~160, while snipes were turned away."""
    exit_hurdle: float = 0.0
    """Close a held set once (expected payout - what closing pays) / what closing pays is at most this
    (0 = only when closing beats settlement). Expected payout from fair values both sources agree on."""
    exit_hurdle_max_loss: float = 0.0
    """Mils per leg below its cost a hurdle close may realize (a set of k legs: k times this), as for a
    single held share."""


class _Walk:
    """Walk best levels of several books in lock-step while the per-set edge stays >= min_edge (and
    ``accept`` holds for the prices, when given)."""

    @staticmethod
    def run(
        books: Sequence[Sequence[Level]],
        edge_of: Callable[[list[Mils]], float],
        min_edge: float,
        max_sets: int,
        fraction: float = 1.0,
        accept: Callable[[list[Mils]], bool] | None = None,
    ) -> tuple[int, float, list[Mils]]:
        def share(qty: float) -> float:
            return qty if qty == UNKNOWN_QTY else qty * fraction

        cursors = [0] * len(books)
        remaining = [share(lv[0].qty) if lv else 0.0 for lv in books]
        qty_total = 0
        profit_total = 0.0
        worst: list[Mils] = [lv[0].price if lv else 0 for lv in books]
        while qty_total < max_sets:
            if any(cursors[i] >= len(books[i]) for i in range(len(books))):
                break
            prices = [books[i][cursors[i]].price for i in range(len(books))]
            edge = edge_of(prices)
            if edge < min_edge or (accept is not None and not accept(prices)):
                break
            if any(r == UNKNOWN_QTY for r in remaining):
                break
            step = int(min(min(remaining), max_sets - qty_total))
            if step <= 0:
                break
            qty_total += step
            profit_total += step * edge
            worst = prices
            for i in range(len(books)):
                remaining[i] -= step
                if remaining[i] <= 0:
                    cursors[i] += 1
                    remaining[i] = share(books[i][cursors[i]].qty) if cursors[i] < len(books[i]) else 0.0
        return qty_total, profit_total, worst


class ArbStrategy:
    name = "arb"

    def __init__(self, cfg: ArbConfig | None = None) -> None:
        self.cfg = cfg or ArbConfig()

    def evaluate(self, race: Race, view: View, now: float) -> list[Intent]:
        if "label_risk" in race.flags:
            return []  # the settlement label is uncertain, so "exactly one outcome pays" is not guaranteed
        exes = [o.sig_exchange_id for o in race.outcomes]
        books = {ex: view.book(ex) for ex in exes}
        if any(b.settled or not b.depth_known for b in books.values()):
            return []
        out: list[Intent] = []
        exit_intent = self._exit(race, exes, books, view, now)
        if exit_intent is not None:
            return [exit_intent]
        if any(now - b.ts > self.cfg.max_book_age_s for b in books.values()):
            return []
        best: Intent | None = None
        for size in range(2, len(exes) + 1):
            for subset in combinations(exes, size):
                cand = self._overpriced(race, subset, books, now)
                if cand is not None and (best is None or cand.expected_profit > best.expected_profit):
                    best = cand
        if best is not None:
            out.append(best)
        if race.exhaustive:
            under = self._underpriced(race, exes, books, now)
            if under is not None:
                out.append(under)
        return out

    def _overpriced(self, race: Race, subset: Sequence[str], books: dict, now: float) -> Intent | None:
        bids = [books[ex].bids for ex in subset]
        if any(not b for b in bids) or sum(b[0].price for b in bids) - ONE < self.cfg.min_edge:
            return None
        cfg = self.cfg
        roc = cfg.min_return_on_capital
        qty, profit, worst = _Walk.run(
            bids,
            lambda p: sum(p) - ONE,
            cfg.min_edge,
            cfg.max_sets,
            cfg.depth_fraction,
            # NO on each leg costs 1 - bid: the set costs len * 1 - sum(bids) for a profit of sum(bids) - 1
            accept=lambda p: sum(p) - ONE >= roc * (len(p) * ONE - sum(p)),
        )
        if qty <= 0:
            return None
        legs = tuple(Leg(ex, Side.NO, sig_floor(ONE - w), qty) for ex, w in zip(subset, worst, strict=True))
        return Intent(
            self.name,
            race.key,
            legs,
            profit / qty,
            TradeReason.CROSS_MARKET_ARB,
            f"overpriced set {'+'.join(subset)}: bids sum {sum(b[0].price for b in bids)}",
            now,
        )

    def _underpriced(self, race: Race, exes: Sequence[str], books: dict, now: float) -> Intent | None:
        asks = [books[ex].asks for ex in exes]
        if any(not a for a in asks) or ONE - sum(a[0].price for a in asks) < self.cfg.min_edge:
            return None
        cfg = self.cfg
        roc = cfg.min_return_on_capital
        qty, profit, worst = _Walk.run(
            asks,
            lambda p: ONE - sum(p),
            cfg.min_edge,
            cfg.max_sets,
            cfg.depth_fraction,
            accept=lambda p: ONE - sum(p) >= roc * sum(p),  # the YES set costs sum(asks) for 1 - sum(asks)
        )
        if qty <= 0:
            return None
        legs = tuple(Leg(ex, Side.YES, sig_floor(w), qty) for ex, w in zip(exes, worst, strict=True))
        return Intent(
            self.name,
            race.key,
            legs,
            profit / qty,
            TradeReason.CROSS_MARKET_ARB,
            f"underpriced set: asks sum {sum(a[0].price for a in asks)}",
            now,
        )

    def _exit(self, race: Race, exes: Sequence[str], books: dict, view: View, now: float) -> Intent | None:
        holdings = {ex: view.holding(ex) for ex in exes}
        no_legs = [ex for ex, h in holdings.items() if h is not None and h.qty < 0]
        yes_legs = [ex for ex, h in holdings.items() if h is not None and h.qty > 0]
        margin = self.cfg.exit_margin if race.exhaustive else self.cfg.non_exhaustive_exit_margin
        if len(no_legs) >= 2:
            asks = [books[ex].asks for ex in no_legs]
            if all(asks):
                cap = int(min(abs(holdings[ex].qty) for ex in no_legs))  # type: ignore[union-attr]
                if ONE - sum(a[0].price for a in asks) >= margin:
                    qty, profit, worst = _Walk.run(asks, lambda p: ONE - sum(p), margin, cap)
                    if qty > 0:
                        legs = tuple(
                            Leg(ex, Side.YES, sig_floor(w), qty) for ex, w in zip(no_legs, worst, strict=True)
                        )
                        return Intent(
                            "arb_exit",
                            race.key,
                            legs,
                            profit / qty,
                            TradeReason.INVENTORY_REBALANCE,
                            "close NO set early",
                            now,
                            reduces_risk=True,
                        )
                if (
                    close := self._hurdle_close(race, no_legs, asks, view, cap, now, no_set=True)
                ) is not None:
                    return close
        if len(yes_legs) >= 2:
            bids = [books[ex].bids for ex in yes_legs]
            if all(bids):
                cap = int(min(holdings[ex].qty for ex in yes_legs))  # type: ignore[union-attr]
                if sum(b[0].price for b in bids) - ONE >= margin:
                    qty, profit, worst = _Walk.run(bids, lambda p: sum(p) - ONE, margin, cap)
                    if qty > 0:
                        legs = tuple(
                            Leg(ex, Side.NO, sig_floor(ONE - w), qty)
                            for ex, w in zip(yes_legs, worst, strict=True)
                        )
                        return Intent(
                            "arb_exit",
                            race.key,
                            legs,
                            profit / qty,
                            TradeReason.INVENTORY_REBALANCE,
                            "close YES set early",
                            now,
                            reduces_risk=True,
                        )
                return self._hurdle_close(race, yes_legs, bids, view, cap, now, no_set=False)
        return None

    def _hurdle_close(
        self,
        race: Race,
        held: Sequence[str],
        levels: Sequence[Sequence[Level]],
        view: View,
        cap: int,
        now: float,
        *,
        no_set: bool,
    ) -> Intent | None:
        """Close a held set whose remaining expected return is at most ``exit_hurdle``: a NO set by buying YES
        at the asks (each pair pays 1, so a leg returns 1 - ask), a YES set by buying NO at 1 - bid (a leg
        returns the bid). The expected payout is what the legs are worth at fair value; the walk stops where
        closing would realize more than ``exit_hurdle_max_loss`` per leg below the set's cost."""
        cfg = self.cfg
        if cfg.exit_hurdle <= 0 or cap <= 0:
            return None
        fairs = [view.fair(ex) for ex in held]
        trusted = [f.value for f in fairs if f is not None and f.confirmed and fair_confidence(f) >= 1.0]
        if len(trusted) != len(held):
            return None  # a shaky fair value is no reason to unwind a set
        cost = sum(h.avg_cost for ex in held if (h := view.holding(ex)) is not None) * 1000
        k = len(held)
        expected = sum(ONE - v for v in trusted) if no_set else float(sum(trusted))

        def proceeds(p: list[Mils]) -> float:
            return k * ONE - sum(p) if no_set else float(sum(p))

        qty, profit, worst = _Walk.run(
            levels,
            lambda p: proceeds(p) - cost,
            -cfg.exit_hurdle_max_loss * k,
            cap,
            accept=lambda p: (1 + cfg.exit_hurdle) * proceeds(p) >= expected,
        )
        if qty <= 0:
            return None
        if no_set:
            legs = tuple(Leg(ex, Side.YES, sig_floor(w), qty) for ex, w in zip(held, worst, strict=True))
        else:
            legs = tuple(Leg(ex, Side.NO, sig_floor(ONE - w), qty) for ex, w in zip(held, worst, strict=True))
        return Intent(
            "arb_exit",
            race.key,
            legs,
            profit / qty,
            TradeReason.INVENTORY_REBALANCE,
            f"{HURDLE} {'NO' if no_set else 'YES'} set: closing pays {proceeds(worst):.0f} a set, worth "
            f"{expected:.0f} at fair, cost {cost:.0f}",
            now,
            reduces_risk=True,
        )
