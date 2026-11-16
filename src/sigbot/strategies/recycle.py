"""Capital recycling under pressure (the "swap valve").

The deployed cap stops new risk, but at 65% of equity most of the book is lopsided-race NO positions
earning 4-5% to settlement that nothing will ever buy back above fair, while the strategies keep finding
11-15% trades and get told "deployed cap reached" (2,963 times in the first nine hours of 2026-10-02).
Selling a position early forgoes its remaining edge, so it only pays when the capital it frees has a strictly
better use waiting. The valve is therefore demand-driven:

* the trigger is a concrete intent the gate just rejected for the deployed cap;
* a position qualifies only if the intent's return on capital beats the position's forgone rate (remaining
  edge per SUSQie it ties up) by ``margin``, and it can be closed at or above cost (never a loss) at the
  book's best level (sized to its visible depth, or to the position when the size is unknown: a partial
  fill of an exit at or above cost is harmless);
* positions are sold cheapest-to-exit first, and only as much as the intent needs;
* settling fills, hedged sets (arb legs), the intent's own race, manual pauses and recently swapped
  exchanges are left alone, and swaps are rate limited.

Every swap is recorded (``swap`` event: capital freed, edge forgone, expected gain) so the tactic's own P&L
is measurable; ``RECYCLE_MARGIN`` and friends tune it, ``/pause recycle`` switches it off.

The cash reserve (``reserve_frac``, off at 0) uses the same machinery without a trigger: while cash is below
that share of equity, it sells the positions with the least edge left per SUSQie (at most
``reserve_max_rate``, at most ``reserve_max_loss`` under cost), cheapest first, until the cash is back. The
dump catcher's resting rungs are only worth something with cash behind them, and after a catch the reserve
refills from whatever earns least by then: a caught dump that rebounded first.
"""

from __future__ import annotations

import logging
import math
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sigbot.config import RecycleConfig
from sigbot.core.reasons import TradeReason
from sigbot.core.util import Mils, now_mono, sig_floor
from sigbot.discord.alerts import activity_line, cents
from sigbot.market.fair import Fair
from sigbot.market.mapping import Race
from sigbot.market.state import Book, Holding
from sigbot.sigapi.models import Side
from sigbot.strategies.base import Intent, Leg
from sigbot.strategies.exits import CARRY_SIDE_FAIR

if TYPE_CHECKING:
    from sigbot.trading.engine import Engine

log = logging.getLogger(__name__)

STRATEGY = "recycle"
TRIGGER = "deployed cap reached"
"""The gate's rejection reason that opens the valve."""
REFUSED_COOLDOWN_S = 600.0


@dataclass(frozen=True, slots=True)
class Candidate:
    """A position we could close right now at or above cost."""

    exchange_id: str
    race: str
    side: Side
    """Side to BUY to close it (NO closes a YES holding)."""
    price: Mils
    """Limit in the bought side's terms."""
    available: int
    """Shares closable at that price: visible depth, at most the position."""
    cost_per_share: float
    """What a share cost us (SUSQies): the capital each closed share frees."""
    realized_per_share: float
    """Profit booked per closed share (SUSQies): >= 0, or >= -max_loss for the cash reserve."""
    forgone_rate: float
    """Remaining edge given up per SUSQie freed (the position's return to settlement, on top of the exit)."""
    carry: bool = True
    """Whether the held side is a settlement hold (worth >= CARRY_SIDE_FAIR). A competitive position can be
    turned over like a new trade, so its forgone rate counts once per turnover too (see ``plan``)."""


def hedged_sets(holdings: Mapping[str, Holding], race_of: Mapping[str, Race]) -> set[str]:
    """Exchanges whose position is one leg of a same-side set in its race (an arb): closing one leg alone
    gives up the locked value, so the arb exit owns those."""
    by_race: dict[tuple[str, int], list[str]] = {}
    for ex, h in holdings.items():
        race = race_of.get(ex)
        if race is None or h.qty == 0:
            continue
        by_race.setdefault((race.key, 1 if h.qty > 0 else -1), []).append(ex)
    return {ex for legs in by_race.values() if len(legs) >= 2 for ex in legs}


def candidates(
    holdings: Mapping[str, Holding],
    book_of: Callable[[str], Book | None],
    fair_of: Callable[[str], Fair | None],
    race_of: Mapping[str, Race],
    *,
    exclude: Iterable[str] = (),
    max_loss: float = 0.0,
    realized_of: Callable[[str], float] | None = None,
) -> list[Candidate]:
    """Positions closable at or above cost (or at most ``max_loss`` mils under it) into visible depth,
    cheapest-to-exit first. A sale below cost is cut to what the market's booked P&L (``realized_of``)
    covers, so no market's total ever turns negative for the sake of recycling."""
    skip = set(exclude) | hedged_sets(holdings, race_of)
    out: list[Candidate] = []
    for ex, h in holdings.items():
        race = race_of.get(ex)
        if ex in skip or race is None or h.qty == 0:
            continue
        book, fair = book_of(ex), fair_of(ex)
        if book is None or fair is None or book.settled:
            continue
        cost = h.avg_cost * 1000
        if h.qty > 0:
            if not book.bids:
                continue
            level = book.bids[0]
            exit_px, side_fair = level.price, fair.value
            side, price = Side.NO, sig_floor(1000 - level.price)
        else:
            if not book.asks:
                continue
            level = book.asks[0]
            exit_px, side_fair = 1000 - level.price, 1000 - fair.value
            side, price = Side.YES, sig_floor(level.price)
        realized = exit_px - cost
        if realized < -max_loss or cost <= 0:
            continue
        # A level of unknown size (top-of-book poll while SIG's feed is down) is sized to the position: an
        # exit at or above cost that only partly fills is harmless, the rest is cancelled and freed later.
        available = int(abs(h.qty)) if level.qty < 0 else min(int(abs(h.qty)), int(level.qty))
        if realized < 0 and realized_of is not None:
            available = min(available, int(max(0.0, realized_of(ex)) * 1000 / -realized))
        if available <= 0:
            continue
        forgone = max(0.0, side_fair - exit_px)
        out.append(
            Candidate(
                ex,
                race.key,
                side,
                price,
                available,
                cost / 1000,
                realized / 1000,
                forgone / cost,
                carry=side_fair >= CARRY_SIDE_FAIR,
            ),
        )
    out.sort(key=lambda c: c.forgone_rate)
    return out


def plan(
    cands: Iterable[Candidate],
    roc: float,
    needed: float,
    cfg: RecycleConfig,
    *,
    days_left: float | None = None,
    trade_turns: bool = True,
) -> list[tuple[Candidate, int]]:
    """Which candidates to close, and how many shares of each, to free ``needed`` SUSQies for a trade that
    returns ``roc`` on its capital. Cheapest-to-exit first; nothing whose forgone rate the trade does not
    beat by ``cfg.margin``; nothing smaller than ``cfg.min_free``. With ``cfg.turnover_days`` and
    ``days_left``, a return that can be made again counts once per turnover left before settlement: the
    trade's when ``trade_turns`` (a competitive snipe, a round trip at the band), a competitive position's
    forgone rate always (it can be cycled just the same), a settlement hold's never. A 3% round trip the
    quoter makes every two days beats a 13% hold with a month to go; a 10% competitive snipe does not beat
    a 14% competitive position (2026-10-03: it did, and the swaps sold SEN-ME R for HOUSE-FL-22)."""
    turns = 1.0
    if cfg.turnover_days > 0 and days_left is not None:
        turns = max(1.0, days_left / cfg.turnover_days)
    if trade_turns:
        roc *= turns
    out: list[tuple[Candidate, int]] = []
    remaining = needed
    for cand in cands:
        if remaining <= 0:
            break
        forgone = cand.forgone_rate * (1.0 if cand.carry else turns)
        if roc < forgone + cfg.margin:
            continue
        qty = min(cand.available, math.ceil(remaining / cand.cost_per_share))
        frees = qty * cand.cost_per_share
        if qty <= 0 or frees < cfg.min_free:
            continue
        out.append((cand, qty))
        remaining -= frees
    return out if remaining <= 0 or out else []


def return_on_capital(intent: Intent) -> float:
    return intent.expected_profit / intent.capital if intent.capital > 0 else 0.0


class Recycler:
    def __init__(
        self,
        engine: Engine,
        cfg: RecycleConfig,
        race_of: Mapping[str, Race],
        *,
        parties: Mapping[str, str] | None = None,
        on_activity: Callable[[str], None] | None = None,
    ) -> None:
        """``on_activity`` receives one readable line per swap (the Discord activity channel); the exit's
        fill itself is reported by the executor like any other fill."""
        self._engine = engine
        self._cfg = cfg
        self._race_of = dict(race_of)
        self._parties = parties or {}
        self._on_activity = on_activity
        self._last: dict[str, float] = {}
        self._refused: dict[str, float] = {}
        """Trade exchange -> when a swap for it freed capital and the trade was still refused: no new swap
        for that trade for REFUSED_COOLDOWN_S (a partial exit fill frees too little, and the next pass would
        sell more carry for the same trade; 2026-10-04 04:15-04:40, SEN-DE)."""
        self._recent: deque[float] = deque()
        self._reserve_at = float("-inf")
        self.swaps = 0
        self.freed = 0.0
        self.forgone = 0.0
        """Sum over swaps of edge given up (SUSQies), to set against the gains of the trades let through."""

    def _room(self, now: float) -> int:
        """Exits still allowed this minute."""
        while self._recent and self._recent[0] <= now - 60.0:
            self._recent.popleft()
        return self._cfg.max_per_minute - len(self._recent)

    def _candidates(self, now: float, exclude: set[str], max_loss: float = 0.0) -> list[Candidate]:
        engine = self._engine
        exclude = (
            exclude
            | set(engine.risk.manual_paused)
            | engine.account.settling_exchanges()
            | {ex for ex, at in self._last.items() if now - at < self._cfg.cooldown_s}
            # markets the quoter is working: their inventory is its (that of a quote filled before a
            # restart included), cycled at the inside, not sold off for a snipe elsewhere
            | {ex for ex in engine.account.holdings if engine.is_quoting(ex)}
        )
        return candidates(
            engine.account.holdings,
            lambda ex: engine.book(ex) if ex in engine.books.books else None,  # our own orders taken out
            engine.fair,
            self._race_of,
            exclude=exclude,
            max_loss=max_loss,
            realized_of=engine.realized,
        )

    def propose(self, intent: Intent, race: Race, equity: float) -> list[Intent]:
        """Exits that would make room for ``intent`` (just rejected for the deployed cap), or nothing."""
        engine, cfg = self._engine, self._cfg
        if intent.reduces_risk or STRATEGY in engine.risk.paused_strategies:
            return []
        roc = return_on_capital(intent)
        cap = engine.risk.tier_def().deployed_frac * equity
        needed = intent.capital - max(0.0, cap - engine.account.deployed)
        if needed <= 0 or roc <= 0:
            return []
        now = now_mono()
        if (
            self._room(now) <= 0
            or now - self._refused.get(intent.legs[0].exchange_id, -1e9) < REFUSED_COOLDOWN_S
        ):
            return []
        # a trade on the favourite's side of a lopsided market is carry like the positions it would replace
        # (nobody buys that back above fair within days): it must beat them on its return to settlement,
        # with the plain margin. Only trades that can turn over (competitive races, the band) count once
        # per turnover. Without this, 145 swaps in 75 minutes sold 9% carry to buy 8.8% carry (2026-10-03).
        carry = all(self._carry_side(leg.exchange_id, leg.side) for leg in intent.legs)
        chosen = plan(
            self._candidates(now, {o.sig_exchange_id for o in race.outcomes}, cfg.swap_max_loss),
            roc,
            needed,
            cfg,
            days_left=engine.days_left(),
            trade_turns=not carry,
        )
        why = f"{intent.strategy} {intent.race} at"
        return self._exits(
            chosen,
            now,
            (f"{why} {roc:.1%}", f"{why} {roc:.0%}"),
            {"for_strategy": intent.strategy, "for_race": intent.race, "roc": roc},
        )

    def refused(self, intent: Intent) -> None:
        """The engine's report that a swap for ``intent`` did not get it through the gate."""
        self._refused[intent.legs[0].exchange_id] = now_mono()

    def _carry_side(self, exchange_id: str, side: Side) -> bool:
        """Whether buying ``side`` here is a settlement hold: the side is worth at least CARRY_SIDE_FAIR."""
        fair = self._engine.fair(exchange_id)
        if fair is None:
            return True
        side_fair = fair.value if side is Side.YES else 1000 - fair.value
        return side_fair >= CARRY_SIDE_FAIR

    def reserve_exits(self, equity: float) -> list[Intent]:
        """Exits that bring the cash back up to ``reserve_frac`` of equity: the positions with the least edge
        left per SUSQie (at most ``reserve_max_rate``), at most ``reserve_max_loss`` under cost, cheapest
        first, within the per-minute swap limit. At most one pass every ``reserve_every_s``; nothing while
        the valve is paused."""
        engine, cfg = self._engine, self._cfg
        now = now_mono()
        if cfg.reserve_frac <= 0 or STRATEGY in engine.risk.paused_strategies:
            return []
        if now - self._reserve_at < cfg.reserve_every_s:
            return []
        self._reserve_at = now
        needed = cfg.reserve_frac * equity - engine.account.cash
        room = self._room(now)
        if needed <= 0 or room <= 0:
            return []
        cands = self._candidates(now, set(), max_loss=cfg.reserve_max_loss)
        # a competitive position's forgone rate counts once per turnover (see plan): the reserve sells
        # settlement holds, not positions the band can cycle
        chosen = plan(
            cands,
            cfg.reserve_max_rate + cfg.margin,
            needed,
            cfg,
            days_left=self._engine.days_left(),
            trade_turns=False,
        )[:room]
        why = "the cash reserve"
        return self._exits(chosen, now, (why, why), {"for_strategy": "reserve"}, head="Reserve")

    def _exits(
        self,
        chosen: list[tuple[Candidate, int]],
        now: float,
        why: tuple[str, str],
        data: dict[str, object],
        *,
        head: str = "Swap",
    ) -> list[Intent]:
        """Exit intents closing ``chosen``, each recorded (``swap`` event with ``data``, log line, activity
        line) as freeing cash for ``why`` (long form for the log, short for the activity channel)."""
        out: list[Intent] = []
        for cand, qty in chosen:
            frees, forgone = qty * cand.cost_per_share, qty * cand.forgone_rate * cand.cost_per_share
            booked = qty * cand.realized_per_share
            self._last[cand.exchange_id] = now
            self._recent.append(now)
            self.swaps += 1
            self.freed += frees
            self.forgone += forgone
            detail = (
                f"{head.lower()}: frees {frees:,.0f} for {why[0]} "
                f"(forgoing {cand.forgone_rate:.1%}, booking {booked:+,.0f})"
            )
            self._engine.rec.event(
                "swap",
                f"{cand.race} {cand.exchange_id}: {detail}",
                data={
                    "exchange_id": cand.exchange_id,
                    **data,
                    "forgone_rate": cand.forgone_rate,
                    "frees": frees,
                    "forgone": forgone,
                    "realized": booked,
                },
            )
            log.info("%s %s: %s", cand.race, cand.exchange_id, detail)
            if self._on_activity is not None:
                held = "YES" if cand.side is Side.NO else "NO"  # closing buys the opposite side
                party = self._parties.get(cand.exchange_id, cand.exchange_id)
                self._on_activity(
                    activity_line(
                        STRATEGY,
                        "SELL",
                        f"{cand.race} {party}",
                        f"{qty:,} {held} @ {cents(1000 - cand.price)}",
                        f"cost {cents(cand.cost_per_share * 1000)}",
                        f"{head.lower()}: frees {frees:,.0f} for {why[1]}, forgoing {cand.forgone_rate:.1%}",
                        "sent",
                        pnl=booked,
                    ),
                )
            out.append(
                Intent(
                    STRATEGY,
                    cand.race,
                    (Leg(cand.exchange_id, cand.side, cand.price, qty),),
                    cand.realized_per_share * 1000,
                    TradeReason.INVENTORY_REBALANCE,
                    detail,
                    now,
                    reduces_risk=True,
                ),
            )
        return out
