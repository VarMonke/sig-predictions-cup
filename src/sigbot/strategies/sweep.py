"""Sweep catching: rest limit orders far from fair value, where market orders that sweep a whole book land.

In the first 100 minutes of trading (2026-10-01) ~1.9M SUSQies changed hands at prints more than 15c from
fair: new members' whole-bankroll market orders walked through every resting order up to 0.99 on long shots
and down to 0.005 on favourites, and only ~300 of 3,600 members had traded yet. The crowd now sits at the
extremes (1M+ shares at 0.90-0.99 and 0.005-0.10), and the zone between the near-fair quotes and that crowd
is empty. An order one tick inside the crowd's best deep level is first in line for everything that gets past
the near-fair layer, at +0.8 per share for ~0.1 of capital. Penny-jumping the crowd is legal and what a bot
does better than people typing orders.

Within an hour of that analysis the gap had filled with other participants' rungs (2-30k shares each at
0.26-0.50 on RI Senate R), so "one tick inside the crowd" is defined against the crowd's WALLS (levels of
``wall_qty`` shares or more): jumping inside every small competitor would walk the primary down to the floor.

Per exchange in scope and per side far enough from fair (``floor``):
* a PRIMARY rung one tick inside the first wall beyond fair +/- ``floor`` (or at the floor when there is no
  wall), big and cheap in collateral: the jackpot catcher for whole-bankroll market orders;
* SECONDARY rungs at fair +/- ``offsets`` (the floor itself, 50c, 70c), smaller, for partial sweeps (takers
  with 0.4-0.5 limits were common), only where they sit between fair and the primary.
Every rung is a BUY limit (NO for the ask side) with its own expiry that SIG enforces. Rungs are renewed
before they expire, moved inward when the crowd undercuts them (at most every ``requote_min_s`` per exchange,
and never outward: staying put keeps time priority), rebuilt once fair moves ``relevel_move``, pulled on
halt, ``/pause sweep``, a manual pause, a settled market or a lost fair value, and placed through
``/orders/batch``. Collateral resting in rungs is capped by ``budget_frac`` of equity and never eats into the
cash the snipes may still deploy under the tier cap; every rung also passes the risk gate like any other
order. Fills come from the account feed; in shadow mode they are simulated from the public tape (a print
beyond a rung fills it), so a dry run shows what the ladder would have caught. The exits strategy sells
filled rungs back near fair.

Dump catching (``roc_levels`` set): whole-bankroll sweeps to 0.99/0.005 stopped after the opening, but on
2026-10-02/03 whales still dumped favourites 4-15% under fair in bursts of 2-3M shares (SEN-NM D, SEN-DE D,
GOV-MI D, SEN-SD R), and the accounts resting bids there made 50-150k per burst. So each lopsided market gets
BUY limits on its favourite's side at the prices that return ``roc_levels`` to settlement at fair
(``value_rungs``): a YES bid on a favourite, a NO bid (YES ask) on a long shot. Rungs are budgeted per race
against the hard loss ceiling as if they all filled, markets are ranked by recent activity (the dumps hit the
busy ones), and value rungs never requote inward (it would chase every fair tick): they move on a
``relevel_move`` fair move or at renewal. Fills are sold back by the exits once SIG pays the hurdle again (the
rebound), else held to settlement at the return they were bought at.

Both geometries run together when ``far`` is on with ``roc_levels``: the value rungs sit at the venue's own
level (the carry trade, expensive), the far rungs (slots ``FAR_SLOT`` and up, ``far_frac`` of the budget) sit
where the spikes land: on 2026-10-03 HOUSE-KY-04 D (fair 2c) printed 553k shares up to 25c and 470k back down
within the hour, GOV-RI R (fair 2c) 564k shares up to 62.5c, and 5-60k shares rest on every tick above the
band, so a spike pays every rung it reaches and reverts within ~30 min. The far rungs ask ``offsets`` above
fair on a long shot (bid below it on a favourite) at ``secondary_size``, with the primary a tick inside the
first wall beyond ``floor``; the quoter sells the inventory back at the band.

Band-relative dump catching (``dump_offsets`` set, 2026-10-04): the rungs are priced off SIG's own level,
the **band** (band.py: the 30-minute median of the venue's mid), not off fair. Fair-priced rungs on the
favourites SIG discounts for days (SEN-DE D at 87.8c against a 98.9c fair) sat above the band and bought
carry: positions marked under cost for a month. A rung ``dump_offsets`` under the band fills only when a
dump goes through the venue's level, and the **flip** (slot ``FLIP_SLOT``) sells every position on a ladder
market back at the better of band - ``flip_margin`` and cost + ``flip_min_profit``: the round trip within
minutes for dump fills, and for carry a resting exit at cost that fills whenever the venue dips to it (the
long shots' asks touch our cost after their spikes). Flips rest no collateral (closing a pair pays out),
come first in every batch, and are marketable when the inside already pays.
"""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING

from sigbot.config import SweepConfig
from sigbot.core.reasons import TradeReason
from sigbot.core.util import (
    SIG_MAX_PRICE,
    SIG_MIN_PRICE,
    SIG_TICK,
    Mils,
    TokenBucket,
    now_mono,
    parse_server_ts,
    sig_ceil,
    sig_floor,
    to_mils,
    utcnow,
    wait_event_or_stop,
)
from sigbot.discord.alerts import activity_line, cents
from sigbot.market.fair import Fair
from sigbot.market.mapping import Kind, Race
from sigbot.market.state import Book, Level
from sigbot.sigapi.models import AccountBatch, Order, RtTrade, Side
from sigbot.strategies.base import Intent, Leg
from sigbot.trading.risk import fair_confidence, race_cap_scale

if TYPE_CHECKING:
    from sigbot.trading.engine import Engine
    from sigbot.trading.executor import Executor

log = logging.getLogger(__name__)

STRATEGY = "sweep"
ALL = "all"
KIND_PRIORITY = {Kind.SEN: 0, Kind.GOV: 1, Kind.CTRL: 2, Kind.HOUSE: 3}
"""Where the sweep flow was at the open: Senate 1.43M, Governor 151k, chamber control 125k, House 41k."""
ACTIVITY_HALF_LIFE_S = 1_800.0
UNKNOWN_HOLD_S = 150.0
"""After a batch whose outcome SIG never confirmed, place nothing on its exchanges for this long: the next
open-orders listing shows what actually rests there and ``_adopt_strays`` takes it over."""
LISTING_LAG_S = 30.0
"""A rung missing from an open-orders listing taken this long after its placement is gone (filled, expired
or cancelled behind our back); a fresher-than-that listing may simply not include it yet."""
STRAY_ROC_SLACK = 0.02
"""Dump catching: an untracked resting buy of the favourite counts as one of our rungs (and is adopted) when
it returns at least the lowest level minus this."""
FAR_SLOT = 10
"""Slot of the far geometry's primary when it runs beside the value rungs (its secondaries follow)."""
FLIP_SLOT = 20
"""Slot of the flip: the resting sell of a market's whole position (see ``flip_rung``)."""
FLIP_RESIZE_MIN = 500
"""A live flip is replaced for a changed position only once the difference is this many shares (and 20%):
each replacement costs a cancel and a placement out of 30 writes a minute."""
FLIP_REPRICE_MIN = 10
"""A live flip is re-priced only once the wanted price differs by this (mils): following the inside tick by
tick would spend two writes per half-cent (HOUSE-VA-05 R, 77 shares, 2026-10-04 shadow run)."""
STRAY_FLIP_SLACK = 5
"""An untracked resting sell of a side we hold is adopted as the flip when it books at least cost minus
this (mils); the flip itself never sells under cost + ``flip_min_profit``."""
EVICT_MAX_PER_PASS = 3
"""Cancels one pass may spend taking the budget back from ``all`` rungs for a named market (writes are
scarce; the named ladder fills in over a few passes rather than in a burst). 2026-10-11: a restart adopted
the previous instance's rungs on the named markets, the ``all`` markets took the whole budget while those
rested, and when they expired the named markets stayed unfunded for good."""
HOLD_SIDE_FAIR = 950
"""With ``hold_frac`` on, a held side worth at least this (mils) is kept to settlement instead of flipped:
NO on a 1-3% long shot, YES on a 97%+ favourite. A 10% long shot's NO is flipped as before."""
EXPIRY_GRACE_S = 3.0
"""A rung this long past its expiry is gone at SIG (clocks and admission latency allowed for): it is placed
again without a cancel."""
CANCELLED_MEMORY_S = 90.0
"""An order the ladder cancelled or saw expire is not adopted back from an open-orders listing for this long
(the listing is taken before the cancel lands, and SIG's read models lag the engine by seconds)."""
ADOPT_MIN_AGE_S = 30.0
"""An open order younger than this is never adopted: a batch answers in seconds under load, and a listing
taken meanwhile shows the quoter's (or our own) new orders before their manager has learnt their ids
(2026-10-03 19:13: six quotes adopted as rungs 130 ms before the quoter's batch response arrived)."""


@dataclass(frozen=True, slots=True)
class Rung:
    side: Side
    """Side we BUY: NO for a rung on the ask side (catches buys), YES on the bid side (catches sells)."""
    yes_price: Mils
    """The rung in YES terms."""
    quantity: int
    slot: int
    """0 = primary (inside the crowd), 1.. = secondary offsets; ``FAR_SLOT`` and up: the far geometry when it
    runs beside the value rungs; ``FLIP_SLOT``: the flip."""
    ref: Mils = 0
    """Flips: the position's average cost in the held side's terms (what the fill is booked against)."""

    @property
    def far(self) -> bool:
        return FAR_SLOT <= self.slot < FLIP_SLOT

    @property
    def flip(self) -> bool:
        return self.slot == FLIP_SLOT

    @property
    def sell_price(self) -> Mils:
        """Flips: what the fill pays per share of the held side (the mirror of the bought side's limit)."""
        return 1000 - self.price

    @property
    def profit(self) -> Mils:
        """Flips: mils booked per share over the position's cost."""
        return self.sell_price - self.ref

    @property
    def price(self) -> Mils:
        """Limit in the bought side's terms (an ask at p is a NO bid at 1 - p)."""
        return self.yes_price if self.side is Side.YES else 1000 - self.yes_price

    @property
    def cost(self) -> float:
        """Collateral the rung locks while resting (SUSQies)."""
        return self.price * self.quantity / 1000

    def edge(self, fair: Mils) -> Mils:
        return self.yes_price - fair if self.side is Side.NO else fair - self.yes_price

    def swept_by(self, print_price: Mils) -> bool:
        """A print at or beyond the rung means a taker went through its level."""
        return print_price >= self.yes_price if self.side is Side.NO else print_price <= self.yes_price


@dataclass(slots=True)
class LiveRung:
    rung: Rung
    order_id: int
    placed: float
    fair_at: Mils
    traded: float = 0.0

    @property
    def remaining(self) -> float:
        return max(0.0, self.rung.quantity - self.traded)

    @property
    def reserve(self) -> float:
        """Collateral the rung still ties up; a flip closes a position and ties up none."""
        return 0.0 if self.rung.flip else self.rung.price * self.remaining / 1000


def without_ours(levels: Sequence[Level], ours: Iterable[LiveRung]) -> list[Level]:
    """One side of SIG's book with our own resting rungs taken out (SIG's books include them): a level at
    our price loses our remaining shares, or disappears when its size is unknown or no bigger than ours."""
    mine: dict[Mils, float] = {}
    for live in ours:
        mine[live.rung.yes_price] = mine.get(live.rung.yes_price, 0.0) + live.remaining
    out: list[Level] = []
    for lv in levels:
        own = mine.get(lv.price)
        if own is None:
            out.append(lv)
        elif lv.qty >= 0 and lv.qty > own + 1e-9:
            out.append(Level(lv.price, lv.qty - own))
    return out


def crowd_best(levels: Sequence[Level], *, beyond: Mils, ask_side: bool, min_qty: float) -> Mils | None:
    """Best price among others' walls in the zone beyond ``beyond`` (>= on the ask side, <= on the bid
    side): levels of at least ``min_qty`` shares (unknown sizes count); smaller orders are competitors'
    rungs, not a crowd to sit inside."""
    best: Mils | None = None
    for lv in levels:
        in_zone = lv.price >= beyond if ask_side else lv.price <= beyond
        if not in_zone or (0 <= lv.qty < min_qty):
            continue
        if best is None or (lv.price < best if ask_side else lv.price > best):
            best = lv.price
    return best


def desired_rungs(
    fair: Fair | None,
    book: Book,
    cfg: SweepConfig,
    *,
    ours: Iterable[LiveRung] = (),
    band: float | None = None,
) -> list[Rung]:
    """The rungs we want on this exchange right now; empty when it should carry none. With ``roc_levels``
    or ``dump_offsets`` set: ``value_rungs``, plus (``far`` on) the far geometry in slots ``FAR_SLOT`` and
    up, a far rung that lands on a value rung's price dropped; without them the far geometry alone, in
    slots 0 and up. The flip is not among them (``flip_rung`` needs the position)."""
    if not (cfg.roc_levels or cfg.dump_offsets):
        return far_rungs(fair, book, cfg, ours=ours)
    out = value_rungs(fair, book, cfg, band=band)
    if cfg.far:
        taken = {(r.side, r.yes_price) for r in out}
        out.extend(
            r
            for r in far_rungs(fair, book, cfg, ours=ours, base=FAR_SLOT)
            if (r.side, r.yes_price) not in taken
        )
    return out


def far_rungs(
    fair: Fair | None,
    book: Book,
    cfg: SweepConfig,
    *,
    ours: Iterable[LiveRung] = (),
    base: int = 0,
) -> list[Rung]:
    """The far-from-fair geometry: a primary (slot ``base``) one tick inside the crowd's first wall beyond
    ``floor`` and secondaries (slots ``base`` + 1..) at ``offsets`` from fair, each side. Empty when the
    book's sizes are unknown (a top-of-book poll hides the crowd and would put the primary at the floor,
    where it stays: rungs never move outward). Each rung stays passive (never crosses the other side) and at
    least ``floor`` from fair."""
    if (
        fair is None
        or not fair.confirmed
        or fair_confidence(fair) < 1.0
        or book.settled
        or not book.depth_known
    ):
        return []
    ours = list(ours)
    bids = without_ours(book.bids, (o for o in ours if o.rung.side is Side.YES))
    asks = without_ours(book.asks, (o for o in ours if o.rung.side is Side.NO))
    best_bid = bids[0].price if bids else None
    best_ask = asks[0].price if asks else None
    out: list[Rung] = []

    def qty(side: Side, yes_price: Mils, shares: int) -> int:
        price = yes_price if side is Side.YES else 1000 - yes_price
        return max(0, min(shares, int(cfg.level_cost * 1000 / price)))

    # ask side: YES asks (NO buys) from fair + floor up to the crowd, costing at most max_rung_cost a share
    lo = fair.value + cfg.floor
    cheap_ask = sig_ceil(1000 - cfg.max_rung_cost)
    if lo <= SIG_MAX_PRICE:
        crowd = crowd_best(asks, beyond=lo, ask_side=True, min_qty=cfg.wall_qty)
        primary = sig_ceil(lo) if crowd is None else max(sig_ceil(lo), crowd - SIG_TICK)
        primary = max(primary, cheap_ask)
        if best_bid is not None:
            primary = max(primary, best_bid + SIG_TICK)
        # the cheap zone may start behind the crowd's wall: a rung there fills only after all of it
        # (joining the wall's own price is still fine, as when a wall sits right at the floor)
        inside = crowd is None or primary <= crowd
        if primary <= SIG_MAX_PRICE and inside and (q := qty(Side.NO, primary, cfg.size)) > 0:
            out.append(Rung(Side.NO, primary, q, base))
            taken = {primary}  # offsets clamped to the price limit would pile up at one price
            for i, off in enumerate(cfg.offsets, start=1):
                level = sig_ceil(fair.value + off)
                if best_bid is not None:
                    level = max(level, best_bid + SIG_TICK)
                # a secondary behind the crowd's wall only fills after all of it: pointless while the zone
                # beyond the band is empty, fine once the book is dense and spikes eat 20 ticks (behind_walls)
                inside = (crowd is None or level < crowd or cfg.behind_walls) and level >= cheap_ask
                if level not in taken and inside and (q := qty(Side.NO, level, cfg.secondary_size)) > 0:
                    taken.add(level)
                    out.append(Rung(Side.NO, level, q, base + i))
    # bid side: YES bids from fair - floor down to the crowd, costing at most max_rung_cost a share
    hi = fair.value - cfg.floor
    cheap_bid = sig_floor(cfg.max_rung_cost)
    if hi >= SIG_MIN_PRICE:
        crowd = crowd_best(bids, beyond=hi, ask_side=False, min_qty=cfg.wall_qty)
        primary = sig_floor(hi) if crowd is None else min(sig_floor(hi), crowd + SIG_TICK)
        primary = min(primary, cheap_bid)
        if best_ask is not None:
            primary = min(primary, best_ask - SIG_TICK)
        inside = crowd is None or primary >= crowd
        if primary >= SIG_MIN_PRICE and inside and (q := qty(Side.YES, primary, cfg.size)) > 0:
            out.append(Rung(Side.YES, primary, q, base))
            taken = {primary}
            for i, off in enumerate(cfg.offsets, start=1):
                level = sig_floor(fair.value - off)
                if best_ask is not None:
                    level = min(level, best_ask - SIG_TICK)
                inside = (crowd is None or level > crowd or cfg.behind_walls) and level <= cheap_bid
                if level not in taken and inside and (q := qty(Side.YES, level, cfg.secondary_size)) > 0:
                    taken.add(level)
                    out.append(Rung(Side.YES, level, q, base + i))
    return [r for r in out if r.cost >= 1.0]


def value_rungs(fair: Fair | None, book: Book, cfg: SweepConfig, *, band: float | None = None) -> list[Rung]:
    """Dump catching: one BUY limit on the favourite's side per level. With ``dump_offsets`` the levels sit
    that far under SIG's own ``band`` in the favourite's terms (a YES bid on a favourite; on a long shot a NO
    bid, i.e. a YES ask above the band), never closer to fair than ``dump_min_edge``, and there are none
    without a band. Else (``roc_levels``) at the price returning that share of its cost to settlement at
    fair (YES at fair / (1 + r) on a favourite; on a long shot NO at (1 - fair) / (1 + r)). Sizes from
    ``roc_sizes``, cut to ``level_cost``. Never crossing: a level the other side already reaches rests one
    tick behind it instead (a better price; what is marketable now is the snipes' job). Needs a confirmed
    two-source fair; no book depth."""
    if fair is None or not fair.confirmed or fair_confidence(fair) < 1.0 or book.settled:
        return []
    yes = fair.value >= 500
    side = Side.YES if yes else Side.NO
    side_fair = fair.value if yes else 1000 - fair.value
    other = book.asks if yes else book.bids
    passive = SIG_MAX_PRICE
    if other:
        passive = other[0].price - SIG_TICK if yes else 1000 - other[0].price - SIG_TICK
    if cfg.dump_offsets:
        if band is None:
            return []
        side_band = band if yes else 1000 - band
        offsets = cfg.dump_offsets
        if not yes and cfg.longshot_offsets and fair.value <= cfg.carry_max_fair:
            offsets = cfg.longshot_offsets  # the carry bucket: asks the crowd's ordinary buying lifts
        levels = [min(side_band - off, side_fair - cfg.dump_min_edge) for off in offsets]
    else:
        levels = [side_fair / (1 + roc) for roc in cfg.roc_levels]
    out: list[Rung] = []
    taken: set[Mils] = set()
    for slot, level in enumerate(levels):
        price = min(sig_floor(int(level)), passive)
        if price < SIG_MIN_PRICE or price in taken:
            continue
        taken.add(price)
        sizes = cfg.roc_sizes
        shares = (
            sizes[min(slot, len(sizes) - 1)] if sizes else (cfg.size if slot == 0 else cfg.secondary_size)
        )
        rung = Rung(
            side,
            price if yes else 1000 - price,
            min(shares, int(cfg.level_cost * 1000 / price)),
            slot,
        )
        if rung.quantity > 0 and rung.cost >= 1.0:
            out.append(rung)
    return out


def flip_rung(
    position: float,
    avg_cost: float,
    band: float | None,
    book: Book,
    cfg: SweepConfig,
    *,
    ours: Iterable[LiveRung] = (),
) -> Rung | None:
    """The flip: a BUY of the opposite side that sells the whole ``position`` (signed shares, YES positive)
    at the better of band - ``flip_margin`` and cost + ``flip_min_profit`` in the held side's terms. Without
    a band only cost counts. When the inside (``ours`` taken out: our own rungs are not buyers) already pays
    that or more, the flip rests at the inside and fills at once (a limit at a bid fills at that bid; SIG
    fills better prices first). None when there is nothing to sell, or no tick between the price and 0.995."""
    if position == 0 or not cfg.flip or book.settled:
        return None
    held_yes = position > 0
    cost = round(avg_cost * 1000)
    target = float(cost + cfg.flip_min_profit)
    if band is not None:
        target = max(target, (band if held_yes else 1000 - band) - cfg.flip_margin)
    # who pays for the held side, in its own terms: the bids when we hold YES, the asks (as NO) when NO
    ours = list(ours)
    buyers = (
        without_ours(book.bids, (o for o in ours if o.rung.side is Side.YES))
        if held_yes
        else without_ours(book.asks, (o for o in ours if o.rung.side is Side.NO))
    )
    if buyers:
        best = buyers[0].price if held_yes else 1000 - buyers[0].price
        if best >= target:
            target = float(best)
    wanted = math.ceil(target)
    if wanted > SIG_MAX_PRICE or wanted < SIG_MIN_PRICE:
        return None  # sig_ceil clamps, and a clamped price would sell under the target
    sell = sig_ceil(wanted)
    side = Side.NO if held_yes else Side.YES
    yes_price = sell if held_yes else 1000 - sell
    return Rung(side, yes_price, int(abs(position)), FLIP_SLOT, cost)


def in_band(fair: Fair, band: Mils) -> bool:
    return fair.value <= band or fair.value >= 1000 - band


@dataclass(frozen=True, slots=True)
class Want:
    """A rung to place, optionally replacing a live one (a primary moving inside a new wall)."""

    exchange_id: str
    rung: Rung
    replaces: LiveRung | None = None


@dataclass(slots=True)
class Scope:
    named: list[str] = field(default_factory=list)
    """Exchanges named in SWEEP_MARKETS, in order (they ignore the fair-value band)."""
    everything: bool = False
    """``all``: every mapped exchange whose fair value is inside the band."""


def resolve_scope(names: Sequence[str], races: Sequence[Race]) -> tuple[Scope, dict[str, Race]]:
    """SWEEP_MARKETS entries (race keys, exchange ids, ``all``) -> scope and exchange id -> race."""
    by_key = {race.key: race for race in races}
    by_exchange = {o.sig_exchange_id: race for race in races for o in race.outcomes}
    scope = Scope()
    for name in names:
        if name.lower() == ALL:
            scope.everything = True
        elif name in by_key:
            scope.named.extend(
                o.sig_exchange_id for o in by_key[name].outcomes if o.sig_exchange_id not in scope.named
            )
        elif name in by_exchange:
            if name not in scope.named:
                scope.named.append(name)
        else:
            log.warning("SWEEP_MARKETS: %r is neither a race key, an exchange id nor 'all'; ignored", name)
    return scope, by_exchange


class SweepLadder:
    def __init__(
        self,
        engine: Engine,
        executor: Executor,
        cfg: SweepConfig,
        scope: Scope,
        races: Mapping[str, Race],
        *,
        parties: Mapping[str, str] | None = None,
        on_activity: Callable[[str], None] | None = None,
        band_of: Callable[[str], float | None] | None = None,
    ) -> None:
        """``races``: exchange id -> race for every mapped exchange. Pulling an exchange's rungs uses one
        cancel-all request, which takes the quoter's quotes there too: set ``on_exchange_wiped`` so it
        places them again (one batch write instead of one cancel write per rung). ``band_of``: exchange ->
        SIG's own YES level (band.py), what band-relative rungs and flips are priced from."""
        self._engine = engine
        self._executor = executor
        self._cfg = cfg
        self._scope = scope
        self._races = dict(races)
        self._parties = parties or {}
        self._on_activity = on_activity
        self._band_of = band_of
        self.flip_profit = 0.0
        """Sum over flip fills of shares x (sell price - cost), SUSQies."""
        self.live: dict[str, list[LiveRung]] = {}
        self._requoted: dict[str, float] = {}
        self._activity: dict[str, tuple[float, float]] = {}
        """exchange -> (decayed notional of recent prints, time of the last update): ranks ``all`` markets."""
        self._bucket = TokenBucket(cfg.actions_per_s, capacity=4)
        self._depth_bucket = TokenBucket(cfg.depth_per_s, capacity=3)
        self._need_depth: dict[str, float] = {}
        """exchange -> time its sizes were last requested; books without sizes carry no rungs."""
        self._unknown: dict[str, float] = {}
        """exchange -> time of the last batch there whose outcome SIG never confirmed."""
        self._cancelled: dict[int, float] = {}
        """Order id -> time the ladder cancelled it (or saw it expire): the listing keeps showing such orders
        for a while, and they must not be adopted back as rungs."""
        self._seen_fills: dict[tuple[int, str, float | None, float], None] = {}
        """Plain account-feed fills already booked (SIG repeats them across batches)."""
        self.foreign_orders: Callable[[], set[int]] | None = None
        """Order ids another manager owns (the quoter's quotes): never adopted as rungs, however rung-like
        their price (the quoter now quotes at the venue's band, 8-12c from fair on long shots)."""
        self.on_exchange_wiped: Callable[[str], None] | None = None
        """Told after a per-exchange cancel-all, which takes other managers' orders on that exchange too."""
        self._depth_tasks: set[asyncio.Task[Book]] = set()
        self._wake = asyncio.Event()
        self._lock = asyncio.Lock()
        self.unfunded = 0
        """Rungs wanted in the latest scan that the collateral budget could not fit."""
        self.min_step_s = 1.0
        """Books change many times a second; one scan a second is plenty for orders resting for minutes."""
        self.rungs_placed = 0
        self.shares_filled = 0.0
        self.edge_filled = 0.0
        """Sum over fills of shares x (distance of the rung from fair at placement), SUSQies."""
        self.rejected: dict[str, int] = {}
        """Risk-gate rejections in the latest scan, by reason (rungs the budget could not fit are not
        rejections: a fully used budget is the normal state)."""
        self.evicted = 0
        """``all`` rungs cancelled since start to fund named markets (``_evict_for``)."""

    def wake(self, exchange_id: str, *_: object) -> None:
        """Book or fair-value change listener."""
        if exchange_id in self.live or self._in_scope(exchange_id):
            self._wake.set()

    def _in_scope(self, exchange_id: str) -> bool:
        return exchange_id in self._scope.named or (self._scope.everything and exchange_id in self._races)

    @property
    def dumps(self) -> bool:
        """Dump catching (``roc_levels`` or ``dump_offsets``), alone or (``far``) beside the far-from-fair
        geometry."""
        return bool(self._cfg.roc_levels or self._cfg.dump_offsets)

    def band(self, exchange_id: str) -> float | None:
        return self._band_of(exchange_id) if self._band_of is not None else None

    @property
    def far(self) -> bool:
        """Whether the far-from-fair geometry is placed (alone, or beside the value rungs)."""
        return not self.dumps or self._cfg.far

    def resting(self, exchange_id: str, side: Side) -> list[tuple[Mils, float]]:
        """Our rungs resting on one side of an exchange as (YES price, shares left), for the quoter."""
        return [
            (lr.rung.yes_price, lr.remaining)
            for lr in self.live.get(exchange_id, [])
            if lr.rung.side is side and lr.remaining > 0
        ]

    def on_trades(self, trades: Sequence[RtTrade]) -> None:
        """Public tape: ranks markets by recent activity; in shadow mode also fills rungs a print went
        through."""
        now = now_mono()
        for t in trades:
            if t.exchange_id not in self._races:
                continue
            notional, at = self._activity.get(t.exchange_id, (0.0, now))
            decay = 0.5 ** ((now - at) / ACTIVITY_HALF_LIFE_S)
            self._activity[t.exchange_id] = (notional * decay + t.price * t.quantity, now)
            if not self._executor.live:
                self._simulate(t)

    def _simulate(self, t: RtTrade) -> None:
        """Shadow fills: a print beyond a rung means a taker went through its level, so the taker would
        have filled us first (we rest inside the crowd). The print's shares are spread over our swept rungs
        from the one nearest fair outward, the way a sweep consumes a book."""
        rungs = self.live.get(t.exchange_id)
        if not rungs:
            return
        price = to_mils(t.price)
        swept = sorted(
            (lr for lr in rungs if lr.remaining > 0 and lr.rung.swept_by(price)),
            key=lambda lr: lr.rung.edge(lr.fair_at),
        )
        left = t.quantity
        for lr in swept:
            if left <= 0:
                break
            fill = min(lr.remaining, left)
            left -= fill
            cost = lr.rung.price * fill / 1000
            lr.traded += fill
            if lr.remaining <= 0:
                rungs.remove(lr)
            self._engine.account.apply_fill(t.exchange_id, lr.rung.side, fill, cost)
            self._engine.rec.write(
                "fills",
                (utcnow(), lr.order_id, t.exchange_id, lr.rung.yes_price / 1000, fill, "shadow_sweep"),
            )
            self._filled(t.exchange_id, lr, fill, cost, simulated=True)
        if not rungs:
            self.live.pop(t.exchange_id, None)
        if left < t.quantity:
            self._wake.set()

    def on_account_batch(self, batch: AccountBatch) -> None:
        """Fills on resting rungs arrive here (the account applies them to positions separately): orders
        that changed state as ``orderUpdates`` (cumulative), orders that only filled as plain ``fills`` (one
        row per execution, repeated across batches by SIG: deduplicated). 2026-10-04 11:15: 17,000 of the
        18,000 flipped SEN-RI D shares came as plain fills and went unreported until this handled them."""
        updated: set[int] = set()
        for update in batch.order_updates:
            rungs = self.live.get(update.exchange_id)
            if not rungs:
                continue
            for lr in list(rungs):
                if lr.order_id != update.order_id:
                    continue
                updated.add(update.order_id)
                new = update.quantity_traded - lr.traded
                if new > 0:
                    cost = update.total_cost * new / update.quantity_traded if update.quantity_traded else 0.0
                    lr.traded = update.quantity_traded
                    self._filled(update.exchange_id, lr, new, cost)
                if not update.open:
                    rungs.remove(lr)
                    if not rungs:
                        self.live.pop(update.exchange_id, None)
                self._wake.set()
        for fill in batch.fills:
            if fill.order_id is None or fill.order_id in updated:
                continue
            rungs = self.live.get(fill.exchange_id)
            if not rungs:
                continue
            key = (fill.order_id, fill.executed_at, fill.price, fill.quantity)
            if key in self._seen_fills:
                continue
            for lr in list(rungs):
                if lr.order_id != fill.order_id:
                    continue
                self._seen_fills[key] = None
                while len(self._seen_fills) > 5000:
                    del self._seen_fills[next(iter(self._seen_fills))]
                # a fill without a price is booked at the rung's own limit (it filled there or better; the
                # account reconciles the exact cost)
                price = fill.price if fill.price is not None else lr.rung.yes_price / 1000
                side_price = price if lr.rung.side is Side.YES else 1 - price
                lr.traded += fill.quantity
                self._filled(fill.exchange_id, lr, fill.quantity, fill.quantity * side_price)
                if lr.remaining <= 0:
                    rungs.remove(lr)
                    if not rungs:
                        self.live.pop(fill.exchange_id, None)
                self._wake.set()

    def _filled(
        self,
        exchange_id: str,
        lr: LiveRung,
        qty: float,
        cost: float,
        *,
        simulated: bool = False,
    ) -> None:
        """Account for ``qty`` more shares filled on ``lr`` (``lr.traded`` is already updated)."""
        race = self._races.get(exchange_id)
        party = self._parties.get(exchange_id, exchange_id)
        where = f"{race.key if race else exchange_id} {party}"
        self.shares_filled += qty
        cash = self._engine.account.cash
        if lr.rung.flip:
            profit = lr.rung.profit
            self.flip_profit += qty * profit / 1000
            held = "YES" if lr.rung.side is Side.NO else "NO"
            text = activity_line(
                STRATEGY,
                "SELL",
                where,
                f"{qty:,.0f} {held} @ {cents(lr.rung.sell_price)}",
                f"cost {cents(lr.rung.ref)}",
                "flip",
                pnl=qty * profit / 1000,
                cash=cash,
                shadow=simulated,
            )
        else:
            edge = lr.rung.edge(lr.fair_at)
            self.edge_filled += qty * edge / 1000
            side = "YES" if lr.rung.side is Side.YES else "NO"
            price = lr.rung.price
            band = self.band(exchange_id)
            side_band = None if band is None else (band if lr.rung.side is Side.YES else 1000 - band)
            text = activity_line(
                STRATEGY,
                "BUY",
                where,
                f"{qty:,.0f} {side} @ {cents(price)}",
                f"band {cents(side_band)}" if side_band is not None else "",
                f"fair {cents(lr.fair_at if lr.rung.side is Side.YES else 1000 - lr.fair_at)} "
                f"({round(edge / 10, 1):+g}c edge)",
                f"cost {cost:,.0f}",
                "dump rung" if self.dumps and not lr.rung.far else "sweep rung",
                shadow=simulated,
            )
        log.info(text)
        if self._on_activity is not None:
            self._on_activity(text)

    def allowed(self) -> bool:
        engine, risk = self._engine, self._engine.risk
        return (
            engine.in_trading_window()
            and self._executor.opening.accepting()
            and not risk.halted
            and STRATEGY not in risk.paused_strategies
        )

    def exchanges(self) -> list[str]:
        """Exchanges to carry rungs, most important first: named ones in their order, then (with ``all``)
        every exchange whose fair value sits inside the band, Senate/Governor/control before House and the
        recently active first (dump catching: by recent activity alone, since the dumps hit the busy
        markets whatever their kind)."""
        out = list(self._scope.named)
        if self._scope.everything:
            band: list[tuple[int, float, str]] = []
            for ex, race in self._races.items():
                if ex in out:
                    continue
                fair = self._engine.fair(ex)
                if fair is not None and in_band(fair, self._cfg.band):
                    kind = 0 if self.dumps else KIND_PRIORITY.get(race.kind, 9)
                    band.append((kind, -self._activity.get(ex, (0.0, 0.0))[0], ex))
            out.extend(ex for _, _, ex in sorted(band))
        return out

    def reserve(self, *, far: bool | None = None) -> float:
        """Collateral resting in rungs: all of them, or only the far / only the value rungs."""
        return sum(
            lr.reserve for rungs in self.live.values() for lr in rungs if far is None or lr.rung.far == far
        )

    def budget(self) -> float:
        """Collateral the ladder may rest in total: a share of equity, and never more than ``cash_multiple``
        times the cash less what the snipes may still deploy under the tier cap."""
        engine, account = self._engine, self._engine.account
        equity = engine.equity
        snipe_room = max(0.0, engine.risk.tier_def().deployed_frac * equity - account.deployed)
        return max(
            0.0,
            min(self._cfg.budget_frac * equity, account.cash * self._cfg.cash_multiple - snipe_room),
        )

    async def step(self) -> None:
        async with self._lock:
            await self._step()

    async def _step(self) -> None:
        risk = self._engine.risk
        risk.check_rate()
        if not self.allowed():
            if risk.halted and self.live:
                await self._pull_everything("halted")
            for ex in list(self.live):  # paused: pull market by market, leaving other strategies' orders
                await self._cancel_exchange(ex, "paused")
            return
        wanted: list[Want] = []
        for ex in self.exchanges():
            if self.far:  # value rungs are priced off fair and the top of book: only far rungs need depth
                self._ensure_depth(ex)
            wanted.extend(await self._sync_exchange(ex))
        # exchanges that left the scope (fair drifted out of the band) still carry rungs: pull them
        for ex in [ex for ex in self.live if ex not in self._scope.named and not self._band_ok(ex)]:
            await self._cancel_exchange(ex, "out of band")
        if wanted:
            await self._place(wanted)

    def _band_ok(self, exchange_id: str) -> bool:
        fair = self._engine.fair(exchange_id)
        return fair is not None and in_band(fair, self._cfg.band)

    def _ensure_depth(self, ex: str) -> None:
        """Fetch sizes for a book that lacks them, in the background (SIG's REST can take 10 s under load,
        and the scan must not stall on it); paced, since the request budget is shared with everything else."""
        book = self._engine.raw_book(ex)
        if book.depth_known or book.settled or self._engine.fair(ex) is None:
            return
        if (
            now_mono() - self._need_depth.get(ex, float("-inf")) < 30.0
            or not self._depth_bucket.try_acquire()
        ):
            return
        self._need_depth[ex] = now_mono()
        task = asyncio.create_task(
            self._engine.books.fetch_depth(ex, depth=self._cfg.depth),
            name=f"depth-{ex}",
        )
        self._depth_tasks.add(task)
        task.add_done_callback(self._depth_done)

    def _depth_done(self, task: asyncio.Task[Book]) -> None:
        self._depth_tasks.discard(task)
        if not task.cancelled() and task.exception() is None:
            self._wake.set()

    async def _sync_exchange(self, ex: str) -> list[Want]:
        """Pull, rebuild or trim this exchange's rungs; return the rungs still to be placed (each with the
        live rung it replaces, if any)."""
        engine, cfg = self._engine, self._cfg
        rungs = self.live.get(ex, [])
        race = self._races.get(ex)
        fair = engine.fair(ex)
        book = engine.raw_book(ex)
        if race is None or ex in engine.risk.manual_paused:
            if rungs:
                await self._cancel_exchange(ex, "manual")
            return []
        self._drop_vanished(ex)
        rungs = self.live.get(ex, [])
        moved = fair is not None and any(
            abs(fair.value - lr.fair_at) >= cfg.relevel_move for lr in rungs if not lr.rung.flip
        )
        band = self.band(ex)
        want = desired_rungs(fair, book, cfg, ours=rungs, band=band)
        flip = self._flip_for(ex, fair, book, band)
        if flip is not None:
            want = [*want, flip]
        if fair is not None and want:
            await self._adopt_strays(ex, fair, want)
            rungs = self.live.get(ex, [])
        now = now_mono()
        if flip is None and (live_flip := next((lr for lr in rungs if lr.rung.flip), None)) is not None:
            # the position is gone (or the quoter works it): a flip left resting would open the other side
            await self._cancel_rung(ex, live_flip)
            rungs = self.live.get(ex, [])
        if not want:
            # keep rungs through a brief unconfirmed fair (a sharp move being checked); pull when the market
            # settled, fair value is gone or it moved for real
            if rungs and (fair is None or book.settled or moved):
                await self._cancel_exchange(ex, "no rungs wanted")
            return []
        assert fair is not None
        if now - self._unknown.get(ex, float("-inf")) < UNKNOWN_HOLD_S:
            return []  # a batch here may still be executing at SIG: wait for the listing, adopt, then act
        if rungs and moved:
            await self._cancel_exchange(ex, "relevel")
            rungs = []
        elif rungs and any(now - lr.placed >= cfg.expiry_s + EXPIRY_GRACE_S for lr in rungs):
            # SIG has expired these by itself: forget them and place the ladder again. Renewing them before
            # expiry cost a cancel (a write) per exchange every half hour; the gap is one step.
            expired = [lr for lr in rungs if now - lr.placed >= cfg.expiry_s + EXPIRY_GRACE_S]
            for lr in expired:
                self._cancelled[lr.order_id] = now  # the listing may still show it for a moment
                rungs.remove(lr)
            if not rungs:
                self.live.pop(ex, None)
            log.info("ladder %s: %d rungs expired at SIG, placing again", ex, len(expired))
        live_by_slot = {(lr.rung.side, lr.rung.slot): lr for lr in rungs}
        missing: list[Want] = []
        far_primary = FAR_SLOT if self.dumps else 0
        requote_ok = now - self._requoted.get(ex, float("-inf")) >= cfg.requote_min_s
        for rung in want:
            live = live_by_slot.get((rung.side, rung.slot))
            if live is None:
                missing.append(Want(ex, rung))
            elif rung.flip:
                # the flip follows the band and the position: re-placed when its price moves a cent or the
                # position changed by FLIP_RESIZE_MIN shares and a fifth (a cancel and a write each time)
                resized = abs(rung.quantity - live.remaining) >= max(FLIP_RESIZE_MIN, 0.2 * live.remaining)
                repriced = abs(rung.yes_price - live.rung.yes_price) >= FLIP_REPRICE_MIN
                if (repriced or resized) and requote_ok:
                    missing.append(Want(ex, rung, replaces=live))
            elif (
                rung.slot == far_primary  # value rungs follow fair: moved on a relevel, not on every tick
                and rung.edge(fair.value) < live.rung.edge(fair.value)
                and requote_ok
            ):
                # a wall appeared inside our primary: move one tick inside it again (never outward: staying
                # put keeps time priority, and a full sweep reaches us either way). The old rung is only
                # cancelled once the new one has passed the gate, so a rejection never leaves the slot empty.
                missing.append(Want(ex, rung, replaces=live))
        return missing

    def _flip_for(self, ex: str, fair: Fair | None, book: Book, band: float | None) -> Rung | None:
        """The flip this exchange wants: the position's resting sell (``flip_rung``), unless the quoter is
        working the market (its asks are that exit) or there is no fair value at all (nothing else rests
        here then, and a market without a price is not one to trade)."""
        engine, cfg = self._engine, self._cfg
        if not self.dumps or fair is None or engine.is_quoting(ex):
            return None
        holding = engine.holding(ex)
        if holding is None or holding.qty == 0:
            return None
        if cfg.hold_frac > 0:
            # a settlement hold: the side is near-certain to pay 1, so it is worth more at settlement than
            # at any flip, and the capital is not needed while the book stays under hold_frac of equity
            side_fair = fair.value if holding.qty > 0 else 1000 - fair.value
            if side_fair >= HOLD_SIDE_FAIR and engine.account.deployed <= cfg.hold_frac * engine.equity:
                return None
        return flip_rung(holding.qty, holding.avg_cost, band, book, cfg, ours=self.live.get(ex, []))

    async def _place(self, wanted: list[Want]) -> None:
        """Risk-gate and batch-place rungs while the collateral budget lasts: every primary first (in market
        priority order), then secondaries by edge per SUSQie of collateral."""
        engine, cfg = self._engine, self._cfg
        priority = {ex: i for i, ex in enumerate(dict.fromkeys(w.exchange_id for w in wanted))}
        named = set(self._scope.named)

        def rank(item: Want) -> tuple[int, float, float]:
            """Named markets get whole ladders before ``all`` markets get anything; within a class,
            primaries come first in market order, then secondaries by edge per SUSQie of collateral. Far
            rungs beside value rungs (their own budget slice) go breadth-first: every market's nearest far
            rung before any market's next one, since spikes land on any busy market and the nearest rungs
            fill most often."""
            ex, rung = item.exchange_id, item.rung
            if rung.flip:
                return -1, float(priority[ex]), 0.0  # the round trip's second half: before any new rung
            klass = (0 if ex in named else 2) + (0 if rung.slot == 0 else 1)
            if rung.slot == 0:
                return klass, float(priority[ex]), 0.0
            if rung.far and self.dumps:
                return klass, float(rung.slot), float(priority[ex])
            fair = engine.fair(ex)
            return klass, -rung.edge(fair.value if fair else 500) / rung.price, float(priority[ex])

        # ``all`` markets whose primary is not resting yet (depth still loading, fair unconfirmed...) keep
        # their primary's collateral reserved, so secondaries never crowd out a market's first rung
        missing = sum(
            1
            for ex in self.exchanges()
            if ex not in named and not any(lr.rung.slot == 0 for lr in self.live.get(ex, []))
        )
        # beside the value rungs the far geometry has its own slice of the budget: the cheap jackpot options
        # must not wait behind every market's expensive band rung, nor eat the band's collateral
        budget = self.budget()
        split = self.dumps and cfg.far
        far_share = cfg.far_frac if split else 0.0
        available = {
            True: budget * far_share - self.reserve(far=True) if split else 0.0,
            False: budget * (1 - far_share) - self.reserve(far=False) if split else budget - self.reserve(),
        }
        fair_map, equity = engine.all_fair_map(), engine.equity
        batch: list[tuple[str, Rung, Intent, Mils]] = []
        pending: dict[str, float] = {}
        """Dump catching: collateral approved in this pass per race, not resting yet."""
        self.rejected = {}
        self.unfunded = 0
        for item in sorted(wanted, key=rank):
            ex, rung = item.exchange_id, item.rung
            race = self._races[ex]
            fair = engine.fair(ex)
            if fair is None:
                continue
            label = "ask" if rung.side is Side.NO else "bid"
            if rung.flip:
                # closes a position: no collateral, no race room, and the gate treats it as an exit
                edge = float(rung.profit)
                detail = f"flip {label} {rung.yes_price}: sells at {rung.sell_price} (cost {rung.ref})"
                intent = Intent(
                    STRATEGY,
                    race.key,
                    (Leg(ex, rung.side, rung.price, rung.quantity),),
                    edge,
                    TradeReason.INVENTORY_REBALANCE,
                    detail,
                    now_mono(),
                    reduces_risk=True,
                )
                approved, reason = engine.risk.check(intent, fair_map, equity, race=race, resting=True)
                if approved is None:
                    self._reject(reason)
                    log.debug("flip %s %s not placed: %s", ex, label, reason)
                    continue
                if item.replaces is not None:
                    if not await self._cancel_rung(ex, item.replaces):
                        continue
                    self._requoted[ex] = now_mono()
                batch.append((ex, rung, approved, fair.value))
                if len(batch) >= cfg.batch_size:
                    await self._send(batch)
                    batch = []
                continue
            if self.dumps:
                # value rungs are expensive and a big dump fills all of them: as if filled, they and the
                # race's holdings must stay within the hard loss ceiling (the gate checks one rung at a time)
                room = self._race_room(race, equity, pending.get(race.key, 0.0), item.replaces)
                fit = min(rung.quantity, int(max(0.0, room) * 1000 / rung.price))
                if fit < max(100, rung.quantity // 10):
                    self._reject("race room")
                    continue
                rung = Rung(rung.side, rung.yes_price, fit, rung.slot)
            slice_ = rung.far and split
            held_back = 0.0 if rung.slot == 0 or ex in named or slice_ else missing * cfg.level_cost
            freed = item.replaces.reserve if item.replaces is not None else 0.0
            shortfall = rung.cost - (available[slice_] + freed - held_back)
            if shortfall > 0 and not (ex in named and not slice_):
                self.unfunded += 1
                continue
            edge = float(rung.edge(fair.value))
            detail = f"rung {rung.slot} {label} {rung.yes_price} vs fair {fair.value}"
            if self.dumps:
                detail = f"dump {detail} ({edge / rung.price:.1%} to settlement)"
            intent = Intent(
                STRATEGY,
                race.key,
                (Leg(ex, rung.side, rung.price, rung.quantity),),
                edge,
                TradeReason.MARKET_MAKE,
                detail,
                now_mono(),
            )
            approved, reason = engine.risk.check(
                intent,
                fair_map,
                equity,
                race=race,
                confidence=race_cap_scale([fair], edge),
                resting=True,
            )
            if approved is None:
                self._reject(reason)
                log.debug("rung %s %s not placed: %s", ex, label, reason)
                continue
            if approved.quantity < max(100, rung.quantity // 10):
                # the gate left a sliver (a race near its loss ceiling): not worth a request or a slot
                self._reject("too small")
                continue
            if shortfall > 0:
                # priority holds over time, not only within a pass: a named market whose rungs became
                # wanted later (band warm-up, adopted rungs expiring, a held batch) takes the budget back
                # from the least valuable ``all`` rungs instead of waiting for them to expire. Only once
                # the gate has passed the rung: evicting for a rung that is then turned away (a stale
                # book) hands the budget straight back to the ``all`` markets, a cancel and a placement
                # per pass for nothing (2026-10-11 19:44-19:47 UTC, three rounds). What the evictions
                # free stays out of this pass's budget unless the rung is placed: a pass that could free
                # only part of the shortfall (one cancel per pass when writes are short) must not refill
                # the evicted slot with it, or it is evict-and-refill every pass (2026-10-15 09:59 UTC,
                # 16 rounds in a minute); the next pass finds the room and this rung is first in line.
                evicted = await self._evict_for(ex, shortfall, named)
                if rung.cost > available[slice_] + evicted + freed - held_back:
                    self.unfunded += 1
                    continue
                available[slice_] += evicted
            if item.replaces is not None:
                if not await self._cancel_rung(ex, item.replaces):
                    continue
                self._requoted[ex] = now_mono()
                available[slice_] += freed
            available[slice_] -= approved.capital
            pending[race.key] = pending.get(race.key, 0.0) + approved.capital
            if rung.slot == 0 and ex not in named:
                missing -= 1
            batch.append((ex, rung, approved, fair.value))
            if len(batch) >= cfg.batch_size:
                await self._send(batch)
                batch = []
        if batch:
            await self._send(batch)

    async def _send(self, batch: list[tuple[str, Rung, Intent, Mils]]) -> None:
        await self._bucket.acquire()
        placed = await self._executor.place_ladder([intent for _, _, intent, _ in batch], self._cfg.expiry_s)
        now = now_mono()
        if placed is None:
            # SIG may have placed some or all of these: hold the exchanges until the next open-orders
            # listing shows what rests there, and adopt it
            exchanges = sorted({ex for ex, _, _, _ in batch})
            for ex in exchanges:
                self._unknown[ex] = now
            log.warning("ladder: batch of %d rungs has an unknown outcome; holding %s", len(batch), exchanges)
            return
        for (ex, rung, intent, fair_at), result in zip(batch, placed, strict=True):
            if result is None:
                continue
            self.rungs_placed += 1
            sized = Rung(
                rung.side,
                rung.yes_price,
                intent.quantity,
                rung.slot,
                rung.ref,
            )  # the gate may have resized it
            live = LiveRung(sized, result.order_id or 0, now, fair_at, result.filled)
            if result.filled:
                self._filled(ex, live, result.filled, result.cost)
            if result.order_id is not None and live.remaining > 0:
                self.live.setdefault(ex, []).append(live)
        log.info(
            "ladder: placed %d of %d rungs [%s] (%d resting on %d exchanges, %.0f reserved)",
            sum(1 for r in placed if r is not None),
            len(batch),
            " ".join(
                f"{ex}:{'ask' if rung.side is Side.NO else 'bid'}{rung.yes_price / 10:g}c"
                f"x{intent.quantity}{'' if result else '!'}"
                for (ex, rung, intent, _), result in zip(batch, placed, strict=True)
            ),
            sum(len(v) for v in self.live.values()),
            len(self.live),
            self.reserve(),
        )

    def _reject(self, reason: str) -> None:
        self.rejected[reason] = self.rejected.get(reason, 0) + 1

    async def _evict_for(self, ex: str, shortfall: float, named: set[str]) -> float:
        """Cancel ``all`` value rungs, deepest slot and least active market first, until ``shortfall`` of
        collateral is free or ``EVICT_MAX_PER_PASS`` cancels are spent; return what was freed."""
        candidates = [
            (-lr.rung.slot, self._activity.get(other, (0.0, 0.0))[0], other, lr)
            for other, rungs in self.live.items()
            if other not in named
            for lr in rungs
            if not lr.rung.flip and not lr.rung.far
        ]
        candidates.sort(key=lambda c: (c[0], c[1]))
        freed, evicted = 0.0, 0
        for _, _, other, lr in candidates:
            if freed >= shortfall or evicted >= EVICT_MAX_PER_PASS:
                break
            if not await self._cancel_rung(other, lr):
                break  # out of writes this minute: the rest waits for the next pass
            freed += lr.reserve
            evicted += 1
            self.evicted += 1
        if evicted:
            log.info("ladder %s: evicted %d rungs (%.0f freed) to fund a named market", ex, evicted, freed)
        return freed

    def _race_room(self, race: Race, equity: float, pending: float, replaces: LiveRung | None) -> float:
        """Collateral more rungs on ``race`` may take: the gate's hard per-result loss ceiling, less the
        race's worst result on current holdings, less what rests (or was just approved) there; every rung
        counted as lost in full, whichever result it loses in (conservative)."""
        risk = self._engine.risk
        resting = sum(
            lr.reserve
            for o in race.outcomes
            for lr in self.live.get(o.sig_exchange_id, [])
            if lr is not replaces
        )
        return risk.race_loss_frac * equity - risk.race_worst_loss(race) - resting - pending

    def _listing(self) -> tuple[dict[int, Order], float] | None:
        """SIG's open orders as of the account's last reconcile (live mode only; the paper account never
        lists orders) and when that listing was taken."""
        account = self._engine.account
        if not self._executor.live or account.paper:
            return None
        return account.open_orders, account.open_orders_at

    def _drop_vanished(self, ex: str) -> None:
        """Forget rungs an open-orders listing taken well after their placement no longer shows: filled,
        expired or cancelled without an update reaching us. Their slot is placed again."""
        listing = self._listing()
        rungs = self.live.get(ex)
        if listing is None or not rungs:
            return
        open_orders, taken = listing
        gone = [
            lr
            for lr in rungs
            if lr.order_id > 0 and taken - lr.placed > LISTING_LAG_S and lr.order_id not in open_orders
        ]
        now = now_mono()
        for lr in gone:
            log.info(
                "ladder %s: rung %s (%s %sc) is no longer open at SIG",
                ex,
                lr.order_id,
                lr.rung.side.value,
                lr.rung.yes_price / 10,
            )
            rungs.remove(lr)
            if lr.rung.flip and now - lr.placed < self._cfg.expiry_s:
                # a flip gone before its expiry has most likely just filled, and the reconcile reads
                # positions before orders: the position it closed can still show as open for a few
                # seconds. Placing the flip again now sells the position twice and opens the other side
                # (SEN-NH R 2026-10-14 19:35 UTC: 7,421 YES of an 11% long shot bought at 21c; SEN-WY R
                # 2026-10-13 20:20: 24k YES sold 2.3 times over). Hold the exchange until the fills land.
                self._unknown[ex] = now
                log.info(
                    "ladder %s: flip vanished before expiry; holding %.0fs for its fills",
                    ex,
                    UNKNOWN_HOLD_S,
                )
        if not rungs:
            self.live.pop(ex, None)

    async def _adopt_strays(self, ex: str, fair: Fair, want: Sequence[Rung]) -> None:
        """Take over resting orders of ours on this exchange that we do not track (a batch SIG executed after
        we had given up on it, or rungs from before a restart that the startup cancel missed): anything far
        enough from fair to be a rung becomes one, under the slot of the nearest wanted rung; a second order
        in a slot already taken is a duplicate and is cancelled. Flow quotes rest near fair and are left
        alone."""
        listing = self._listing()
        if listing is None:
            return
        open_orders, _ = listing
        now = now_mono()
        self._cancelled = {oid: at for oid, at in self._cancelled.items() if now - at < CANCELLED_MEMORY_S}
        known = {lr.order_id for lr in self.live.get(ex, [])} | set(self._cancelled)
        if self.foreign_orders is not None:
            known |= self.foreign_orders()
        wall = utcnow() - timedelta(seconds=ADOPT_MIN_AGE_S)
        for order in sorted(open_orders.values(), key=lambda o: o.created_at):
            if order.exchange_id != ex or order.id in known or not order.open or order.price_limit is None:
                continue
            created = parse_server_ts(order.created_at)
            if created is not None and created > wall:
                continue  # may be a batch of the quoter's (or ours) whose response is still in flight
            price = to_mils(order.price_limit)
            yes_price = price if order.side is Side.YES else 1000 - price
            flip_like = self._flip_like(ex, order.side, yes_price)
            if not flip_like and not self._rung_like(order.side, yes_price, fair):
                continue
            same_side = [r for r in want if r.side is order.side and r.flip == flip_like]
            slot = (
                min(same_side, key=lambda r: abs(r.yes_price - yes_price)).slot
                if same_side
                else (FLIP_SLOT if flip_like else 9)
            )
            rungs = self.live.setdefault(ex, [])
            if any(lr.rung.slot == slot and lr.rung.side is order.side for lr in rungs):
                if self._bucket.try_acquire():
                    log.warning(
                        "ladder %s: cancelling duplicate order %s (%s %sc)",
                        ex,
                        order.id,
                        order.side.value,
                        yes_price / 10,
                    )
                    self._cancelled[order.id] = now
                    await self._executor.cancel_quote(order.id)
                continue
            ref = 0
            if flip_like and (holding := self._engine.holding(ex)) is not None:
                ref = round(holding.avg_cost * 1000)
            rung = Rung(order.side, yes_price, int(order.quantity), slot, ref)
            rungs.append(LiveRung(rung, order.id, now_mono(), fair.value))
            log.warning(
                "ladder %s: adopted untracked order %s as rung %s (%s %sc x%d)",
                ex,
                order.id,
                slot,
                order.side.value,
                yes_price / 10,
                rung.quantity,
            )

    def _flip_like(self, ex: str, side: Side, yes_price: Mils) -> bool:
        """Whether an untracked resting buy of ours is a flip: it sells a side we hold at about cost or
        better (a batch SIG executed after we gave up on it; a second flip would sell the position twice)."""
        if not (self.dumps and self._cfg.flip):
            return False
        holding = self._engine.holding(ex)
        if holding is None or holding.qty == 0 or side is (Side.YES if holding.qty > 0 else Side.NO):
            return False
        sells_at = yes_price if holding.qty > 0 else 1000 - yes_price
        return sells_at >= round(holding.avg_cost * 1000) - STRAY_FLIP_SLACK

    def _rung_like(self, side: Side, yes_price: Mils, fair: Fair) -> bool:
        """Whether a resting buy of ours at ``yes_price`` looks like one of our rungs: far enough from fair
        (sweep catching), or buying the favourite at about the lowest return level (dump catching; with
        band-relative rungs any buy of the favourite under its fair)."""
        cfg = self._cfg
        far_like = (
            yes_price >= fair.value + cfg.floor if side is Side.NO else yes_price <= fair.value - cfg.floor
        )
        if not (cfg.roc_levels or cfg.dump_offsets):
            return far_like
        if cfg.far and far_like:
            return True
        yes = fair.value >= 500
        if side is not (Side.YES if yes else Side.NO):
            return False
        price, side_fair = (yes_price, fair.value) if yes else (1000 - yes_price, 1000 - fair.value)
        if cfg.dump_offsets:
            return side_fair - price >= cfg.dump_min_edge
        return (side_fair - price) / price >= min(cfg.roc_levels) - STRAY_ROC_SLACK

    async def _cancel_rung(self, ex: str, live: LiveRung) -> bool:
        if not self._bucket.try_acquire():
            return False
        self._cancelled[live.order_id] = now_mono()
        await self._executor.cancel_quote(live.order_id)
        rungs = self.live.get(ex, [])
        if live in rungs:
            rungs.remove(live)
        if not rungs:
            self.live.pop(ex, None)
        return True

    async def _cancel_exchange(self, ex: str, why: str) -> None:
        """Pull every rung on an exchange: one cancel-all request when there are several (it takes the
        quoter's quotes on that exchange too, so the quoter is told to place them again), else one cancel."""
        rungs = self.live.pop(ex, [])
        if not rungs:
            return
        log.info("ladder %s: pulling %d rungs (%s)", ex, len(rungs), why)
        now = now_mono()
        for live in rungs:
            self._cancelled[live.order_id] = now
        await self._bucket.acquire()
        if len(rungs) >= 2:
            await self._executor.cancel_exchange(ex)
            if self.on_exchange_wiped is not None:
                self.on_exchange_wiped(ex)
            return
        for live in rungs:
            await self._executor.cancel_quote(live.order_id)

    async def _pull_everything(self, why: str) -> None:
        """Halt or pause: one global cancel-all (other strategies' resting orders go too; the flow quotes are
        re-placed by their manager within seconds, snipe remainders are cancelled anyway)."""
        total = sum(len(v) for v in self.live.values())
        self.live.clear()
        log.warning("ladder: pulling all %d rungs (%s) with a global cancel-all", total, why)
        await self._executor.cancel_everything()

    async def cancel_all(self) -> None:
        if self.live:
            await self._pull_everything("shutdown")

    def stats(self) -> dict[str, object]:
        return {
            "resting": sum(len(v) for v in self.live.values()),
            "flips": sum(1 for v in self.live.values() for lr in v if lr.rung.flip),
            "exchanges": len(self.live),
            "reserved": round(self.reserve()),
            "budget": round(self.budget()),
            "placed": self.rungs_placed,
            "shares_filled": round(self.shares_filled),
            "edge_filled": round(self.edge_filled),
            "flip_profit": round(self.flip_profit),
            "unfunded": self.unfunded,
            "evicted": self.evicted,
            "rejected": dict(self.rejected),
        }

    async def run(self, stop: asyncio.Event) -> None:
        cfg = self._cfg
        where = (f"{len(self._scope.named)} named exchanges" if self._scope.named else "") + (
            " + every lopsided market" if self._scope.everything else ""
        )
        if self.dumps:
            log.info(
                "dump catching on %s: favourite bids %s, %s shares (at most %.0f "
                "a rung), budget %.0f%% of equity (<= %.1fx cash), band %dc, relevel %.1fc, expiry %.0fs%s%s",
                where,
                (
                    f"{[o / 10 for o in cfg.dump_offsets]}c under SIG's own level "
                    f"(>= {cfg.dump_min_edge / 10:g}c under fair)"
                    if cfg.dump_offsets
                    else "returning " + ", ".join(f"{r:.0%}" for r in cfg.roc_levels) + " to settlement"
                ),
                list(cfg.roc_sizes) or [cfg.size, cfg.secondary_size],
                cfg.level_cost,
                cfg.budget_frac * 100,
                cfg.cash_multiple,
                cfg.band // 10,
                cfg.relevel_move / 10,
                cfg.expiry_s,
                (
                    f"; far rungs too ({cfg.far_frac:.0%} of the budget): floor {cfg.floor // 10}c, "
                    f"offsets {[o // 10 for o in cfg.offsets]}, {cfg.size}/{cfg.secondary_size} shares, "
                    f"walls >= {cfg.wall_qty}{', behind walls' if cfg.behind_walls else ''}"
                    if cfg.far
                    else ""
                ),
                (
                    f"; flips: every position sold back at the better of band - {cfg.flip_margin / 10:g}c "
                    f"and cost + {cfg.flip_min_profit / 10:g}c"
                    if cfg.flip
                    else ""
                ),
            )
        else:
            log.info(
                "sweep catching on %s: floor %dc, secondaries %s, %d/%d shares, budget %.0f%% of equity, "
                "expiry %.0fs",
                where,
                cfg.floor // 10,
                [o // 10 for o in cfg.offsets],
                cfg.size,
                cfg.secondary_size,
                cfg.budget_frac * 100,
                cfg.expiry_s,
            )
        while not stop.is_set():
            await wait_event_or_stop(self._wake, stop, 2.0)
            self._wake.clear()
            if stop.is_set():
                break
            started = now_mono()
            try:
                await self.step()
            except Exception:
                log.exception("sweep step failed")
            if (pause := self.min_step_s - (now_mono() - started)) > 0:
                await wait_event_or_stop(stop, stop, pause)
