"""Flow trading: rest a bid and an ask around the venue's price, to earn the spread from other traders' market
orders. Off unless ``MM_MARKETS`` names races, exchanges or ``all``.

Each quote is a BUY limit order that SIG expires by itself after ``expiry_s``: the bid buys YES, the ask buys
NO at 1 - ask. Prices come from ``desired_quotes``:
* the **anchor** is SIG's own mid (the others' best bid and ask, our resting orders taken out) whenever both
  sides exist and the spread is at most ``max_spread``; a book with one side missing is not quoted, and a
  wider one is quoted around fair value. SIG clears far from Polymarket/Kalshi fair for days (2026-10-03: a
  3% long shot trades 12.5-15.5c all day, 36-160k shares per half hour, with 5-60k shares resting on every
  tick above), so quoting around fair there sells 5c under the venue's own bid and never buys;
* each side is quoted ``half_spread`` from the anchor when that improves on the inside, else it **joins** the
  inside when that is at least ``min_half_spread`` from the anchor (a 1c-wide market is joined, a 1-tick one
  is left alone); never crossing the other side (that is snipe territory);
* fair value is the **guard**, per side: a quote that buys a side above what it is worth gets ``rich_frac``
  of the size, and one buying more than ``fair_guard`` above worth only closes a position we hold the other
  way (the round trip books the spread; opening long a 3% long shot at 13c would not). The ask on that long
  shot buys NO 12c under its worth and is quoted in full: the inventory this leaves carries positive
  expectation to settlement whichever way the venue's band drifts.
A side is quoted only while fair value is confirmed and fully trusted (both sources agree; a sharp external
move cancels the side's quote within a step, whatever the fill feed says), the book is fresh, and inventory
is below ``max_inventory`` in that direction.

Scale comes from treating requests, not fills, as the scarce resource (fills on resting orders cost no API
calls; the leaders' market makers took 2-7k fills a day under the same 100/min key):
* quotes are placed in ``/orders/batch`` requests of ``batch_size`` (one request for up to 50 quotes);
* a quote is replaced, not cancelled, when its wanted price moves ``requote_move`` or it nears expiry: the
  new one is placed and the old one left to expire; a quote (live or superseded) left at or through the
  anchor (the mid: adverse-selection bait) is cancelled, as are
  quotes on a side whose inventory cap is full, and everything on a halt, pause, settled market or lost
  fair value;
* collateral resting in quotes is capped by ``budget_frac`` of equity (superseded quotes are cancelled,
  biggest first, when the budget needs their collateral), positions our quotes acquired by
  ``inventory_frac`` (above it only the reducing side is quoted); quotes skip the deployed-capital cap.
Fills arrive on the account feed: ``orderUpdates`` for orders that also changed state, plain ``fills`` for
quotes that only filled (the account applies positions separately and deduplicates). Every placement
passes the risk gate like any other order.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sigbot.config import QuoteConfig
from sigbot.core.reasons import TradeReason
from sigbot.core.util import SIG_TICK, Mils, TokenBucket, now_mono, sig_floor, wait_event_or_stop
from sigbot.discord.alerts import activity_line, cents
from sigbot.market.fair import Fair
from sigbot.market.mapping import Kind, Race
from sigbot.market.state import Book, Level
from sigbot.sigapi.models import AccountBatch, RtTrade, Side
from sigbot.strategies.base import Intent, Leg
from sigbot.trading.risk import fair_confidence

if TYPE_CHECKING:
    from sigbot.trading.engine import Engine
    from sigbot.trading.executor import Executor

log = logging.getLogger(__name__)

STRATEGY = "quote"
ALL = "all"
ACTIVITY_HALF_LIFE_S = 1_800.0
"""Recent prints weigh half as much every 30 min in the activity ranking behind ``top_n``."""
BATCH_MAX_WAIT_S = 3.0
"""A batch of quotes the write budget cannot take within this long is dropped (re-priced next step)."""
GONE_MEMORY_S = 180.0
"""How long past its expiry a quote's id stays in ``order_ids`` (SIG's listing lags its engine by up to a
minute, and a quote cancelled at the anchor shows there until the cancel lands). The ladder adopted the
quoter's cancelled asks on SEN-TN D as rungs five times in three minutes (2026-10-03 18:54) before every
placed id was remembered this way."""
KIND_PRIORITY = {Kind.SEN: 0, Kind.GOV: 1, Kind.CTRL: 2, Kind.HOUSE: 3}


@dataclass(frozen=True, slots=True)
class Quote:
    side: Side
    """Side we BUY: YES for our bid, NO for our ask."""
    yes_price: Mils
    """Our bid or ask in YES terms."""
    quantity: int

    @property
    def price(self) -> Mils:
        """Limit in the bought side's terms (an ask at p is a NO bid at 1 - p)."""
        return self.yes_price if self.side is Side.YES else 1000 - self.yes_price


@dataclass(slots=True)
class LiveQuote:
    quote: Quote
    order_id: int
    placed: float
    traded: float = 0.0
    exchange_id: str = ""

    @property
    def remaining(self) -> float:
        return max(0.0, self.quote.quantity - self.traded)

    @property
    def reserve(self) -> float:
        """Collateral the quote still locks (SUSQies)."""
        return self.quote.price * self.remaining / 1000


def _as_list(ours: LiveQuote | Iterable[LiveQuote] | None) -> list[LiveQuote]:
    if ours is None:
        return []
    return [ours] if isinstance(ours, LiveQuote) else list(ours)


def _edge(live: LiveQuote, anchor: float) -> float:
    """How far a resting quote sits on our side of the anchor (mils; <= 0 = at or through it)."""
    q = live.quote
    return anchor - q.yes_price if q.side is Side.YES else q.yes_price - anchor


def others_best(levels: Sequence[Level], ours: LiveQuote | Iterable[LiveQuote] | None) -> Mils | None:
    """Best price on one side of the book once our own resting quotes are taken out (SIG's books include
    them): a level at one of our prices is ours unless it holds more than we rest there."""
    mine: dict[Mils, float] = {}
    for live in _as_list(ours):
        mine[live.quote.yes_price] = mine.get(live.quote.yes_price, 0.0) + live.remaining
    for lv in levels:
        own = mine.get(lv.price)
        if own is not None and (lv.qty < 0 or lv.qty <= own + 1e-9):
            continue
        return lv.price
    return None


def quotable(fair: Fair | None, book: Book) -> bool:
    """Whether this market may carry quotes at all: a confirmed, fully trusted fair and an open market."""
    return fair is not None and fair.confirmed and fair_confidence(fair) >= 1.0 and not book.settled


def anchor_of(
    fair: Fair,
    book: Book,
    cfg: QuoteConfig,
    *,
    bid_live: LiveQuote | Iterable[LiveQuote] | None = None,
    ask_live: LiveQuote | Iterable[LiveQuote] | None = None,
) -> tuple[float | None, Mils | None, Mils | None]:
    """(anchor, others' best bid, others' best ask): the venue's mid when the others' spread is at most
    ``max_spread``, fair value when it is wider, None when a side of the book is empty (no venue price to
    quote around: a lone ask would just be undercut toward fair, giving the venue's premium away)."""
    other_bid = others_best(book.bids, bid_live)
    other_ask = others_best(book.asks, ask_live)
    if other_bid is None or other_ask is None or other_ask <= other_bid:
        return None, other_bid, other_ask
    if other_ask - other_bid <= cfg.max_spread:
        return (other_bid + other_ask) / 2, other_bid, other_ask
    return float(fair.value), other_bid, other_ask


def _side_price(
    anchor: float,
    half: int,
    min_half: int,
    other_same: Mils | None,
    other_cross: Mils | None,
    *,
    bid: bool,
) -> Mils | None:
    """Our price on one side: ``half`` from the anchor when that improves on the others' best, else joined
    to their best when it is at least ``min_half`` from the anchor; None when the side cannot be quoted
    without crossing or sitting too close to the anchor. Prices in the quoting direction (for the ask the
    caller mirrors around 1000 so that 'better' always means higher)."""
    if anchor - half < 5:
        return None  # too close to the extreme for our spread
    ideal = sig_floor(int(anchor - half))
    if other_same is None or ideal > other_same:
        price = ideal
    elif other_same <= anchor - min_half:
        price = other_same
    else:
        return None
    if other_cross is not None and price >= other_cross:
        return None
    return price if 5 <= price <= 995 else None


def desired_quotes(
    fair: Fair | None,
    book: Book,
    inventory: float,
    cfg: QuoteConfig,
    *,
    bid_live: LiveQuote | Iterable[LiveQuote] | None = None,
    ask_live: LiveQuote | Iterable[LiveQuote] | None = None,
    own: float | None = None,
    cost: float | None = None,
    cushion: float = 0.0,
) -> dict[Side, Quote]:
    """The quotes we want right now (side -> quote); empty when we should not quote at all. ``inventory`` is
    the whole position on the exchange (the caps), ``own`` the part of it a quote past the fair guard may
    close (the buy-back leg of the round trip). ``cost`` is the position's average cost per share in the
    held side's terms (mils) and ``cushion`` the market's booked P&L: a buy-back under cost closes no more
    than the cushion pays for."""
    if fair is None or not quotable(fair, book):
        return {}
    if own is None:
        own = inventory
    anchor, other_bid, other_ask = anchor_of(fair, book, cfg, bid_live=bid_live, ask_live=ask_live)
    if anchor is None:
        return {}
    out: dict[Side, Quote] = {}
    bid = _side_price(anchor, cfg.half_spread, cfg.min_half_spread, other_bid, other_ask, bid=True)
    # the ask mirrored: buying NO at 1 - ask, so a higher NO price is a lower ask
    mirrored = _side_price(
        1000 - anchor,
        cfg.half_spread,
        cfg.min_half_spread,
        None if other_ask is None else 1000 - other_ask,
        None if other_bid is None else 1000 - other_bid,
        bid=False,
    )
    ask = None if mirrored is None else 1000 - mirrored
    # A tight market (the venue's busiest books are one tick wide) leaves no room to improve or to join
    # min_half_spread from the anchor. Its inside is still joined on a side that is at least
    # min_half_spread better than fair: the long shot's 13.5c ask on a 3c contract is the venue's premium
    # itself, and queueing behind the wall there is the only way to collect it (2026-10-03: the 20 most
    # active markets were all one or two ticks wide and the quoter sat out every one of them).
    # The other side of such a market is joined only to close what our quotes bought there (the round trip
    # the rich-side cap below sizes it to).
    edge = max(cfg.min_half_spread, SIG_TICK)
    if bid is None and other_bid is not None and (other_bid <= fair.value - edge or own < 0):
        bid = other_bid if other_ask is None or other_bid < other_ask else None
    if ask is None and other_ask is not None and (other_ask >= fair.value + edge or own > 0):
        ask = other_ask if other_bid is None or other_ask > other_bid else None
    # every quote still resting on a side may fill before we hear of it (superseded ones rest until SIG
    # expires them, and SIG's live feed was silent for hours on 2026-10-02: SEN-ME D took 4,736 YES against
    # a 1,000 cap): a new quote only gets the room they leave
    resting_bid = sum(lq.remaining for lq in _as_list(bid_live))
    resting_ask = sum(lq.remaining for lq in _as_list(ask_live))
    bid_qty = int(min(cfg.size, cfg.max_inventory - inventory - resting_bid))
    ask_qty = int(min(cfg.size, cfg.max_inventory + inventory - resting_ask))
    if bid is not None and bid_qty > 0:
        rich = bid - fair.value  # mils above what a YES share is worth
        if rich > rich_guard(cfg, fair.value):
            # only to close NO we hold: the round trip books the spread, an open long would not
            closable = max(0.0, -own - resting_bid)
            if cost is not None and own < 0:
                closable = min(closable, _affordable(1000 - bid, cost, cushion))
            bid_qty = min(bid_qty, int(closable))
        elif rich > 0:
            bid_qty = int(bid_qty * cfg.rich_frac)  # buying YES above its fair: lean toward fair
        if bid_qty > 0:
            out[Side.YES] = Quote(Side.YES, bid, bid_qty)
    if ask is not None and ask_qty > 0:
        rich = fair.value - ask  # mils above what a NO share is worth
        if rich > rich_guard(cfg, 1000 - fair.value):
            closable = max(0.0, own - resting_ask)
            if cost is not None and own > 0:
                closable = min(closable, _affordable(ask, cost, cushion))
            ask_qty = min(ask_qty, int(closable))
        elif rich > 0:
            ask_qty = int(ask_qty * cfg.rich_frac)  # buying NO above its fair
        if ask_qty > 0:
            out[Side.NO] = Quote(Side.NO, ask, ask_qty)
    return out


BUYBACK_MIN_PROFIT = 15.0
"""Mils per share over average cost a buy-back past the fair guard must book. Closing a long-shot NO worth
99c at the band's bid gives up ~12c of edge to settlement for the band's spread; it only pays when the ask
rebuys the exposure and the spread is real. At cost or a tick over, the evening of 2026-10-03 lost ~3.5k at
fair this way (SEN-RI R 12k NO closed at +0.5c, SEN-DE R 12.8k at -1.3c) while the asks had not refilled."""


def _affordable(close_px: float, cost: float, cushion: float) -> float:
    """Shares a buy-back at ``close_px`` (what it pays per share of the held side) may close: all of them
    when it books at least BUYBACK_MIN_PROFIT over cost, else none. ``cushion`` (the market's booked P&L)
    is kept for callers that may lose: none do now."""
    del cushion
    return float("inf") if close_px >= cost + BUYBACK_MIN_PROFIT else 0.0


RICH_GUARD_REL = 0.5
"""A side is never bought for more than (1 + this) times its worth, however small ``fair_guard`` is next to
it: 4.5c for a 1.8c share is "within 3c of fair" and still a 150% premium (SEN-IL R, 2026-10-03, -135 on
one fill); 53c for a 50c share is the 6% the band usually costs."""


def rich_guard(cfg: QuoteConfig, side_worth: float) -> float:
    """Mils above its worth a side may be bought at (beyond that only our own inventory is closed)."""
    return min(float(cfg.fair_guard), max(float(SIG_TICK), side_worth * RICH_GUARD_REL))


def in_band(fair: Fair, band: Mils) -> bool:
    return band <= fair.value <= 1000 - band


@dataclass(frozen=True, slots=True)
class _Want:
    exchange_id: str
    side: Side
    quote: Quote
    cancel: LiveQuote | None = None
    """A live quote that must go now (at or through fair, or over the inventory cap); superseded ones
    just expire."""


class QuoteManager:
    def __init__(
        self,
        engine: Engine,
        executor: Executor,
        cfg: QuoteConfig,
        races: Mapping[str, Race],
        *,
        parties: Mapping[str, str] | None = None,
        on_activity: Callable[[str], None] | None = None,
        everything: bool = False,
        all_races: Mapping[str, Race] | None = None,
    ) -> None:
        """``races``: exchange id -> race for every named exchange; with ``everything`` every exchange of
        ``all_races`` whose fair value is inside the band is quoted too, after the named ones."""
        self._engine = engine
        self._executor = executor
        self._cfg = cfg
        self._races = dict(races)
        self._everything = everything
        self._all_races = dict(all_races or {})
        self.exchanges = list(races)
        self._parties = parties or {}
        self._on_activity = on_activity
        self.live: dict[tuple[str, Side], LiveQuote] = {}
        self.expiring: list[LiveQuote] = []
        """Superseded quotes left to expire: they still lock collateral and can still fill."""
        self.other_resting: Callable[[str, Side], Iterable[tuple[Mils, float]]] | None = None
        """Other strategies' resting buys on an exchange and side as (YES price, shares left): the ladder's
        rungs. SIG's book includes them, so without this a rung of ours one tick behind our quote would read
        as competition and drag the anchor; and they may fill too, so they use up inventory room."""
        self._bucket = TokenBucket(cfg.actions_per_s, capacity=4)
        self._wake = asyncio.Event()
        self._known: dict[int, float] = {}
        """Order id -> time every quote was placed (see ``order_ids``)."""
        self._quoting_now: set[str] = set()
        """Exchanges the last step found in scope, allowed and quotable (see ``is_quoting``)."""
        self._seen_fills: dict[tuple[int, str, float | None, float], None] = {}
        self._activity: dict[str, tuple[float, float]] = {}
        """exchange -> (decayed shares of recent prints, time of the last update): ranks ``all`` markets."""
        self.acquired: dict[str, float] = {}
        """Exchange -> shares our quotes bought net (YES positive, NO negative): the inventory the budget
        bounds. Positions other strategies opened on quoted markets (snipes held to settlement) are theirs."""
        self.quotes_placed = 0
        self.batches = 0
        self.shares_filled = 0.0
        self.unfunded = 0
        """Quotes wanted in the latest step that the collateral budget could not fit."""

    def race_of(self, exchange_id: str) -> Race | None:
        return self._races.get(exchange_id) or self._all_races.get(exchange_id)

    def wake(self, exchange_id: str, *_: object) -> None:
        """Book or fair-value change listener."""
        if exchange_id in self._races or (self._everything and exchange_id in self._all_races):
            self._wake.set()

    def allowed(self, exchange_id: str) -> bool:
        engine, risk = self._engine, self._engine.risk
        return (
            engine.in_trading_window()
            and self._executor.opening.accepting()
            and not risk.halted
            and not risk.error_limited
            and STRATEGY not in risk.paused_strategies
            and exchange_id not in risk.manual_paused
            and risk.book_is_fresh(exchange_id)
        )

    def on_trades(self, trades: Iterable[RtTrade]) -> None:
        """Public tape: ranks band markets by recent activity (shares printed: every share traded is a fill
        we could have had, whatever its price) for ``top_n``."""
        now = now_mono()
        for t in trades:
            shares, at = self._activity.get(t.exchange_id, (0.0, now))
            decay = 0.5 ** ((now - at) / ACTIVITY_HALF_LIFE_S)
            self._activity[t.exchange_id] = (shares * decay + t.quantity, now)

    def activity(self, exchange_id: str) -> float:
        notional, at = self._activity.get(exchange_id, (0.0, 0.0))
        return notional * 0.5 ** ((now_mono() - at) / ACTIVITY_HALF_LIFE_S) if notional else 0.0

    def scope(self) -> list[str]:
        """Exchanges to quote: named ones first, then (with ``all``) every exchange inside the band, the
        most active first (then Senate/Governor/control before House), cut to ``top_n`` when set."""
        out = list(self.exchanges)
        if self._everything:
            extra: list[tuple[float, int, str]] = []
            for ex, race in self._all_races.items():
                if ex in self._races:
                    continue
                fair = self._engine.fair(ex)
                if fair is not None and in_band(fair, self._cfg.band):
                    extra.append((-self.activity(ex), KIND_PRIORITY.get(race.kind, 9), ex))
            ranked = [ex for _, _, ex in sorted(extra)]
            out.extend(ranked[: self._cfg.top_n] if self._cfg.top_n else ranked)
        return out

    def resting(self, exchange_id: str, side: Side) -> list[tuple[Mils, float]]:
        """Our quotes resting on one side of an exchange as (YES price, shares left): live and superseded
        ones, for the engine's view of the book (``Engine.own_resting``)."""
        out = [
            (lq.quote.yes_price, lq.remaining)
            for lq in self.expiring
            if lq.exchange_id == exchange_id and lq.quote.side is side and lq.remaining > 0
        ]
        live = self.live.get((exchange_id, side))
        if live is not None and live.remaining > 0:
            out.append((live.quote.yes_price, live.remaining))
        return out

    def is_quoting(self, exchange_id: str) -> bool:
        """Whether the quoter is working this market: in scope, allowed and quotable as of the last step.
        Not "has a live quote right now": a quote cancelled at the anchor and not yet re-placed left a gap
        in which the recycler sold SEN-SD D's inventory under the quoter's own buy-back (2026-10-03 20:00)."""
        return exchange_id in self._quoting_now

    def order_ids(self) -> set[int]:
        """Ids of every quote placed within the last expiry + GONE_MEMORY_S, however it ended (filled,
        expired, cancelled at the anchor, released for budget): SIG's listing may still show any of them."""
        now = now_mono()
        limit = self._cfg.expiry_s + GONE_MEMORY_S
        self._known = {oid: at for oid, at in self._known.items() if now - at < limit}
        return set(self._known)

    def forget_exchange(self, exchange_id: str) -> None:
        """Another manager's cancel-all took our quotes on this exchange: drop them and quote again."""
        for key in [key for key in self.live if key[0] == exchange_id]:
            del self.live[key]
        self.expiring = [lq for lq in self.expiring if lq.exchange_id != exchange_id]
        self._wake.set()

    def _ours(self, exchange_id: str, side: Side) -> list[LiveQuote]:
        """Our resting orders on one side of an exchange: the live quote, superseded ones still expiring, and
        other strategies' rungs (as quotes that are never ours to cancel)."""
        out = [lq for lq in self.expiring if lq.exchange_id == exchange_id and lq.quote.side is side]
        live = self.live.get((exchange_id, side))
        if live is not None:
            out.append(live)
        if self.other_resting is not None:
            for yes_price, remaining in self.other_resting(exchange_id, side):
                if remaining > 0:
                    out.append(
                        LiveQuote(
                            Quote(side, yes_price, int(remaining)),
                            -1,
                            now_mono(),
                            exchange_id=exchange_id,
                        ),
                    )
        return out

    def wanted(self, exchange_id: str) -> dict[Side, Quote]:
        if not self.allowed(exchange_id):
            return {}
        engine = self._engine
        position = engine.account.position(exchange_id)
        holding = engine.account.holdings.get(exchange_id)
        # The whole position on a quoted market cycles at the band, not just what our quotes bought: a
        # snipe's NO carry bought at 87c is the same share as the one the ask just sold at 89c, the
        # buy-back bid at the band's bid closes either, and a restart forgets ``acquired`` anyway. What the
        # buy-back must not do is close a market at a net loss (Smart Score counts resolved markets won):
        # under cost it is sized to the market's booked gains.
        return desired_quotes(
            engine.fair(exchange_id),
            engine.raw_book(exchange_id),
            position,
            self._cfg,
            bid_live=self._ours(exchange_id, Side.YES),
            ask_live=self._ours(exchange_id, Side.NO),
            own=position,
            cost=holding.avg_cost * 1000 if holding is not None else None,
            cushion=engine.realized(exchange_id),
        )

    def reserve(self) -> float:
        return sum(lq.reserve for lq in self.live.values()) + sum(lq.reserve for lq in self.expiring)

    def inventory(self, scope: Iterable[str]) -> float:
        """Cost basis (SUSQies) of what our quotes acquired and still hold: per exchange the smaller of the
        shares our fills bought net and the position actually held (exits by other strategies may have sold
        some), at the holding's average cost."""
        holdings = self._engine.account.holdings
        total = 0.0
        for ex in scope:
            got, h = self.acquired.get(ex, 0.0), holdings.get(ex)
            if h is None or got == 0 or (got > 0) != (h.qty > 0):
                continue
            total += min(abs(got), abs(h.qty)) * h.avg_cost
        return total

    def budget(self) -> float:
        """Collateral quotes may rest in total: ``budget_frac`` of equity, and never more than
        ``cash_multiple`` times the cash less what the snipes may still deploy under the tier cap."""
        engine, account = self._engine, self._engine.account
        equity = engine.equity
        snipe_room = max(0.0, engine.risk.tier_def().deployed_frac * equity - account.deployed)
        return max(
            0.0,
            min(self._cfg.budget_frac * equity, account.cash * self._cfg.cash_multiple - snipe_room),
        )

    async def step(self) -> None:
        cfg, engine = self._cfg, self._engine
        engine.risk.check_rate()
        now = now_mono()
        self._drop_expired(now)
        wants: list[_Want] = []
        cancels: list[LiveQuote] = []
        scope = self.scope()
        # above the inventory budget only the side that reduces a market's position is quoted
        reduce_only = self.inventory(scope) >= cfg.inventory_frac * engine.equity
        quoting_now: set[str] = set()
        for exchange_id in scope:
            allowed = self.allowed(exchange_id)
            fair = engine.fair(exchange_id)
            quoting = allowed and quotable(fair, engine.raw_book(exchange_id))
            if quoting:
                quoting_now.add(exchange_id)
            want = self.wanted(exchange_id) if quoting else {}
            position = engine.account.position(exchange_id)
            if reduce_only:
                want = {
                    side: q
                    for side, q in want.items()
                    if (position < 0 and side is Side.YES) or (position > 0 and side is Side.NO)
                }
            anchor = None
            if quoting and fair is not None:
                anchor, _, _ = anchor_of(
                    fair,
                    engine.raw_book(exchange_id),
                    cfg,
                    bid_live=self._ours(exchange_id, Side.YES),
                    ask_live=self._ours(exchange_id, Side.NO),
                )
                # superseded quotes still rest: one the market has moved onto is bait like any other
                for old in [lq for lq in self.expiring if lq.exchange_id == exchange_id]:
                    if anchor is not None and _edge(old, anchor) <= 0:
                        self.expiring.remove(old)
                        cancels.append(old)
            for side in (Side.YES, Side.NO):
                key = (exchange_id, side)
                live, quote = self.live.get(key), want.get(side)
                if live is None:
                    if quote is not None:
                        wants.append(_Want(exchange_id, side, quote))
                    continue
                if not quoting:
                    cancels.append(self.live.pop(key))  # halt, pause, lost or shaky fair, settled: pull
                    continue
                if anchor is None:
                    continue  # a side of the book was swept away: no venue price now, let the quote rest
                capped = position >= cfg.max_inventory if side is Side.YES else position <= -cfg.max_inventory
                if _edge(live, anchor) <= 0 or capped:
                    # at or through the anchor (adverse-selection bait), or we hold enough that way: go now.
                    # Closer than min_half_spread but still on our side is left alone: in a 1c-wide market
                    # every tick of the other side would otherwise cost a cancel (a write) and a re-quote.
                    gone = self.live.pop(key)
                    if quote is not None:
                        wants.append(_Want(exchange_id, side, quote, cancel=gone))
                    else:
                        cancels.append(gone)
                    continue
                if quote is None:
                    continue  # no longer a price we would place, but still off the anchor: let it rest
                moved = abs(quote.yes_price - live.quote.yes_price)
                if moved >= cfg.requote_move or now - live.placed >= cfg.refresh_s:
                    self._supersede(key)  # the new quote goes in; the old one expires by itself
                    wants.append(_Want(exchange_id, side, quote))
        self._quoting_now = quoting_now
        for live in cancels:
            if not self._bucket.try_acquire():
                # try again next step (a superseded one goes back to expiring, a live one to its slot)
                if (live.exchange_id, live.quote.side) in self.live:
                    self.expiring.append(live)
                else:
                    self.live[(live.exchange_id, live.quote.side)] = live
                continue
            await self._executor.cancel_quote(live.order_id)
        await self._place(wants)

    def _drop_expired(self, now: float) -> None:
        """SIG expired these by itself; they lock nothing and cannot fill any more (their ids stay in
        ``order_ids`` a while longer: SIG's listing shows an expired quote for up to a minute)."""
        limit = self._cfg.expiry_s + 5.0
        self.expiring = [lq for lq in self.expiring if now - lq.placed < limit]
        for key, live in list(self.live.items()):
            if now - live.placed >= limit:
                del self.live[key]

    def _supersede(self, key: tuple[str, Side]) -> None:
        live = self.live.pop(key, None)
        if live is not None and live.remaining > 0:
            self.expiring.append(live)

    async def _place(self, wants: list[_Want]) -> None:
        """Pass every wanted quote through the risk gate, fit the collateral budget, send in batches."""
        cfg, engine = self._cfg, self._engine
        self.unfunded = 0
        budget = self.budget()
        reserved = self.reserve()
        fair_map, equity = engine.all_fair_map(), engine.equity
        approved: list[tuple[_Want, Intent]] = []
        for want in wants:
            fair = engine.fair(want.exchange_id)
            race = self.race_of(want.exchange_id)
            if fair is None or race is None:
                continue
            label = "bid" if want.side is Side.YES else "ask"
            edge = (
                fair.value - want.quote.yes_price
                if want.side is Side.YES
                else want.quote.yes_price - fair.value
            )
            intent = Intent(
                STRATEGY,
                race.key,
                (Leg(want.exchange_id, want.quote.side, want.quote.price, want.quote.quantity),),
                float(edge),
                TradeReason.MARKET_MAKE,
                f"{label} {want.quote.yes_price} (fair {fair.value})",
                now_mono(),
            )
            # resting=True: quotes skip the deployed-capital cap (see QuoteConfig.inventory_frac); the
            # race-loss, order-size, swing and daily-loss rules still apply
            ok, reason = engine.risk.check(
                intent,
                fair_map,
                equity,
                race=race,
                confidence=fair_confidence(fair),
                resting=True,
            )
            if ok is None:
                log.debug("quote %s %s not placed: %s", want.exchange_id, label, reason)
                continue
            cost = ok.capital
            position = engine.account.position(want.exchange_id)
            closing = position < 0 if want.side is Side.YES else position > 0
            if closing and ok.quantity <= abs(position):
                # a buy-back nets against the position it closes and frees capital (YES at 12c against NO
                # held returns 88c a share): it needs no budget. With cash at zero (2026-10-03 21:36: 696 of
                # cash, 115k deployed) the budget had stopped exactly the quotes that cycle capital.
                approved.append((want, ok))
                continue
            if reserved + cost > budget:
                # superseded quotes still lock collateral until SIG expires them (up to expiry_s): when the
                # budget is the limit, cancel the biggest ones to make room (a write each, bucket-limited)
                reserved -= await self._release(reserved + cost - budget)
                if reserved + cost > budget:
                    self.unfunded += 1
                    continue
            reserved += cost
            approved.append((want, ok))
        for start in range(0, len(approved), cfg.batch_size):
            chunk = approved[start : start + cfg.batch_size]
            if not self._bucket.try_acquire():
                self.unfunded += len(approved) - start
                break
            for want, _ in chunk:
                if want.cancel is not None:
                    await self._executor.cancel_quote(want.cancel.order_id)
            placed = await self._executor.place_ladder(
                [ok for _, ok in chunk],
                cfg.expiry_s,
                max_wait_s=BATCH_MAX_WAIT_S,
            )
            self.batches += 1
            if placed is None:
                engine.account.request_reconcile()
                continue
            for (want, ok), result in zip(chunk, placed, strict=True):
                if result is None:
                    continue
                self.quotes_placed += 1
                if result.filled:
                    self._filled(want.exchange_id, want.quote, result.filled, result.cost)
                if result.order_id is not None:
                    quote = Quote(want.quote.side, want.quote.yes_price, ok.quantity)
                    self.live[(want.exchange_id, want.side)] = LiveQuote(
                        quote,
                        result.order_id,
                        now_mono(),
                        traded=result.filled,
                        exchange_id=want.exchange_id,
                    )
                    self._known[result.order_id] = now_mono()

    def stats(self) -> dict[str, object]:
        scope = self.scope()
        return {
            "live": len(self.live),
            "expiring": len(self.expiring),
            "exchanges": len({ex for ex, _ in self.live}),
            "scope": len(scope),
            "reserved": round(self.reserve()),
            "budget": round(self.budget()),
            "inventory": round(self.inventory(scope)),
            "inventory_budget": round(self._cfg.inventory_frac * self._engine.equity),
            "placed": self.quotes_placed,
            "batches": self.batches,
            "shares_filled": round(self.shares_filled),
            "unfunded": self.unfunded,
        }

    async def _release(self, needed: float) -> float:
        """Cancel superseded quotes, biggest collateral first, until ``needed`` SUSQies are freed or the
        action bucket runs dry. Returns the collateral released."""
        freed = 0.0
        for old in sorted(self.expiring, key=lambda lq: lq.reserve, reverse=True):
            if freed >= needed or old.reserve <= 0:
                break
            if not self._bucket.try_acquire():
                break
            self.expiring.remove(old)
            await self._executor.cancel_quote(old.order_id)
            freed += old.reserve
        return freed

    def on_account_batch(self, batch: AccountBatch) -> None:
        """Fills on resting quotes arrive here (the account reconciles positions separately). Orders that
        changed state come as ``orderUpdates`` (cumulative); quotes that only filled come as plain
        ``fills`` (one row per execution, repeated across batches by SIG: deduplicated)."""
        updated: set[int] = set()
        for update in batch.order_updates:
            for key, live in list(self.live.items()):
                if live.order_id != update.order_id:
                    continue
                updated.add(update.order_id)
                self._advance(key[0], live, update.quantity_traded, update.total_cost)
                if not update.open:
                    self.live.pop(key, None)
                self._wake.set()
            for live in list(self.expiring):
                if live.order_id == update.order_id:
                    updated.add(update.order_id)
                    self._advance(live.exchange_id, live, update.quantity_traded, update.total_cost)
                    if not update.open:
                        self.expiring.remove(live)
        for fill in batch.fills:
            if fill.order_id is None or fill.order_id in updated:
                continue
            key = (fill.order_id, fill.executed_at, fill.price, fill.quantity)
            if key in self._seen_fills:
                continue
            for exchange_id, live in self._all_live():
                if live.order_id != fill.order_id:
                    continue
                self._seen_fills[key] = None
                while len(self._seen_fills) > 5000:
                    del self._seen_fills[next(iter(self._seen_fills))]
                # a fill without a price (spec 2026-10-03) is booked at the quote's own price: it filled at
                # that limit or better, and the account reconciles the exact cost
                price = fill.price if fill.price is not None else live.quote.yes_price / 1000
                side_price = price if live.quote.side is Side.YES else 1 - price
                self._advance(
                    exchange_id,
                    live,
                    live.traded + fill.quantity,
                    (live.traded + fill.quantity) * side_price,
                )
                if live.remaining <= 0:
                    self._forget(live)
                self._wake.set()

    def _all_live(self) -> list[tuple[str, LiveQuote]]:
        out = [(key[0], live) for key, live in self.live.items()]
        out.extend((lq.exchange_id, lq) for lq in self.expiring)
        return out

    def _forget(self, live: LiveQuote) -> None:
        for key, lq in list(self.live.items()):
            if lq is live:
                del self.live[key]
        if live in self.expiring:
            self.expiring.remove(live)

    def _advance(self, exchange_id: str, live: LiveQuote, traded: float, total_cost: float) -> None:
        """Bring a quote's filled quantity up to ``traded`` (cumulative) and report the new part."""
        new = traded - live.traded
        if new <= 0:
            return
        cost = total_cost * new / traded if traded else 0.0
        before = live.traded
        live.traded = traded
        self._filled(exchange_id, live.quote, new, cost)
        log.info("quote %s filled %s (%s of %s)", live.order_id, new, before + new, live.quote.quantity)

    def _filled(self, exchange_id: str, quote: Quote, qty: float, cost: float) -> None:
        self.shares_filled += qty
        self.acquired[exchange_id] = self.acquired.get(exchange_id, 0.0) + (
            qty if quote.side is Side.YES else -qty
        )
        if self._on_activity is None:
            return
        fair = self._engine.fair(exchange_id)
        race = self.race_of(exchange_id)
        party = self._parties.get(exchange_id, exchange_id)
        # a quote is a BUY of its side; the ask (buying NO) reads as selling YES at the ask
        yes = quote.side is Side.YES
        trade = f"{qty:,.0f} YES @ {cents(quote.yes_price)}"
        fair_text = "" if fair is None else f"fair {cents(fair.value)}"
        self._on_activity(
            activity_line(
                STRATEGY,
                "BUY" if yes else "SELL",
                f"{race.key if race else exchange_id} {party}",
                trade,
                fair_text,
                f"cost {cost:,.0f}",
                cash=self._engine.account.cash,
            ),
        )

    async def cancel_all(self) -> None:
        for live in [*self.live.values(), *self.expiring]:
            await self._executor.cancel_quote(live.order_id)
        self.live.clear()
        self.expiring.clear()

    async def run(self, stop: asyncio.Event) -> None:
        cfg = self._cfg
        extra = ""
        if self._everything:
            extra = f" + the {cfg.top_n} most active" if cfg.top_n else " + every"
            extra += " market inside the band"
        log.info(
            "flow trading on %d named exchanges%s: anchor +/- %.1fc, %d shares, inventory cap %d, "
            "collateral budget %.0f%% of equity, batches of %d",
            len(self.exchanges),
            extra,
            cfg.half_spread / 10,
            self._cfg.size,
            self._cfg.max_inventory,
            self._cfg.budget_frac * 100,
            self._cfg.batch_size,
        )
        while not stop.is_set():
            await wait_event_or_stop(self._wake, stop, 1.0)
            self._wake.clear()
            if stop.is_set():
                break
            try:
                await self.step()
            except Exception:
                log.exception("quoting step failed")
        await self.cancel_all()


def resolve_markets(names: Sequence[str], races: Sequence[Race]) -> dict[str, Race]:
    """MM_MARKETS entries (race keys or exchange ids) -> exchange id -> race. ``all`` is handled by the
    caller (see ``wants_all``)."""
    by_key = {race.key: race for race in races}
    by_exchange = {o.sig_exchange_id: race for race in races for o in race.outcomes}
    out: dict[str, Race] = {}
    for name in names:
        if name.lower() == ALL:
            continue
        if name in by_key:
            out.update({o.sig_exchange_id: by_key[name] for o in by_key[name].outcomes})
        elif name in by_exchange:
            out[name] = by_exchange[name]
        else:
            log.warning("MM_MARKETS: %r is neither a race key nor an exchange id; ignored", name)
    return out


def wants_all(names: Sequence[str]) -> bool:
    return any(name.lower() == ALL for name in names)
