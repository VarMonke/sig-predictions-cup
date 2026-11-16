"""SIG-side state: order books for every tournament exchange, and our account (cash, holdings, orders).

Books come from three sources, cheapest first:
  * realtime ``tournament:{id}:market:{id}`` batches push the complete, versioned book (``asOf``) of every
    exchange they touch; a pushed book is applied only when newer than the one held,
  * a full bulk top-of-book scan (3 requests for all 237 exchanges) every ``scan_every_s`` keeps quiet books
    provably fresh for the stale-book rule and catches changes that emit nothing (order expiry);
    every ``fast_scan_s`` while the feed is quiet. Trades/changes without a pushed book, gaps and
    ``resyncRequired`` mark books dirty for a coalesced bulk refresh,
  * full depth (sizes, which arbitrage needs) is fetched for one depth-less book every ``depth_sync_s``.
Books are YES-normalized. REST is the source of truth for the account; realtime fills trigger reconciles.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import NamedTuple

from sigbot.core.db import Recorder
from sigbot.core.util import Mils, now_mono, parse_server_ts, to_mils, utcnow, wait_event_or_stop
from sigbot.sigapi import SigApiError, SigClient
from sigbot.sigapi import models as m

log = logging.getLogger(__name__)

UNKNOWN_QTY = -1.0


class Level(NamedTuple):
    price: Mils
    qty: float
    """Shares available; UNKNOWN_QTY when only the price is known (bulk scan)."""


@dataclass(slots=True)
class Book:
    exchange_id: str
    market_id: str
    bids: list[Level] = field(default_factory=list)
    asks: list[Level] = field(default_factory=list)
    depth_known: bool = False
    ts: float = 0.0
    settled: bool = False
    version: tuple[int, datetime] | None = None
    """Engine version (``asOf``: sequence, then time) of the last complete book applied."""
    pushed: float = 0.0
    """Monotonic time the last complete, versioned book was applied."""

    @property
    def bid(self) -> Level | None:
        return self.bids[0] if self.bids else None

    @property
    def ask(self) -> Level | None:
        return self.asks[0] if self.asks else None

    def top(self) -> tuple[Mils | None, Mils | None]:
        return (self.bids[0].price if self.bids else None, self.asks[0].price if self.asks else None)


type BookListener = Callable[[str], None]
type OwnResting = Callable[[str, m.Side], Iterable[tuple[Mils, float]]]
"""Our own resting BUY orders of a side on an exchange, as (YES price, shares left): what the quoter and the
ladder have on the book. SIG's books include them, and a strategy that takes them for liquidity trades with
itself: on 2026-10-03 the exits sent 139 orders in 12 minutes into the quoter's own ask (every one cancelled
by self-trade prevention, two writes each) before this existed."""

EPOCH = datetime.min.replace(tzinfo=UTC)


def strip_own(levels: Sequence[Level], own: Iterable[tuple[Mils, float]]) -> list[Level]:
    """One side of SIG's book without our own resting orders: a level at one of our prices loses our shares,
    and disappears when its size is unknown or no bigger than ours."""
    mine: dict[Mils, float] = {}
    for price, remaining in own:
        if remaining > 0:
            mine[price] = mine.get(price, 0.0) + remaining
    if not mine:
        return list(levels)
    out: list[Level] = []
    for lv in levels:
        ours = mine.get(lv.price)
        if ours is None:
            out.append(lv)
        elif lv.qty >= 0 and lv.qty > ours + 1e-9:
            out.append(Level(lv.price, lv.qty - ours))
    return out


def without_own(book: Book, yes: Iterable[tuple[Mils, float]], no: Iterable[tuple[Mils, float]]) -> Book:
    """A copy of ``book`` with our resting YES buys taken out of the bids and our NO buys (YES asks, given in
    YES terms) out of the asks."""
    return Book(
        book.exchange_id,
        book.market_id,
        strip_own(book.bids, yes),
        strip_own(book.asks, no),
        book.depth_known,
        book.ts,
        book.settled,
        book.version,
        book.pushed,
    )


def book_version(version: m.BookVersion | None) -> tuple[int, datetime] | None:
    """Comparable ``asOf``: by sequence, then time. None when the book carries no engine version."""
    if version is None or version.sequence is None:
        return None
    return version.sequence, parse_server_ts(version.at) or EPOCH


def market_of(topic: str) -> str | None:
    """``tournament:{id}:market:{market_id}`` -> market_id."""
    return topic.rsplit(":market:", 1)[1] if ":market:" in topic else None


class SigBooks:
    def __init__(
        self,
        sig: SigClient,
        exchanges: dict[str, str],
        rec: Recorder,
        *,
        coalesce_s: float = 1.0,
        scan_every_s: float = 60.0,
        depth: int = 20,
        depth_min_interval_s: float = 3.0,
        observe: Callable[[str, str, float], None] | None = None,
        fast_scan_s: float = 5.0,
        silent_after_s: float = 30.0,
        broken_after_s: float = 600.0,
        depth_sync_s: float = 6.0,
        on_alert: Callable[[str], None] | None = None,
    ) -> None:
        """The feed counts as broken (alerted once, and again when it returns) after ``broken_after_s``
        without a market batch while scans kept finding book changes that it should have announced."""
        self._sig = sig
        self._rec = rec
        self._observe = observe
        self._on_alert = on_alert
        self._coalesce_s = coalesce_s
        self._scan_every_s = scan_every_s
        self._fast_scan_s = fast_scan_s
        self._silent_after_s = silent_after_s
        self._broken_after_s = broken_after_s
        self._started = now_mono()
        self.last_batch_mono = float("-inf")
        self.polled_changes = 0
        """Top-of-book changes found by scans since the last market batch."""
        self.pushed_books = 0
        self._feed_alerted = False
        self._seen_trades: dict[tuple[str, str, float, float, int | None], None] = {}
        """Recent trade prints already recorded: SIG replayed market batches 6-11x during 2026-10-02
        03:00-07:00 UTC, which inflated ``sig_trades`` (and every volume number read off it) the same way."""
        self._depth = depth
        self._depth_min_interval_s = depth_min_interval_s
        self._depth_sync_s = depth_sync_s
        self.books: dict[str, Book] = {ex: Book(ex, mk) for ex, mk in exchanges.items()}
        self._by_market: defaultdict[str, list[str]] = defaultdict(list)
        for ex, mk in exchanges.items():
            self._by_market[mk].append(ex)
        self._dirty: set[str] = set()
        self._wake = asyncio.Event()
        self._scan_now = asyncio.Event()
        self._listeners: list[BookListener] = []
        self._last_depth: dict[str, float] = {}
        self._depth_locks: dict[str, asyncio.Lock] = {}
        self.last_scan = 0.0
        self._last_hint_refresh = float("-inf")
        self.bulk_requests = 0
        self.depth_requests = 0

    def subscribe(self, listener: BookListener) -> None:
        self._listeners.append(listener)

    def _notify(self, exchange_id: str) -> None:
        for listener in self._listeners:
            listener(exchange_id)

    def on_market_batch(self, topic: str, batch: m.MarketBatch) -> list[m.RtTrade]:
        """Apply a pushed batch; returns the trade prints not seen before (SIG replays batches)."""
        now = utcnow()
        fresh: list[m.RtTrade] = []
        self.last_batch_mono, self.polled_changes = now_mono(), 0
        if self._feed_alerted:
            self._feed_alerted = False
            log.warning("SIG live feed is back")
            self._alert("SIG live feed is back: books are pushed again")
        pushed: set[str] = set()
        for b in batch.books:
            exchange_id = str(b.exchange_id)
            self.apply_book(exchange_id, book_version(b.as_of), b.bids, b.asks)
            pushed.add(exchange_id)
            self.pushed_books += 1
            book = self.books.get(exchange_id)
            self._rec.write(
                "book_events",
                (
                    now,
                    parse_server_ts(b.as_of.at if b.as_of else None),
                    "book",
                    exchange_id,
                    book and book.market_id,
                ),
            )
        for t in batch.trades:
            key = (t.exchange_id, t.id or t.executed_at, t.price, t.quantity, t.sequence)
            if key not in self._seen_trades:
                self._seen_trades[key] = None
                while len(self._seen_trades) > 20_000:
                    del self._seen_trades[next(iter(self._seen_trades))]
                fresh.append(t)
                self._rec.write(
                    "sig_trades",
                    (now, t.exchange_id, t.market_id, t.price, t.quantity, parse_server_ts(t.executed_at)),
                )
            if t.exchange_id not in pushed or t.sequence is None:
                self.mark_dirty(t.exchange_id)
        for d in batch.book_dirty:
            server_ts = parse_server_ts(d.at)
            self._rec.write("book_events", (now, server_ts, "dirty", d.exchange_id, d.market_id))
            if server_ts is not None and self._observe is not None:
                self._observe("sig", "feed", (now - server_ts).total_seconds() * 1000)
            if d.exchange_id not in pushed:
                self.mark_dirty(d.exchange_id)
        if batch.resync_required and (market_id := market_of(topic)) is not None:
            self.resync_market(market_id)
        for s in batch.market_settled:
            self._rec.write("book_events", (now, parse_server_ts(s.at), "settled", None, s.market_id))
            for book in self.books.values():
                if book.market_id == s.market_id:
                    book.settled = True
                    self._notify(book.exchange_id)
            self._rec.event(
                "market_settled",
                f"market {s.market_id} settled with {s.settled_with}",
                data={"market_id": s.market_id, "settled_with": s.settled_with},
            )
        return fresh

    def mark_dirty(self, exchange_id: str) -> None:
        if exchange_id in self.books:
            self._dirty.add(exchange_id)
            self._wake.set()

    def resync(self, min_interval_s: float = 30.0) -> None:
        """Rescan every book after a realtime (re)join or gap; at most once per ``min_interval_s``."""
        if now_mono() - self.last_scan >= min_interval_s:
            self._scan_now.set()

    def resync_market(self, market_id: str) -> None:
        """Refresh one market's books (after its channel joins, a gap, a token refresh or resyncRequired)."""
        for exchange_id in self._by_market.get(market_id, ()):
            self.mark_dirty(exchange_id)

    def apply_book(
        self,
        exchange_id: str,
        version: tuple[int, datetime] | None,
        bids: Iterable[m.BookLevel],
        asks: Iterable[m.BookLevel],
    ) -> bool:
        """A complete book (realtime push or REST depth), applied only when newer than the one held: a held
        book without a version, or a pushed one without, is always replaced."""
        book = self.books.get(exchange_id)
        if book is None:
            return False
        if version is not None and book.version is not None and version <= book.version:
            return False
        self._apply(
            exchange_id,
            sorted((Level(to_mils(lv.price), lv.quantity) for lv in bids), key=lambda lv: -lv.price),
            sorted((Level(to_mils(lv.price), lv.quantity) for lv in asks), key=lambda lv: lv.price),
            depth_known=True,
        )
        if version is not None:
            book.version = version
        book.pushed = now_mono()
        return True

    async def scan_all(self) -> None:
        """Top of book for every exchange (3 requests for 237 exchanges). ``last_scan`` is when it started,
        so scans keep their interval however long the requests take."""
        started = now_mono()
        await self._refresh_tops(list(self.books), from_scan=True)
        self.last_scan = started

    @property
    def feed_quiet_s(self) -> float:
        """Seconds since the last market batch (or since start, before the first one)."""
        return now_mono() - max(self.last_batch_mono, self._started)

    @property
    def feed_silent(self) -> bool:
        """SIG's live feed has been quiet long enough that the books are on the fast poll."""
        return self.feed_quiet_s >= self._silent_after_s

    @property
    def feed_broken(self) -> bool:
        return self.feed_quiet_s >= self._broken_after_s and self.polled_changes > 0

    def scan_interval(self) -> float:
        return self._fast_scan_s if self.feed_quiet_s >= self._silent_after_s else self._scan_every_s

    def _check_feed(self) -> None:
        if self._feed_alerted or not self.feed_broken:
            return
        self._feed_alerted = True
        minutes = self.feed_quiet_s / 60
        log.warning(
            "SIG live feed silent for %.0f min while polling saw %d book changes; polling every %.0fs",
            minutes,
            self.polled_changes,
            self._fast_scan_s,
        )
        self._alert(
            f"SIG live feed SILENT for {minutes:.0f} min while polling saw {self.polled_changes} book "
            f"changes. Books are still polled every {self._fast_scan_s:.0f}s, so trading continues.",
        )

    def _alert(self, message: str) -> None:
        if self._on_alert is not None:
            self._on_alert(message)

    async def _refresh_tops(self, exchange_ids: list[str], *, from_scan: bool = False) -> None:
        """One request per 100 exchanges, each applied on its own: when SIG answers 503 to one chunk of a
        scan the other 200 books still get their refresh (2026-10-11: intermittent 503s failed whole scans,
        most books went stale and the gate turned rungs away all evening)."""
        chunks = [exchange_ids[i : i + 100] for i in range(0, len(exchange_ids), 100)]
        await asyncio.gather(*(self._refresh_chunk(chunk, from_scan=from_scan) for chunk in chunks))

    async def _refresh_chunk(self, exchange_ids: list[str], *, from_scan: bool) -> None:
        sent = now_mono()
        try:
            bulk = await self._sig.bulk_prices(exchange_ids)
        except SigApiError as exc:
            log.warning("bulk price refresh (%d exchanges) failed: %s", len(exchange_ids), exc)
            for ex in exchange_ids:  # try again on the next cycle
                self.mark_dirty(ex)
            return
        self.bulk_requests += 1
        for p in bulk.data:
            book = self.books.get(p.exchange_id)
            if book is None or book.pushed > sent:  # a newer complete book arrived while this was in flight
                continue
            bid = to_mils(p.best_bid) if p.best_bid is not None else None
            ask = to_mils(p.best_ask) if p.best_ask is not None else None
            if (bid, ask) == book.top():
                # Same prices: still current. A hint means sizes may have moved, so mark them stale.
                if not from_scan:
                    book.depth_known = False
                book.ts = now_mono()
                continue
            if from_scan and book.ts > 0:
                self.polled_changes += 1
            self._apply(
                p.exchange_id,
                [Level(bid, UNKNOWN_QTY)] if bid is not None else [],
                [Level(ask, UNKNOWN_QTY)] if ask is not None else [],
                depth_known=False,
            )

    async def fetch_depth(self, exchange_id: str, depth: int | None = None) -> Book:
        """Full depth (sizes) for one exchange. Cached for ``depth_min_interval_s`` to protect the budget.
        ``depth``: levels per side (default 20; sweep ladders ask for more to see the crowd behind the
        near-fair layer)."""
        book = self.books[exchange_id]
        lock = self._depth_locks.setdefault(exchange_id, asyncio.Lock())
        async with lock:
            if (
                book.depth_known
                and now_mono() - self._last_depth.get(exchange_id, 0.0) < self._depth_min_interval_s
            ):
                return book
            self._last_depth[exchange_id] = now_mono()
            try:
                ob = await self._sig.exchange_orderbook(exchange_id, depth=depth or self._depth)
            except SigApiError as exc:
                log.warning("orderbook %s fetch failed: %s", exchange_id, exc)
                return book
            self.depth_requests += 1
            self.apply_book(exchange_id, book_version(ob.as_of), ob.bids, ob.asks)
            return book

    async def _depth_loop(self, stop: asyncio.Event) -> None:
        """Fill in sizes for books that lack them (arbitrage needs them), the longest-waiting first."""
        while not stop.is_set():
            await wait_event_or_stop(stop, stop, self._depth_sync_s)
            missing = [ex for ex, b in self.books.items() if not b.depth_known and not b.settled]
            if missing and not stop.is_set():
                await self.fetch_depth(min(missing, key=lambda ex: self._last_depth.get(ex, 0.0)))

    def _apply(self, exchange_id: str, bids: list[Level], asks: list[Level], *, depth_known: bool) -> None:
        book = self.books[exchange_id]
        changed = bids != book.bids or asks != book.asks
        top_changed = [lv.price for lv in bids[:1]] != [lv.price for lv in book.bids[:1]] or [
            lv.price for lv in asks[:1]
        ] != [lv.price for lv in book.asks[:1]]
        book.bids, book.asks, book.depth_known, book.ts = bids, asks, depth_known, now_mono()
        if top_changed:
            b, a = bids[0] if bids else None, asks[0] if asks else None
            self._rec.write(
                "sig_tob",
                (utcnow(), exchange_id, b and b.price / 1000, b and b.qty, a and a.price / 1000, a and a.qty),
            )
        if changed:
            self._notify(exchange_id)

    async def run(self, stop: asyncio.Event) -> None:
        await asyncio.gather(self._dirty_loop(stop), self._scan_loop(stop), self._depth_loop(stop))

    async def _dirty_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await wait_event_or_stop(self._wake, stop, 5.0)
            self._wake.clear()
            if not self._dirty or stop.is_set():
                continue
            # Fetch at once after a quiet spell; otherwise let hints accumulate briefly so one request
            # covers every book that changed (at most one hint refresh per coalesce_s).
            wait = self._coalesce_s - (now_mono() - self._last_hint_refresh)
            if wait > 0:
                await asyncio.sleep(wait)
            batch = list(self._dirty)[:100]
            self._dirty.difference_update(batch)
            self._last_hint_refresh = now_mono()
            await self._refresh_tops(batch)
            if self._dirty:
                self._wake.set()

    @property
    def scan_tick(self) -> float:
        """How often the scan loop checks whether a scan is due: several times per interval, since waking once
        per interval skips a whole interval whenever a timer fires a hair early (production polled every ~10s
        instead of 5s that way)."""
        return min(1.0, min(self._fast_scan_s, self._scan_every_s) / 5)

    async def _scan_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await wait_event_or_stop(self._scan_now, stop, self.scan_tick)
            forced = self._scan_now.is_set()
            self._scan_now.clear()
            if stop.is_set():
                break
            self._check_feed()
            if forced or now_mono() - self.last_scan >= self.scan_interval():
                await self.scan_all()


@dataclass(slots=True)
class Holding:
    exchange_id: str
    qty: float
    """Signed shares: + YES, - NO."""
    avg_cost: float
    cost_basis: float
    sig_price: float | None = None
    market_value: float = 0.0


class AccountState:
    """Cash/holdings/orders. A reconcile costs 3+ requests, so it runs every ``reconcile_every_s`` and on
    demand (after fills or unknown order outcomes), never more often than ``min_interval_s``.

    ``paper=True`` (shadow mode): the first reconcile seeds cash/holdings from SIG, after which the
    portfolio evolves only through simulated fills. Later reconciles refresh tournament metadata only,
    otherwise SIG's real (untouched) account would wipe the simulated positions every minute.

    SIG's position read can lag its own fills by seconds under load (at the open it reported a position we
    had just closed, and the bot re-sent the exit). A holding that we filled within ``fill_settle_s`` is
    therefore kept as we track it when SIG disagrees, and another reconcile is scheduled.

    While SIG's account feed is silent (``feed_silent`` says so; SIG paused Realtime for six hours on
    2026-10-02) fills against resting quotes and rungs are only learnt from reconciles, so they run every
    ``reconcile_silent_s`` instead of ``reconcile_every_s`` (3 requests each; the books poll faster then too).
    """

    def __init__(
        self,
        sig: SigClient,
        slug: str,
        rec: Recorder,
        *,
        paper: bool = False,
        reconcile_every_s: float = 60.0,
        reconcile_silent_s: float = 20.0,
        min_interval_s: float = 15.0,
        fill_settle_s: float = 20.0,
        on_drift: Callable[[str], None] | None = None,
        on_meta: Callable[[str, str | None, str | None], None] | None = None,
    ) -> None:
        """``on_meta(status, start_date, end_date)`` receives the tournament metadata of every sync."""
        self._sig = sig
        self._slug = slug
        self._rec = rec
        self.paper = paper
        self._on_drift = on_drift
        self._on_meta = on_meta
        self._seeded = False
        self._logged_no_position = False
        self._fill_settle_s = fill_settle_s
        self._last_fill: dict[str, float] = {}
        self._pre_fill: dict[str, float] = {}
        """exchange -> our quantity before the first of the fills still settling."""
        self._orders_applied: dict[int, tuple[str, m.Side, float, float]] = {}
        """order id -> (exchange, side, shares the placement response reported and we already applied,
        shares the feed may still add: the order's size minus everything applied so far)."""
        self._early_fills: dict[int, list[m.RtFill]] = {}
        self._seen_fills: dict[tuple[int, str, float, float], None] = {}
        """Feed fills already applied, by (order, executedAt, price, quantity): SIG's account feed repeats
        fills across batches (one 346-share fill arrived 142 times in two hours on 2026-10-02)."""
        self._every = reconcile_every_s
        self._every_silent = reconcile_silent_s
        self._min_interval = min_interval_s
        self.feed_silent: Callable[[], bool] = lambda: False
        """Whether SIG's live feed is currently silent (wired to ``SigBooks.feed_silent`` by main)."""
        self.cash = 0.0
        self.initial_balance = 0.0
        self.holdings: dict[str, Holding] = {}
        self.realized: dict[str, float] = {}
        """Realized P&L per exchange: netted YES/NO pairs and settlements."""
        self.open_orders: dict[int, m.Order] = {}
        self.open_orders_at = 0.0
        """When the listing in ``open_orders`` was requested (monotonic); see ``reconcile``."""
        self.last_reconcile = 0.0
        self.tournament_status = ""
        self.start_date: str | None = None
        self.end_date: str | None = None
        self._soon = asyncio.Event()
        self._lock = asyncio.Lock()

    def request_reconcile(self) -> None:
        self._soon.set()

    def _update_meta(self, tour: m.Tournament) -> None:
        self.initial_balance = tour.initial_balance
        self.tournament_status = tour.status
        self.start_date, self.end_date = tour.start_date, tour.end_date
        if self._on_meta is not None:
            self._on_meta(tour.status, tour.start_date, tour.end_date)

    async def reconcile(self) -> None:
        async with self._lock:
            if self.paper and self._seeded:
                self._update_meta(await self._sig.tournament(self._slug))
                self.last_reconcile = now_mono()
                return
            tour, pos = await asyncio.gather(
                self._sig.tournament(self._slug),
                self._sig.positions(self._slug),
            )
            # the listing's age is counted from when its first request went out (the budget may hold the
            # pages back for many seconds): an order placed after that may well be missing from it
            listed_at = now_mono()
            orders = [o async for o in self._sig.iter_orders()]
            self._update_meta(tour)
            self.cash = tour.my_balance
            fresh = {p.exchange_id: p for p in pos.positions if not p.settled and p.quantity != 0}
            settling = {ex for ex in self._settling() if self._sig_lags(ex, fresh.get(ex))}
            if self._seeded:
                self._check_drift(fresh, settling)
            self._log_first_no_position(fresh)
            holdings = {
                ex: Holding(
                    ex,
                    p.quantity,
                    p.cost_basis / abs(p.quantity),
                    p.cost_basis,
                    p.current_price,
                    p.market_value,
                )
                for ex, p in fresh.items()
            }
            for ex in settling:
                mine, theirs = self.holdings.get(ex), holdings.get(ex)
                if self._differs(mine, theirs):
                    log.info(
                        "%s: SIG shows %s, keeping our just-filled %s until it settles",
                        ex,
                        theirs,
                        mine,
                    )
                    if mine is None:
                        holdings.pop(ex, None)
                    else:
                        holdings[ex] = mine
                    self._soon.set()
            self.holdings = holdings
            self.open_orders = {o.id: o for o in orders}
            self.open_orders_at = listed_at
            self._seeded = True
            self.last_reconcile = now_mono()

    def settling_exchanges(self) -> set[str]:
        """Exchanges with a fill so recent that their position is still being confirmed."""
        return self._settling()

    def _settling(self) -> set[str]:
        """Exchanges we filled on so recently that SIG's position read may not show the fill yet."""
        cutoff = now_mono() - self._fill_settle_s
        return {ex for ex, at in self._last_fill.items() if at > cutoff}

    def _sig_lags(self, exchange_id: str, theirs: m.Position | None) -> bool:
        """SIG shows a quantity our own recent fills passed through: it has not caught up yet. Anything
        else (a remainder that filled after its response, an order whose response never came) is news."""
        mine = self.holdings.get(exchange_id)
        q_mine, q_sig = (mine.qty if mine else 0.0), (theirs.quantity if theirs else 0.0)
        before = self._pre_fill.get(exchange_id, q_mine)
        return min(before, q_mine) - 0.5 <= q_sig <= max(before, q_mine) + 0.5

    @staticmethod
    def _differs(mine: Holding | None, theirs: Holding | None) -> bool:
        q_mine, c_mine = (mine.qty, mine.cost_basis) if mine else (0.0, 0.0)
        q_sig, c_sig = (theirs.qty, theirs.cost_basis) if theirs else (0.0, 0.0)
        return abs(q_mine - q_sig) > 0.5 or abs(c_mine - c_sig) > max(1.0, 0.02 * max(c_mine, c_sig))

    def _check_drift(
        self,
        fresh: dict[str, m.Position],
        skip: frozenset[str] | set[str] = frozenset(),
    ) -> None:
        """Compare what we believed (fills applied since the last reconcile) with SIG's positions. Every
        difference is recorded; only material ones (> 10 shares and > 1%, or > 5 SUSQies and > 2% of cost)
        alert, since a few shares of a remainder filling after the response is normal at SIG's latency."""
        problems: list[str] = []
        material = False
        for ex in sorted((set(self.holdings) | set(fresh)) - skip):
            mine, theirs = self.holdings.get(ex), fresh.get(ex)
            q_mine, c_mine = (mine.qty, mine.cost_basis) if mine else (0.0, 0.0)
            q_sig, c_sig = (theirs.quantity, theirs.cost_basis) if theirs else (0.0, 0.0)
            if abs(q_mine - q_sig) > 0.5 and self._book_orphan_fills(ex, q_mine, q_sig):
                mine = self.holdings.get(ex)
                q_mine, c_mine = (mine.qty, mine.cost_basis) if mine else (0.0, 0.0)
            dq, dc = abs(q_mine - q_sig), abs(c_mine - c_sig)
            if dq > 0.5 or dc > max(1.0, 0.02 * max(c_mine, c_sig)):
                big = dq > max(10.0, 0.01 * max(abs(q_mine), abs(q_sig))) or dc > max(
                    5.0,
                    0.02 * max(c_mine, c_sig),
                )
                material = material or big
                problems.append(
                    f"{ex}: bot {q_mine:+.0f} sh / {c_mine:.2f} vs SIG {q_sig:+.0f} sh / {c_sig:.2f}",
                )
                self._rec.event(
                    "position_drift",
                    problems[-1],
                    level="warning" if big else "info",
                    data={
                        "exchange_id": ex,
                        "bot_qty": q_mine,
                        "bot_cost": c_mine,
                        "sig_qty": q_sig,
                        "sig_cost": c_sig,
                    },
                )
        if problems:
            log.log(
                logging.WARNING if material else logging.INFO,
                "position drift vs SIG: %s",
                "; ".join(problems),
            )
            if material and self._on_drift is not None:
                self._on_drift("POSITION MISMATCH, adopting SIG's numbers: " + "; ".join(problems[:5]))

    def _book_orphan_fills(self, exchange_id: str, q_mine: float, q_sig: float) -> float:
        """Fills on orders that never got a placement response (a batch whose outcome SIG left unknown and
        then executed) sit in ``_early_fills`` with no side. When SIG's position differs from ours by what
        those fills add up to, the drift tells the side, and the fills are booked like any other: the
        realized P&L is ours either way (2026-10-05 12:53: a flip sold 7,916 SEN-MA D at 90.5c that way;
        the reconcile adopted the position but +277 of profit was never booked). Returns the shares booked;
        0 when the orphans do not explain the drift (then it is adopted as before)."""
        orphans = [
            (oid, f)
            for oid, fills in self._early_fills.items()
            for f in fills
            if f.exchange_id == exchange_id
            and f.price is not None
            and (oid, f.executed_at, f.price, f.quantity) not in self._seen_fills
        ]
        if not orphans:
            return 0.0
        delta = q_sig - q_mine
        total = sum(f.quantity for _, f in orphans)
        if total > abs(delta) + 0.5:
            return 0.0  # more than the drift: not (only) these; let the reconcile adopt SIG's numbers
        side = m.Side.YES if delta > 0 else m.Side.NO
        for oid, f in orphans:
            assert f.price is not None
            self._seen_fills[(oid, f.executed_at, f.price, f.quantity)] = None
            cost = f.quantity * (f.price if side is m.Side.YES else 1 - f.price)
            log.warning(
                "orphan fill booked: order %s %s %s %.0f sh @ %.3f (no placement response; side from the "
                "position drift)",
                oid,
                exchange_id,
                side.value,
                f.quantity,
                f.price,
            )
            self.apply_fill(exchange_id, side, f.quantity, cost)
            self._orders_applied.setdefault(oid, (exchange_id, side, f.quantity, 0.0))
        for oid, _ in orphans:
            self._early_fills.pop(oid, None)
        return total

    def _log_first_no_position(self, fresh: dict[str, m.Position]) -> None:
        if self._logged_no_position:
            return
        for p in fresh.values():
            if p.quantity < 0:
                self._logged_no_position = True
                log.warning(
                    "first NO position as SIG reports it: %s qty=%s avgCost=%s costBasis=%s currentPrice=%s "
                    "marketValue=%s",
                    p.exchange_id,
                    p.quantity,
                    p.avg_cost,
                    p.cost_basis,
                    p.current_price,
                    p.market_value,
                )
                return

    def note_order(
        self,
        order_id: int,
        exchange_id: str,
        side: m.Side,
        applied_qty: float,
        quantity: float | None = None,
    ) -> None:
        """Register a placed order so that fills reported later on the account feed (a resting remainder
        filling after SIG's response, before our cancel lands) are applied once, on top of the
        ``applied_qty`` shares the placement response already reported. ``quantity`` is the order's size:
        the feed can never add more than ``quantity - applied_qty`` whatever it repeats."""
        room = float("inf") if quantity is None else max(0.0, quantity - applied_qty)
        self._orders_applied[order_id] = (exchange_id, side, applied_qty, room)
        while len(self._orders_applied) > 1000:
            del self._orders_applied[next(iter(self._orders_applied))]
        for fill in self._early_fills.pop(order_id, []):
            self._apply_feed_fill(fill)

    def _apply_feed_fill(self, fill: m.RtFill) -> bool:
        """Apply the part of a feed fill that the placement response did not cover. False if the order is
        unknown (placed before a restart, or its response is still in flight). A fill already seen (same
        order, time, price and size) is ignored: SIG repeats fills across batches."""
        if fill.order_id is None or (entry := self._orders_applied.get(fill.order_id)) is None:
            return False
        if fill.price is None:
            # the fill carried no price: the position is learnt from the next reconcile instead
            log.warning("fill on order %s without a price: reconciling", fill.order_id)
            self.request_reconcile()
            return True
        key = (fill.order_id, fill.executed_at, fill.price, fill.quantity)
        if key in self._seen_fills:
            return True
        self._seen_fills[key] = None
        while len(self._seen_fills) > 5000:
            del self._seen_fills[next(iter(self._seen_fills))]
        exchange_id, side, skip, room = entry
        covered = min(skip, fill.quantity)
        extra = min(fill.quantity - covered, room)
        self._orders_applied[fill.order_id] = (exchange_id, side, skip - covered, room - extra)
        if extra > 0:
            # feed prices are YES-normalized; a NO share costs 1 - price
            cost = extra * (fill.price if side is m.Side.YES else 1 - fill.price)
            log.info(
                "late fill: order %s %s %s +%.0f sh @ %.3f (after the response)",
                fill.order_id,
                exchange_id,
                side.value,
                extra,
                fill.price,
            )
            self.apply_fill(exchange_id, side, extra, cost)
        return True

    def on_account_batch(self, batch: m.AccountBatch) -> None:
        now = utcnow()
        for f in batch.fills:
            self._rec.write("fills", (now, f.order_id, f.exchange_id, f.price, f.quantity, "realtime"))
            if f.order_id is not None and not self._apply_feed_fill(f):
                # the order's own response may still be in flight: keep the fill until note_order()
                self._early_fills.setdefault(f.order_id, []).append(f)
                while len(self._early_fills) > 200:
                    del self._early_fills[next(iter(self._early_fills))]
        for s in batch.settlements:
            pnl = s.realized_pnl if s.realized_pnl is not None else s.payout - (s.cost_basis or 0.0)
            self.realized[s.exchange_id] = self.realized.get(s.exchange_id, 0.0) + pnl
            self._rec.event(
                "settlement",
                f"{s.exchange_id} settled {s.settlement_outcome}: payout {s.payout}",
                data={"exchange_id": s.exchange_id, "payout": s.payout, "pnl": s.realized_pnl},
            )
        if batch.fills or batch.order_updates or batch.settlements or batch.refunds:
            self.request_reconcile()

    def apply_fill(self, exchange_id: str, side: m.Side, qty: float, total_cost: float) -> None:
        """Optimistic update between reconciles (buys only; the engine nets opposite positions)."""
        if qty <= 0:
            return
        if self._last_fill.get(exchange_id, float("-inf")) <= now_mono() - self._fill_settle_s:
            self._pre_fill[exchange_id] = self.position(exchange_id)
        signed = qty if side is m.Side.YES else -qty
        price = total_cost / qty
        h = self.holdings.get(exchange_id)
        if h is not None and h.qty * signed < 0:
            # Opposite side: each YES/NO pair cancels and pays 1 immediately.
            netted = min(qty, abs(h.qty))
            self.realized[exchange_id] = self.realized.get(exchange_id, 0.0) + netted * (
                1.0 - h.avg_cost - price
            )
            self.cash += netted - total_cost
            h.cost_basis -= netted * h.avg_cost
            h.qty += signed
            crossed = qty - netted
            if crossed > 0:
                h.avg_cost, h.cost_basis = price, crossed * price
        else:
            self.cash -= total_cost
            if h is None:
                h = self.holdings[exchange_id] = Holding(exchange_id, 0.0, price, 0.0)
            h.qty += signed
            h.cost_basis += total_cost
            h.avg_cost = h.cost_basis / abs(h.qty)
        if abs(h.qty) < 1e-9:
            del self.holdings[exchange_id]
        self._last_fill[exchange_id] = now_mono()
        if not self.paper:
            self.request_reconcile()

    def position(self, exchange_id: str) -> float:
        h = self.holdings.get(exchange_id)
        return h.qty if h else 0.0

    @property
    def deployed(self) -> float:
        return sum(h.cost_basis for h in self.holdings.values())

    def reconcile_interval(self) -> float:
        return self._every_silent if self.feed_silent() else self._every

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            every = self.reconcile_interval()
            await wait_event_or_stop(self._soon, stop, every)
            self._soon.clear()
            # Debounce bursts of fills and respect the minimum interval (request budget).
            wait = max(2.0, min(self._min_interval, every) - (now_mono() - self.last_reconcile))
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=wait)
            if stop.is_set():
                break
            try:
                await self.reconcile()
            except SigApiError as exc:
                log.warning("account reconcile failed: %s", exc)
