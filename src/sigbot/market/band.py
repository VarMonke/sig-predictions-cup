"""SIG's own price level per market: the "band".

SIG clears its lopsided markets far from Polymarket/Kalshi fair for days (2026-10-03/04: a 1% long shot
trades 10-15c all day, its favourite 85-90c), so a position's edge comes in two kinds:
* **recyclable**: what the market pays back within minutes. Whole-bankroll market orders knock a favourite
  12-20c under SIG's own level and the bid is back within 1-5 minutes (SEN-LA R 2026-10-03 20:04: 600k
  shares to 0.775, 400k bought back at 0.885-0.94 inside two minutes; a second dump to 0.77 at 20:38 was
  bought back at 0.91 ten seconds later). Buying under the band and selling at it is a round trip.
* **carry**: the gap between the band and fair, which only settlement pays (Nov 4). A share bought at the
  band on the favourite's side is worth more than any price SIG will pay back for it until then; the
  leaderboard marks it at the band, below cost. 85% of the book sat in such holds on 2026-10-04.
The band is the time-weighted median of the venue's mid (others' orders only) over ``window_s``: a dump
lasting two minutes barely moves a 30-minute median, while a real repricing follows within ~15 minutes.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable

from sigbot.core.util import Mils, now_mono
from sigbot.market.state import Book

MAX_SPREAD = 100
"""Mids of books wider than this (mils) are not evidence of a level: a dead book with a 10c bid and a 90c ask
has no band."""


def mid_of(book: Book, *, max_spread: Mils = MAX_SPREAD) -> float | None:
    """The book's mid in mils, None when a side is missing, the book is crossed or too wide."""
    if not book.bids or not book.asks:
        return None
    bid, ask = book.bids[0].price, book.asks[0].price
    if ask <= bid or ask - bid > max_spread:
        return None
    return (bid + ask) / 2


class BandTracker:
    """Per exchange, the mids observed as (time, mid) change points; ``band`` is their time-weighted median
    over the window. ``book_of`` should return the book without our own resting orders (the engine's view),
    so our own quotes and rungs never define the level we price them from."""

    def __init__(
        self,
        book_of: Callable[[str], Book | None],
        *,
        window_s: float = 1_800.0,
        min_span_s: float = 600.0,
        max_samples: int = 4_000,
    ) -> None:
        self.book_of = book_of
        """Where ``wake`` reads books; main points it at the engine's view once that exists."""
        self.window_s = window_s
        self.min_span_s = min_span_s
        """Seconds of mid coverage inside the window a band needs before it is reported at all."""
        self._samples: dict[str, deque[tuple[float, float | None]]] = {}
        self._max_samples = max_samples

    def wake(self, exchange_id: str, *_: object) -> None:
        """Book listener: record the mid when it changed."""
        book = self.book_of(exchange_id)
        if book is not None:
            self.observe(exchange_id, mid_of(book), now_mono())

    def observe(self, exchange_id: str, mid: float | None, now: float) -> None:
        samples = self._samples.get(exchange_id)
        if samples is None:
            samples = self._samples[exchange_id] = deque(maxlen=self._max_samples)
        if samples and samples[-1][1] == mid:
            return
        samples.append((now, mid))

    def band(self, exchange_id: str, now: float | None = None) -> float | None:
        """Time-weighted median mid (mils) over the last ``window_s``; None without ``min_span_s`` of
        coverage. Each sample holds from its time to the next sample's (the last one until now)."""
        samples = self._samples.get(exchange_id)
        if not samples:
            return None
        now = now_mono() if now is None else now
        start = now - self.window_s
        spans: list[tuple[float, float]] = []
        covered = 0.0
        for i, (t, mid) in enumerate(samples):
            end = samples[i + 1][0] if i + 1 < len(samples) else now
            lo, hi = max(t, start), min(end, now)
            if hi <= lo or mid is None:
                continue
            spans.append((mid, hi - lo))
            covered += hi - lo
        if covered < self.min_span_s:
            return None
        spans.sort()
        half, acc = covered / 2, 0.0
        for mid, weight in spans:
            acc += weight
            if acc >= half:
                return mid
        return spans[-1][0]

    def side_band(self, exchange_id: str, yes: bool, now: float | None = None) -> float | None:
        """The band in one side's price terms (NO = 1000 - YES)."""
        band = self.band(exchange_id, now)
        if band is None:
            return None
        return band if yes else 1000 - band
