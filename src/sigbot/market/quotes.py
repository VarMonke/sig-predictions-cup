"""Top-of-book board for external instruments (Polymarket tokens, Kalshi tickers)."""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum
from typing import NamedTuple

from sigbot.core.util import Mils, now_mono


class Source(StrEnum):
    POLYMARKET = "pm"
    KALSHI = "kalshi"


class Quote(NamedTuple):
    bid: Mils
    ask: Mils
    bid_size: float
    ask_size: float
    ts: float
    """Monotonic time of the last update."""

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2

    @property
    def spread(self) -> Mils:
        return self.ask - self.bid


type Listener = Callable[[Source, str], None]


class QuoteBoard:
    __slots__ = ("_down_since", "_listeners", "_quotes", "_source_ok")

    def __init__(self) -> None:
        self._quotes: dict[str, Quote] = {}
        self._listeners: list[Listener] = []
        self._source_ok: dict[Source, bool] = dict.fromkeys(Source, False)
        self._down_since: dict[Source, float] = {}

    def subscribe(self, listener: Listener) -> None:
        self._listeners.append(listener)

    def update(
        self,
        source: Source,
        instrument: str,
        bid: Mils,
        ask: Mils,
        bid_size: float = 0.0,
        ask_size: float = 0.0,
        *,
        notify_unchanged: bool = False,
    ) -> None:
        """``notify_unchanged``: tell listeners even if bid/ask did not move (a fresh quote can still
        confirm a move elsewhere, e.g. Kalshi agreeing with a sharp Polymarket move)."""
        prev = self._quotes.get(instrument)
        quote = Quote(bid, ask, bid_size, ask_size, now_mono())
        self._quotes[instrument] = quote
        if notify_unchanged or prev is None or prev.bid != bid or prev.ask != ask:
            for listener in self._listeners:
                listener(source, instrument)

    def touch(self, instrument: str) -> None:
        """Refresh the timestamp of an unchanged quote (e.g. a poll that returned the same book)."""
        q = self._quotes.get(instrument)
        if q is not None:
            self._quotes[instrument] = q._replace(ts=now_mono())

    def get(self, instrument: str) -> Quote | None:
        return self._quotes.get(instrument)

    def set_source_ok(self, source: Source, ok: bool) -> None:
        if self._source_ok[source] and not ok:
            self._down_since[source] = now_mono()
        self._source_ok[source] = ok

    def source_ok(self, source: Source, grace_s: float = 0.0) -> bool:
        """Healthy, or dropped less than ``grace_s`` ago (its last quotes are still fresh). A source that
        was never healthy is not ok."""
        if self._source_ok[source]:
            return True
        down_since = self._down_since.get(source)
        return down_since is not None and now_mono() - down_since < grace_s

    def __len__(self) -> int:
        return len(self._quotes)
