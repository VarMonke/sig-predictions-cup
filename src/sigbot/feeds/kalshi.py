"""Kalshi public REST poller (no account) -> QuoteBoard.

``/markets?tickers=`` is CDN-cached (~15s) so it only serves as a slow baseline. Fresh data comes from the
uncached per-market ``/orderbook`` endpoint, polled round-robin with priorities. ``boost()`` spends the
burst allowance on specific tickers (e.g. to confirm a sharp Polymarket move).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
from collections.abc import Callable, Iterable

import aiohttp
import msgspec

from sigbot.core.util import TokenBucket, to_mils, wait_event_or_stop
from sigbot.market.quotes import QuoteBoard, Source

log = logging.getLogger(__name__)

BASE = "https://api.elections.kalshi.com/trade-api/v2"
BASELINE_EVERY_S = 15.0


class OrderbookFp(msgspec.Struct, gc=False):
    yes_dollars: list[tuple[str, str]] | None = None
    no_dollars: list[tuple[str, str]] | None = None


class OrderbookResp(msgspec.Struct, gc=False):
    orderbook_fp: OrderbookFp | None = None


class KMarket(msgspec.Struct, gc=False):
    ticker: str
    yes_bid_dollars: str | None = None
    yes_ask_dollars: str | None = None


class KMarkets(msgspec.Struct, gc=False):
    markets: list[KMarket] = []


_ob_decoder = msgspec.json.Decoder(OrderbookResp)
_mk_decoder = msgspec.json.Decoder(KMarkets)


class KalshiPoller:
    def __init__(
        self,
        tickers: Iterable[str],
        board: QuoteBoard,
        *,
        max_rps: float = 12.0,
        observe: Callable[[str, str, float], None] | None = None,
    ) -> None:
        self._tickers = sorted(set(tickers))
        self._board = board
        self._observe = observe
        self._bucket = TokenBucket(max_rps, capacity=max(1.0, max_rps))
        self._priority: dict[str, float] = dict.fromkeys(self._tickers, 10.0)
        self._next_due: dict[str, float] = dict.fromkeys(self._tickers, 0.0)
        self._boost: dict[str, tuple[float, float]] = {}
        self._boost_ends: dict[str, float] = {}
        self._wake = asyncio.Event()
        self._last_ok = 0.0
        self.errors = 0
        self.boosts = 0

    def set_interval(self, ticker: str, seconds: float) -> None:
        """Target refresh interval for a ticker (lower = hotter)."""
        if ticker in self._priority:
            self._priority[ticker] = seconds

    def boost(
        self,
        tickers: Iterable[str],
        seconds: float = 4.0,
        interval: float = 0.25,
        cooldown: float = 6.0,
        *,
        now: float | None = None,
    ) -> list[str]:
        """Poll these tickers every ``interval`` s for the next ``seconds``, ahead of the round-robin.

        A ticker is not re-boosted while boosted or within ``cooldown`` s after, so a flickering quote
        cannot starve everything else. Total polling stays under ``max_rps``. Returns the boosted tickers.
        """
        now = time.monotonic() if now is None else now
        boosted = [
            t for t in tickers if t in self._priority and now >= self._boost_ends.get(t, -math.inf) + cooldown
        ]
        for t in boosted:
            self._boost[t] = (now + seconds, interval)
            self._boost_ends[t] = now + seconds
            self._next_due[t] = 0.0
        if boosted:
            self.boosts += 1
            self._wake.set()
        return boosted

    def _boosted(self, ticker: str, now: float) -> bool:
        boost = self._boost.get(ticker)
        return boost is not None and boost[0] > now

    def _next_ticker(self, now: float) -> str | None:
        """Most urgent due ticker: boosted ones first, then the most overdue."""
        best: tuple[bool, float, str] | None = None
        for t in self._tickers:
            due_at = self._next_due[t]
            if due_at <= now:
                key = (not self._boosted(t, now), due_at, t)
                if best is None or key < best:
                    best = key
        return best[2] if best else None

    def _interval(self, ticker: str, now: float) -> float:
        boost = self._boost.get(ticker)
        if boost is not None:
            if boost[0] > now:
                return boost[1]
            del self._boost[ticker]
        return self._priority[ticker]

    async def run(self, stop: asyncio.Event) -> None:
        timeout = aiohttp.ClientTimeout(total=8, sock_connect=4)
        connector = aiohttp.TCPConnector(limit=16, ttl_dns_cache=300)
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={"User-Agent": "sigbot/0.1"},
        ) as session:
            await asyncio.gather(self._baseline_loop(session, stop), self._orderbook_loop(session, stop))

    async def _baseline_loop(self, session: aiohttp.ClientSession, stop: asyncio.Event) -> None:
        while not stop.is_set():
            for i in range(0, len(self._tickers), 100):
                chunk = self._tickers[i : i + 100]
                await self._bucket.acquire()
                try:
                    async with session.get(
                        f"{BASE}/markets",
                        params={"tickers": ",".join(chunk), "limit": 1000},
                    ) as r:
                        if r.status == 200:
                            for mk in _mk_decoder.decode(await r.read()).markets:
                                self._baseline(mk)
                        elif r.status == 429:
                            await asyncio.sleep(1.0)
                except (aiohttp.ClientError, TimeoutError) as exc:
                    log.debug("kalshi baseline error: %r", exc)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=BASELINE_EVERY_S)

    def _baseline(self, mk: KMarket) -> None:
        # Only seed tickers we have never polled directly the orderbook poll is fresher.
        if self._board.get(mk.ticker) is not None or not mk.yes_bid_dollars or not mk.yes_ask_dollars:
            return
        self._board.update(
            Source.KALSHI,
            mk.ticker,
            to_mils(float(mk.yes_bid_dollars)),
            to_mils(float(mk.yes_ask_dollars)),
        )

    async def _orderbook_loop(self, session: aiohttp.ClientSession, stop: asyncio.Event) -> None:
        while not stop.is_set():
            now = time.monotonic()
            self._board.set_source_ok(Source.KALSHI, now - self._last_ok < 10.0)
            ticker = self._next_ticker(now)
            if ticker is None:
                soonest = min(self._next_due.values(), default=now + 1.0)
                self._wake.clear()
                await wait_event_or_stop(self._wake, stop, max(0.05, soonest - now))
                continue
            self._next_due[ticker] = now + self._interval(ticker, now)
            await self._bucket.acquire()
            if await self._poll(session, ticker):
                self._last_ok = time.monotonic()

    async def _poll(self, session: aiohttp.ClientSession, ticker: str) -> bool:
        try:
            sent = time.perf_counter()
            async with session.get(f"{BASE}/markets/{ticker}/orderbook", params={"depth": 5}) as r:
                body = await r.read()
                if self._observe is not None:
                    self._observe("kalshi", "rest", (time.perf_counter() - sent) * 1000)
                if r.status == 429:
                    self.errors += 1
                    await asyncio.sleep(0.5)
                    return False
                if r.status != 200:
                    self.errors += 1
                    return False
                book = _ob_decoder.decode(body).orderbook_fp
        except (aiohttp.ClientError, TimeoutError, msgspec.DecodeError) as exc:
            self.errors += 1
            log.debug("kalshi %s poll error: %r", ticker, exc)
            return False
        if book is None:
            return True
        yes = book.yes_dollars or []
        no = book.no_dollars or []
        best_yes = max(yes, key=lambda lv: float(lv[0]), default=None)
        best_no = max(no, key=lambda lv: float(lv[0]), default=None)
        bid = to_mils(float(best_yes[0])) if best_yes else 0
        ask = 1000 - to_mils(float(best_no[0])) if best_no else 1000
        self._board.update(
            Source.KALSHI,
            ticker,
            bid,
            ask,
            float(best_yes[1]) if best_yes else 0.0,
            float(best_no[1]) if best_no else 0.0,
            notify_unchanged=self._boosted(ticker, time.monotonic()),
        )
        return True
