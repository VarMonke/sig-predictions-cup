"""Small shared helpers: integer price math, rate limiting, clocks."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque
from datetime import UTC, datetime

type Mils = int
"""Prices in thousandths (0.435 -> 435). Exact arithmetic for books, sums and ticks."""

SIG_TICK: Mils = 5
SIG_MIN_PRICE: Mils = 5
SIG_MAX_PRICE: Mils = 995
ONE: Mils = 1000


def to_mils(price: float) -> Mils:
    return round(price * 1000)


def from_mils(mils: Mils) -> float:
    return mils / 1000


def sig_floor(mils: Mils) -> Mils:
    """Largest valid SIG limit price <= mils (use for buy limits)."""
    return max(SIG_MIN_PRICE, min(SIG_MAX_PRICE, (mils // SIG_TICK) * SIG_TICK))


def sig_ceil(mils: Mils) -> Mils:
    """Smallest valid SIG limit price >= mils (use for sell limits)."""
    return max(SIG_MIN_PRICE, min(SIG_MAX_PRICE, -((-mils) // SIG_TICK) * SIG_TICK))


def is_sig_tick(mils: Mils) -> bool:
    return SIG_MIN_PRICE <= mils <= SIG_MAX_PRICE and mils % SIG_TICK == 0


def now_mono() -> float:
    return time.monotonic()


def utcnow() -> datetime:
    return datetime.now(UTC)


def parse_server_ts(value: str | None) -> datetime | None:
    """Parse a SIG server timestamp (ISO 8601, up to 7 fractional digits); None if missing or invalid."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def new_key(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex}"


async def wait_event_or_stop(event: asyncio.Event, stop: asyncio.Event, max_wait_s: float) -> None:
    """Return when ``event`` or ``stop`` is set, or after ``max_wait_s`` (keeps shutdown prompt)."""
    waiters = {asyncio.ensure_future(event.wait()), asyncio.ensure_future(stop.wait())}
    try:
        await asyncio.wait(waiters, timeout=max_wait_s, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for w in waiters:
            w.cancel()


class TokenBucket:
    """Async token bucket: ``rate`` tokens/second, holding at most ``capacity`` tokens."""

    __slots__ = ("_last", "_lock", "_tokens", "capacity", "rate")

    def __init__(self, rate: float, capacity: float | None = None) -> None:
        self.rate = rate
        self.capacity = capacity if capacity is not None else max(1.0, rate)
        self._tokens = self.capacity
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
        self._last = now

    def try_acquire(self, tokens: float = 1.0) -> bool:
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    async def acquire(self, tokens: float = 1.0) -> None:
        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                await asyncio.sleep((tokens - self._tokens) / self.rate)

    @property
    def available(self) -> float:
        self._refill()
        return self._tokens


class RequestBudget:
    """Sliding-window request budget shared by every call made with one API key.

    SIG gives a standard key 100 reads and **30 writes** a minute (GET/HEAD are reads; every other method,
    including one batch or multi-leg request, is a write) and punishes an overrun with a ~60s lockout of
    every request (orders included), so we stay under ``limit`` requests and ``write_limit`` writes.
    Market-data calls may never take the last ``trade_headroom`` slots of a window, nor more than
    ``data_limit`` in total, so orders and cancels always get through. A 429 sets a shared penalty clock
    that pauses *every* request until it expires.
    """

    def __init__(
        self,
        limit: int = 90,
        data_limit: int = 60,
        trade_headroom: int = 20,
        window_s: float = 60.0,
        max_per_s: float = 4.0,
        write_limit: int = 27,
    ) -> None:
        self.limit = limit
        self.data_limit = data_limit
        self.trade_headroom = trade_headroom
        self.write_limit = write_limit
        self.window_s = window_s
        self._all: deque[float] = deque()
        self._data: deque[float] = deque()
        self._writes: deque[float] = deque()
        self._burst = TokenBucket(max_per_s, capacity=max_per_s)
        self._blocked_until = 0.0

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_s
        for q in (self._all, self._data, self._writes):
            while q and q[0] <= cutoff:
                q.popleft()

    def wait_time(self, trade: bool, now: float | None = None, *, write: bool | None = None) -> float:
        """``write`` defaults to ``trade`` (orders and cancels are writes)."""
        now = time.monotonic() if now is None else now
        if now < self._blocked_until:
            return self._blocked_until - now
        self._prune(now)
        waits = [0.0]
        if len(self._all) >= self.limit:
            waits.append(self._all[len(self._all) - self.limit] + self.window_s - now)
        if (trade if write is None else write) and len(self._writes) >= self.write_limit:
            waits.append(self._writes[len(self._writes) - self.write_limit] + self.window_s - now)
        if not trade:
            cap = self.limit - self.trade_headroom
            if len(self._all) >= cap:
                waits.append(self._all[len(self._all) - cap] + self.window_s - now)
            if len(self._data) >= self.data_limit:
                waits.append(self._data[len(self._data) - self.data_limit] + self.window_s - now)
        return max(waits)

    async def acquire(self, trade: bool = False, *, write: bool | None = None) -> None:
        write = trade if write is None else write
        while True:
            now = time.monotonic()
            wait = self.wait_time(trade, now, write=write)
            if wait <= 0:
                # Reserve synchronously (no await between check and append) so callers can't overshoot.
                self._all.append(now)
                if write:
                    self._writes.append(now)
                if not trade:
                    self._data.append(now)
                break
            await asyncio.sleep(min(wait, 5.0))
        await self._burst.acquire()

    def penalize(self, seconds: float) -> None:
        self._blocked_until = max(self._blocked_until, time.monotonic() + seconds)

    @property
    def blocked_for(self) -> float:
        return max(0.0, self._blocked_until - time.monotonic())

    def used(self) -> tuple[int, int]:
        """(all, data) requests in the current window."""
        self._prune(time.monotonic())
        return len(self._all), len(self._data)

    def writes_used(self) -> int:
        self._prune(time.monotonic())
        return len(self._writes)

    def assume_writes_spent(self) -> None:
        """Treat the write window as full right now: a restarted bot shares SIG's per-key window with the
        process it replaced, whose last minute of batches and cancels it cannot see (two 429 lockouts within
        15 s of a restart on 2026-10-02)."""
        now = time.monotonic()
        self._writes.clear()
        self._writes.extend([now] * self.write_limit)
