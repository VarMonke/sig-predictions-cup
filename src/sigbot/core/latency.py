"""Latency samples per (source, metric), written as one row per minute (count, p50, p95, p99, max) to
``latency``, with spike detection.

Metrics: ``sig/rest`` and ``sig/order`` (HTTP round trips), ``sig/feed`` (realtime hint delay from SIG's
server timestamp), ``polymarket/feed`` (price-change delay from Polymarket's server timestamp) and
``kalshi/rest`` (order-book poll round trips).

A minute whose p50 or p99 exceeds ``SPIKE_LIMITS`` logs a WARNING (shown on the dashboards); only a spike
lasting ``sustain`` minutes in a row alerts on Discord, followed by a "back to normal" message.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import datetime

from sigbot.core.db import Recorder
from sigbot.core.util import utcnow, wait_event_or_stop

log = logging.getLogger(__name__)

type Observe = Callable[[str, str, float], None]

SPIKE_LIMITS: dict[tuple[str, str], tuple[float, float]] = {
    ("sig", "rest"): (800.0, 2_500.0),
    ("sig", "order"): (800.0, 2_500.0),
    ("sig", "feed"): (2_500.0, 6_000.0),
    ("polymarket", "feed"): (250.0, 1_500.0),
    ("kalshi", "rest"): (400.0, 1_500.0),
}
"""(p50, p99) limits in ms. Normal from Helsinki (2026-09-30): SIG REST ~360/~700, SIG feed ~1,000/~1,900,
Polymarket feed ~20/~40, Kalshi poll ~125/~360."""
MIN_SAMPLES_P50 = 3
MIN_SAMPLES_P99 = 20


def percentile(sorted_values: list[float], q: float) -> float:
    """Nearest-rank percentile of an already sorted, non-empty list."""
    return sorted_values[min(len(sorted_values) - 1, int(q * len(sorted_values)))]


class LatencyStats:
    def __init__(
        self,
        rec: Recorder,
        *,
        every_s: float = 60.0,
        max_samples: int = 20_000,
        on_alert: Callable[[str], None] | None = None,
        sustain: int = 3,
    ) -> None:
        self._rec = rec
        self._every = every_s
        self._max = max_samples
        self._on_alert = on_alert
        self._sustain = sustain
        self._samples: defaultdict[tuple[str, str], list[float]] = defaultdict(list)
        self._spiking: defaultdict[tuple[str, str], int] = defaultdict(int)
        self._alerted: set[tuple[str, str]] = set()
        self.last: dict[tuple[str, str], tuple[datetime, int, float, float]] = {}
        """Newest minute per (source, metric): (ts, samples, p50, p99)."""
        self.spike_minutes: Counter[tuple[str, str]] = Counter()

    def observe(self, source: str, metric: str, ms: float) -> None:
        samples = self._samples[(source, metric)]
        if len(samples) < self._max:
            samples.append(ms)

    def flush(self, now: datetime) -> None:
        for key, samples in self._samples.items():
            if not samples:
                continue
            samples.sort()
            p50, p95, p99 = (percentile(samples, q) for q in (0.5, 0.95, 0.99))
            self._rec.write("latency", (now, *key, len(samples), p50, p95, samples[-1], p99))
            self.last[key] = (now, len(samples), p50, p99)
            self._check_spike(key, len(samples), p50, p99)
        self._samples.clear()

    def _check_spike(self, key: tuple[str, str], n: int, p50: float, p99: float) -> None:
        limits = SPIKE_LIMITS.get(key)
        if limits is None:
            return
        name = "/".join(key)
        if (n >= MIN_SAMPLES_P50 and p50 > limits[0]) or (n >= MIN_SAMPLES_P99 and p99 > limits[1]):
            self._spiking[key] += 1
            self.spike_minutes[key] += 1
            log.warning(
                "latency spike %s: p50 %.0f ms, p99 %.0f ms over %d samples (limits %.0f / %.0f ms)",
                name,
                p50,
                p99,
                n,
                *limits,
            )
            if self._spiking[key] >= self._sustain and key not in self._alerted:
                self._alerted.add(key)
                self._alert(
                    f"LATENCY {name} high for {self._spiking[key]} min: p50 {p50:.0f} ms, p99 {p99:.0f} ms",
                )
            return
        self._spiking[key] = 0
        if key in self._alerted:
            self._alerted.discard(key)
            self._alert(f"LATENCY {name} back to normal: p50 {p50:.0f} ms, p99 {p99:.0f} ms")

    def _alert(self, message: str) -> None:
        if self._on_alert is not None:
            self._on_alert(message)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await wait_event_or_stop(stop, stop, self._every)
            self.flush(utcnow())
