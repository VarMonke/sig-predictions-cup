"""Is SIG open for trading? SIG decides, and the first signal wins:

- open: SIG accepts an order, a trade print appears on the tournament feed (there were none before the
  open), or the tournament status leaves "draft" once the start time has passed;
- not started: SIG rejects an order with "Tournament has not started yet". Orders then pause for
  ``RETRY_S`` instead of counting as errors, so trading begins within seconds of SIG opening even if it
  opens late.

The first time SIG shows as open is logged and alerted, and so is every tournament status change.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime

from sigbot.core.util import now_mono, parse_server_ts, utcnow
from sigbot.sigapi import SigApiError

log = logging.getLogger(__name__)

RETRY_S = 5.0
NOT_STARTED_STATUSES = frozenset({"draft", "upcoming", "scheduled", "pending"})
OPEN, NOT_STARTED, ENDED = "OPEN", "NOT STARTED", "ENDED"


def is_not_started(exc: SigApiError) -> bool:
    message = exc.message.lower()
    return exc.status == 403 and ("not started" in message or "not yet started" in message)


class SigOpenState:
    def __init__(self, on_alert: Callable[[str], None] | None = None) -> None:
        self._on_alert = on_alert
        self.status = ""
        self.start: datetime | None = None
        self.end: datetime | None = None
        self.opened_at: datetime | None = None
        self.not_started_rejections = 0
        self.closed_until = 0.0
        """Monotonic time until which orders pause after a "not started" rejection."""

    def on_meta(self, status: str, start_date: str | None, end_date: str | None) -> None:
        """Tournament metadata from each account sync."""
        old, self.status = self.status, status
        self.start, self.end = parse_server_ts(start_date), parse_server_ts(end_date)
        if old and status != old:
            log.warning("SIG tournament status changed: %s -> %s", old, status)
            self._alert(f"SIG tournament status changed: {old} -> {status}")
        started = self.start is None or utcnow() >= self.start
        if started and status and status.lower() not in NOT_STARTED_STATUSES:
            self._mark_open(f"tournament status is {status!r}")

    def on_not_started(self) -> None:
        self.not_started_rejections += 1
        self.closed_until = now_mono() + RETRY_S
        if self.not_started_rejections == 1:
            log.warning(
                "SIG rejected an order: tournament has not started yet; retrying every %.0fs",
                RETRY_S,
            )

    def on_order_accepted(self) -> None:
        self.closed_until = 0.0
        self._mark_open("SIG accepted an order")

    def on_trade_print(self) -> None:
        self._mark_open("first trade print on SIG's feed")

    def accepting(self) -> bool:
        return now_mono() >= self.closed_until

    def state(self, now: datetime) -> str:
        if self.end is not None and now > self.end:
            return ENDED
        if not self.accepting():
            return NOT_STARTED
        if self.opened_at is not None:
            return OPEN
        if (self.start is not None and now < self.start) or self.status.lower() in NOT_STARTED_STATUSES:
            return NOT_STARTED
        return OPEN

    def _mark_open(self, why: str) -> None:
        if self.opened_at is not None:
            return
        self.opened_at = utcnow()
        self.closed_until = 0.0
        late = ""
        if self.start is not None and self.opened_at > self.start:
            late = f", {(self.opened_at - self.start).total_seconds():.0f}s after the scheduled start"
        rejected = (
            f" ({self.not_started_rejections} 'not started' rejections before)"
            if self.not_started_rejections
            else ""
        )
        log.warning(
            "SIG is OPEN: %s at %s%s%s",
            why,
            self.opened_at.isoformat(timespec="seconds"),
            late,
            rejected,
        )
        self._alert(f"SIG is OPEN: {why} at {self.opened_at:%H:%M:%S} UTC{late}{rejected}")

    def _alert(self, message: str) -> None:
        if self._on_alert is not None:
            self._on_alert(message)
