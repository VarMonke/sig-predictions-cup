"""SIG API health: alerts once when SIG keeps failing (no successful request for ``fail_after_s``), with
the status codes seen, and again when it works again. Trading needs no help here: stale books already
stop it. Not failures: 404/409 (normal answers, e.g. cancelling a closed order), 400/422 (our own bad
request, logged where it happens) and "tournament has not started yet" (closed, not failing)."""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from collections.abc import Callable

from sigbot.core.util import now_mono, wait_event_or_stop

log = logging.getLogger(__name__)


def is_failure(status: int, message: str = "") -> bool:
    if status == 403 and "not started" in message.lower():
        return False
    return status in (0, 401, 403, 429) or status >= 500


class SigHealth:
    def __init__(self, on_alert: Callable[[str], None] | None = None, *, fail_after_s: float = 180.0) -> None:
        self._on_alert = on_alert
        self._fail_after = fail_after_s
        self.failing_since: float | None = None
        """Monotonic time of the first failure since the last success."""
        self.errors: Counter[str] = Counter()
        self._alerted = False

    def record(self, status: int, code: str, message: str = "") -> None:
        """Every HTTP attempt's outcome from SigClient (status 0: transport error)."""
        if is_failure(status, message):
            self.errors[f"{status} {code}".strip()] += 1
            if self.failing_since is None:
                self.failing_since = now_mono()
        elif status < 400 or status in (404, 409):
            if self._alerted and self.failing_since is not None:
                self._alert(f"SIG API working again after {(now_mono() - self.failing_since) / 60:.0f} min")
            self.failing_since, self._alerted = None, False
            self.errors.clear()

    @property
    def ok(self) -> bool:
        return self.failing_since is None or now_mono() - self.failing_since < self._fail_after

    def summary(self) -> str:
        return ", ".join(f"{error} x{n}" for error, n in self.errors.most_common(4))

    def check(self) -> None:
        if self.ok or self._alerted or self.failing_since is None:
            return
        self._alerted = True
        minutes = (now_mono() - self.failing_since) / 60
        log.error("SIG API failing for %.0f min: %s", minutes, self.summary())
        self._alert(
            f"SIG API FAILING for {minutes:.0f} min, no successful request since: {self.summary()}. "
            "Trading pauses by itself while SIG prices are stale.",
        )

    def _alert(self, message: str) -> None:
        if self._on_alert is not None:
            self._on_alert(message)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await wait_event_or_stop(stop, stop, 15.0)
            self.check()
