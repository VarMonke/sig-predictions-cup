"""Whale sightings: one activity line when a block trade hits a long shot far above its worth.

One account bought 100-230k-share blocks of SEN-RI R (fair 0.9%) at 14-20c every 2-5 hours for days
(2026-10-05..11; identical share counts hours apart, e.g. 229,431 at 18c and again at 14c), and others did
the same on GOV-CA R, GOV-RI R, SEN-OK D, SEN-MA R. Whoever has asks resting above the band when they come
gets paid; the line is the signal for a manual lift when someone is watching, and a record of the schedule.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping

from sigbot.core.db import Recorder
from sigbot.core.util import Mils, now_mono, to_mils
from sigbot.discord.alerts import PARTY_NAMES, cents
from sigbot.market.fair import Fair
from sigbot.market.mapping import Race
from sigbot.sigapi.models import RtTrade

log = logging.getLogger(__name__)


class WhaleWatch:
    def __init__(
        self,
        fair_of: Callable[[str], Fair | None],
        band_of: Callable[[str], float | None],
        races: Mapping[str, Race],
        parties: Mapping[str, str],
        on_activity: Callable[[str], None],
        *,
        rec: Recorder | None = None,
        min_shares: float = 50_000.0,
        max_fair: Mils = 50,
        min_ratio: float = 2.0,
        min_price: Mils = 80,
        cooldown_s: float = 120.0,
    ) -> None:
        """A print counts when it is at least ``min_shares`` on a side worth at most ``max_fair`` (the long
        shot's side, whichever side of the market that is) at a price of at least ``min_ratio`` times that
        worth and ``min_price``. One line per market per ``cooldown_s``; the shares of a burst are summed.
        Each sighting is also logged and recorded as a ``whale`` event (the schedule is the point)."""
        self._fair_of = fair_of
        self._band_of = band_of
        self._races = races
        self._parties = parties
        self._on_activity = on_activity
        self._rec = rec
        self.min_shares = min_shares
        self.max_fair = max_fair
        self.min_ratio = min_ratio
        self.min_price = min_price
        self.cooldown_s = cooldown_s
        self._last: dict[str, float] = {}
        self._burst: dict[str, float] = {}
        self.sightings = 0

    def on_trades(self, trades: Iterable[RtTrade]) -> None:
        now = now_mono()
        for t in trades:
            fair = self._fair_of(t.exchange_id)
            if fair is None or t.exchange_id not in self._races:
                continue
            price = to_mils(t.price)
            # the long shot's side: YES when the market is a long shot, NO (at 1 - price) when a favourite
            yes = fair.value < 500
            side_fair = fair.value if yes else 1000 - fair.value
            side_price = price if yes else 1000 - price
            if (
                side_fair > self.max_fair
                or side_price < max(self.min_price, self.min_ratio * side_fair)
                or t.quantity < self.min_shares
            ):
                continue
            last = self._last.get(t.exchange_id, float("-inf"))
            if now - last < self.cooldown_s:
                self._burst[t.exchange_id] = self._burst.get(t.exchange_id, 0.0) + t.quantity
                continue
            self._last[t.exchange_id] = now
            self._burst[t.exchange_id] = t.quantity
            self.sightings += 1
            race = self._races[t.exchange_id]
            party = PARTY_NAMES.get(
                self._parties.get(t.exchange_id, ""),
                self._parties.get(t.exchange_id, ""),
            )
            band = self._band_of(t.exchange_id)
            band_text = "" if band is None else f" · band {cents(band if yes else 1000 - band)}"
            line = (
                f"WHALE {race.key} {party} · {t.quantity:,.0f} {'YES' if yes else 'NO'} @ {cents(side_price)}"
                f" · fair {cents(side_fair)}{band_text}"
            )
            self._on_activity(line)
            log.info(line)
            if self._rec is not None:
                self._rec.event(
                    "whale",
                    line,
                    data={
                        "exchange_id": t.exchange_id,
                        "race": race.key,
                        "party": self._parties.get(t.exchange_id, ""),
                        "shares": t.quantity,
                        "price": side_price,
                        "fair": side_fair,
                        "band": band,
                    },
                )
