"""Fair value per SIG outcome from Polymarket + Kalshi.

* Party-level outcomes sum the YES mids of that party's candidate markets on each venue.
* Blend = inverse-spread weights; Kalshi weights decay with poll age; Polymarket counts while its
  websockets are connected (it pushes on change, so a quiet quote is still live) and for ``pm_grace_s``
  after a drop, so a ~1s reconnect does not flip every fair value to Kalshi-only and back.
* No moving average: always the latest quotes.
* Sharp moves (>= ``sharp_move`` within ``sharp_window_s``) mark the outcome *unconfirmed* until Kalshi
  agrees or the move persists; strategies demand more edge and size down meanwhile
  ("defense fast, offense confirmed").
"""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import NamedTuple

from sigbot.core.db import Recorder
from sigbot.core.util import Mils, now_mono, utcnow
from sigbot.market.mapping import Mapping, Outcome
from sigbot.market.quotes import QuoteBoard, Source


@dataclass(frozen=True, slots=True)
class FairConfig:
    kalshi_fresh_s: float = 30.0
    kalshi_max_age_s: float = 120.0
    pm_grace_s: float = 10.0
    """Keep using Polymarket's last quotes this long after a websocket drop (reconnects take ~1s)."""
    min_spread: float = 5.0
    base_unc: float = 5.0
    sharp_move: float = 25.0
    sharp_window_s: float = 10.0
    confirm_tol: float = 20.0
    confirm_after_s: float = 20.0
    extremize: float = 1.0
    record_min_change: float = 2.0
    record_every_s: float = 60.0


class Fair(NamedTuple):
    value: Mils
    """YES probability in mils (0-1000)."""
    unc: Mils
    """Uncertainty, mils."""
    pm: float | None
    kalshi: float | None
    n_sources: int
    confirmed: bool
    ts: float


LOW_CONFIDENCE_UNC = 20
"""Fair uncertainty (mils) from spreads or source disagreement of ~3c or more."""


def fair_confidence(fair: Fair | None) -> float:
    """Share of the race loss cap usable, given how far fair value can be trusted."""
    if fair is None or fair.unc >= LOW_CONFIDENCE_UNC:
        return 0.25
    return 0.5 if fair.n_sources < 2 else 1.0


class _Agg(NamedTuple):
    mid: float
    spread: float
    oldest_ts: float
    newest_ts: float


type FairListener = Callable[[str, Fair], None]
type SharpListener = Callable[[str, Outcome], object]


def extremize(value: float, k: float) -> float:
    if k == 1.0:
        return value
    p = min(0.999, max(0.001, value / 1000))
    z = k * math.log(p / (1 - p))
    return 1000 / (1 + math.exp(-z))


class FairEngine:
    def __init__(
        self,
        mapping: Mapping,
        board: QuoteBoard,
        cfg: FairConfig | None = None,
        rec: Recorder | None = None,
    ) -> None:
        self.cfg = cfg or FairConfig()
        self._board = board
        self._rec = rec
        self._outcomes: dict[str, Outcome] = {o.sig_exchange_id: o for r in mapping.races for o in r.outcomes}
        self._by_instrument: dict[str, list[str]] = {}
        for ex, o in self._outcomes.items():
            for inst in (*o.pm_tokens, *o.kalshi_tickers):
                self._by_instrument.setdefault(inst, []).append(ex)
        self._fair: dict[str, Fair] = {}
        self._anchor: dict[str, tuple[float, float]] = {}
        self._unconfirmed: dict[str, float] = {}
        self._recorded: dict[str, tuple[float, float]] = {}
        self._listeners: list[FairListener] = []
        self._sharp: list[SharpListener] = []
        board.subscribe(self._on_quote)

    def subscribe(self, listener: FairListener) -> None:
        self._listeners.append(listener)

    def on_sharp_move(self, listener: SharpListener) -> None:
        self._sharp.append(listener)

    def _on_quote(self, _source: Source, instrument: str) -> None:
        for ex in self._by_instrument.get(instrument, ()):
            fair = self.compute(ex)
            if fair is not None:
                for listener in self._listeners:
                    listener(ex, fair)

    def _agg(self, instruments: Sequence[str]) -> _Agg | None:
        if not instruments:
            return None
        mid = spread = 0.0
        oldest, newest = math.inf, 0.0
        for inst in instruments:
            q = self._board.get(inst)
            if q is None:
                return None
            mid += q.mid
            spread += q.spread
            oldest, newest = min(oldest, q.ts), max(newest, q.ts)
        return _Agg(min(1000.0, mid), spread, oldest, newest)

    def compute(self, exchange_id: str) -> Fair | None:
        outcome = self._outcomes.get(exchange_id)
        if outcome is None:
            return None
        cfg = self.cfg
        now = now_mono()
        pm_ok = self._board.source_ok(Source.POLYMARKET, grace_s=cfg.pm_grace_s)
        pm = self._agg(outcome.pm_tokens) if pm_ok else None
        k = self._agg(outcome.kalshi_tickers)
        k_weight = 0.0
        if k is not None:
            age = now - k.oldest_ts
            if age > cfg.kalshi_max_age_s:
                k = None
            else:
                k_weight = (
                    1.0
                    if age <= cfg.kalshi_fresh_s
                    else 1.0 - (age - cfg.kalshi_fresh_s) / (cfg.kalshi_max_age_s - cfg.kalshi_fresh_s)
                )
        parts: list[tuple[float, float, float]] = []
        if pm is not None:
            parts.append((pm.mid, pm.spread, 1.0 / max(pm.spread, cfg.min_spread)))
        if k is not None and k_weight > 0:
            parts.append((k.mid, k.spread, k_weight / max(k.spread, cfg.min_spread)))
        if not parts:
            self._fair.pop(exchange_id, None)
            return None
        total_w = sum(w for _, _, w in parts)
        value = sum(mid * w for mid, _, w in parts) / total_w
        half_spread = sum(sp / 2 * w for _, sp, w in parts) / total_w
        disagreement = abs(pm.mid - k.mid) if pm is not None and k is not None else 0.0
        unc = round(cfg.base_unc + max(half_spread, disagreement / 2))
        value = extremize(value, cfg.extremize)
        confirmed = self._track_move(exchange_id, outcome, value, pm, k, now)
        fair = Fair(
            round(value),
            unc,
            pm.mid if pm else None,
            k.mid if k else None,
            len(parts),
            confirmed,
            now,
        )
        self._fair[exchange_id] = fair
        self._maybe_record(exchange_id, fair, now)
        return fair

    def _track_move(
        self,
        ex: str,
        outcome: Outcome,
        value: float,
        pm: _Agg | None,
        k: _Agg | None,
        now: float,
    ) -> bool:
        cfg = self.cfg
        anchor = self._anchor.get(ex)
        if anchor is None or now - anchor[0] > cfg.sharp_window_s:
            self._anchor[ex] = (now, value)
        elif abs(value - anchor[1]) >= cfg.sharp_move:
            self._unconfirmed[ex] = now
            self._anchor[ex] = (now, value)
            self._event(
                "sharp_move",
                ex,
                f"{ex} fair {anchor[1]:.0f} -> {value:.0f}",
                {"from": round(anchor[1]), "to": round(value), "has_kalshi": bool(outcome.kalshi_tickers)},
            )
            for listener in self._sharp:
                listener(ex, outcome)
        since = self._unconfirmed.get(ex)
        if since is None:
            return True
        kalshi_agrees = (
            k is not None
            and pm is not None
            and k.newest_ts >= since - 0.5  # allow same-second updates (monotonic clock)
            and abs(k.mid - pm.mid) <= cfg.confirm_tol
        )
        if kalshi_agrees or now - since >= cfg.confirm_after_s:
            del self._unconfirmed[ex]
            by = "kalshi" if kalshi_agrees else "timeout"
            self._event(
                "sharp_confirmed",
                ex,
                f"{ex} confirmed by {by}",
                {"by": by, "after_s": round(now - since, 2)},
            )
            return True
        return False

    def _event(self, kind: str, ex: str, message: str, data: dict[str, object]) -> None:
        if self._rec is not None:
            self._rec.event(kind, message, data={"exchange_id": ex, **data})

    def _maybe_record(self, ex: str, fair: Fair, now: float) -> None:
        if self._rec is None or not self._rec.enabled:
            return
        last = self._recorded.get(ex)
        if (
            last is None
            or abs(fair.value - last[1]) >= self.cfg.record_min_change
            or now - last[0] >= self.cfg.record_every_s
        ):
            self._recorded[ex] = (now, fair.value)
            self._rec.write(
                "fair_values",
                (
                    utcnow(),
                    ex,
                    fair.value / 1000,
                    fair.unc / 1000,
                    fair.pm / 1000 if fair.pm is not None else None,
                    fair.kalshi / 1000 if fair.kalshi is not None else None,
                    fair.n_sources,
                    fair.confirmed,
                ),
            )

    def get(self, exchange_id: str) -> Fair | None:
        """Latest fair value, recomputed so staleness/disconnects are always reflected."""
        return self.compute(exchange_id)

    def outcome(self, exchange_id: str) -> Outcome | None:
        return self._outcomes.get(exchange_id)
