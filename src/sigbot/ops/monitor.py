"""Periodic snapshots for the dashboard: equity/P&L, per-position P&L by race, and standings.

Every ``every_s`` one equity row plus one row per position (open, or closed with realized P&L) is
written, all sharing one timestamp so Grafana can select "the latest snapshot" with max(ts).
Every ``standings_every_s`` the leaderboard and Smart Score are read (2 read-only SIG requests).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

import msgspec

from sigbot.core.util import now_mono, utcnow, wait_event_or_stop
from sigbot.discord.alerts import AlertSink
from sigbot.market.opening import SigOpenState
from sigbot.sigapi import SigApiError, SigClient
from sigbot.trading.engine import Engine

log = logging.getLogger(__name__)

type Row = tuple[Any, ...]


class Monitor:
    def __init__(
        self,
        engine: Engine,
        sig: SigClient,
        slug: str,
        alerts: AlertSink,
        *,
        every_s: float = 60.0,
        standings_every_s: float = 900.0,
        opening: SigOpenState | None = None,
    ) -> None:
        self._engine = engine
        self._sig = sig
        self._slug = slug
        self._alerts = alerts
        self._opening = opening
        self._every = every_s
        self._standings_every = standings_every_s
        self._by_exchange = engine.mapping.by_exchange()
        self.rank: tuple[int | None, int | None] | None = None
        """(our rank, participants) from the newest leaderboard read."""
        self.fair_missing = 0
        """Holdings without a fair value in the latest snapshot (valued at SIG's mid or cost instead)."""

    def race_rows(self) -> list[Row]:
        return [
            (o.sig_exchange_id, o.sig_market_id, r.key, o.party.value, r.kind.value, r.name)
            for r in self._engine.mapping.races
            for o in r.outcomes
        ]

    def _fair(self, exchange_id: str) -> float | None:
        fv = self._engine.fair(exchange_id)
        return fv.value / 1000 if fv is not None else None

    def _sig_mid(self, exchange_id: str) -> float | None:
        book = self._engine.books.books.get(exchange_id)
        if book is None:
            return None
        bid, ask = book.top()
        if bid is not None and ask is not None:
            return (bid + ask) / 2000
        return None

    def snapshot(self, now: datetime) -> tuple[Row, list[Row]]:
        """(equity row, position rows). Unrealized P&L marks to fair (fallback: SIG mid, then cost)."""
        engine, account = self._engine, self._engine.account
        rows: list[Row] = []
        positions_fair = positions_sig = unrealized_total = 0.0
        self.fair_missing = 0
        exchange_ids = set(account.holdings) | {ex for ex, pnl in account.realized.items() if pnl}
        for ex in sorted(exchange_ids):
            h = account.holdings.get(ex)
            fair, sig_mid = self._fair(ex), self._sig_mid(ex)
            race, outcome = self._by_exchange.get(ex, (None, None))
            unrealized = 0.0
            if h is not None:
                if fair is None and h.qty != 0:
                    self.fair_missing += 1
                mark = fair if fair is not None else (sig_mid if sig_mid is not None else h.avg_cost)
                sig_mark = sig_mid if sig_mid is not None else mark
                value = abs(h.qty) * (mark if h.qty > 0 else 1 - mark)
                positions_fair += value
                positions_sig += abs(h.qty) * (sig_mark if h.qty > 0 else 1 - sig_mark)
                unrealized = value - h.cost_basis
            unrealized_total += unrealized
            rows.append(
                (
                    now,
                    ex,
                    h.qty if h else 0.0,
                    h.avg_cost if h else None,
                    h.cost_basis if h else 0.0,
                    fair,
                    sig_mid,
                    race.key if race else None,
                    outcome.party.value if outcome else None,
                    account.realized.get(ex, 0.0),
                    unrealized,
                ),
            )
        equity_fair = account.cash + positions_fair
        initial = account.initial_balance or None
        equity = (
            now,
            account.cash,
            positions_fair,
            positions_sig,
            equity_fair,
            account.cash + positions_sig,
            account.deployed,
            engine.national_delta(),
            engine.risk.tier,
            engine.risk.halted,
            sum(account.realized.values()),
            unrealized_total,
            equity_fair - initial if initial else None,
            initial,
            self._opening.state(now) if self._opening is not None else None,
            account.tournament_status or None,
        )
        return equity, rows

    async def standings(self, now: datetime) -> None:
        rec = self._engine.rec
        try:
            board = await self._sig.tournament_leaderboard(self._slug, limit=20)
            self.rank = (board.my_rank, board.total)
            rec.write("standing", (now, board.my_rank, board.total))
            for e in board.leaderboard:
                rec.write(
                    "leaderboard",
                    (
                        now,
                        e.rank,
                        e.profile_id,
                        e.username,
                        e.pnl,
                        e.trades_count,
                        e.volume,
                        e.roi,
                        e.win_rate,
                    ),
                )
        except (SigApiError, msgspec.MsgspecError) as exc:
            log.warning("leaderboard snapshot failed: %s", exc)
        try:
            for s in await self._sig.smart_score(self._slug):
                rec.write(
                    "smart_score",
                    (
                        now,
                        s.market_type,
                        s.smart_score,
                        s.smart_score_decayed,
                        s.rank,
                        s.total_traders,
                        s.is_elite,
                        s.minimum_elite_score,
                    ),
                )
        except (SigApiError, msgspec.MsgspecError) as exc:
            log.warning("smart score snapshot failed: %s", exc)

    def tick(self, now: datetime) -> None:
        engine = self._engine
        equity, positions = self.snapshot(now)
        engine.rec.write("equity", equity)
        for row in positions:
            engine.rec.write("positions", row)
        for from_tier, to_tier, why in engine.tiers.tick(
            equity[4],
            now.strftime("%Y-%m-%d"),
            fair_ready=self.fair_missing == 0,
        ):
            self._alerts.alert(f"TIER {from_tier}->{to_tier} ({why})")
            engine.rec.write("ladder", (now, from_tier, to_tier, why))

    async def run(self, stop: asyncio.Event) -> None:
        last_standings = float("-inf")
        while not stop.is_set():
            now = utcnow()
            try:
                self.tick(now)
            except Exception:
                log.exception("monitor snapshot failed")
            if now_mono() - last_standings >= self._standings_every:
                last_standings = now_mono()
                await self.standings(now)
            await wait_event_or_stop(stop, stop, self._every)
