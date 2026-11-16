"""Status embed for the Discord activity channel: every 15 minutes (on the quarter hour) and once shortly
after start. Shows that the bot is alive, whether it trades (or when it starts), money, rank, activity since
the previous post, latency per venue and feed health. Read-only (no SIG requests); a failure only logs."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from sigbot.core.db import TABLES
from sigbot.core.util import now_mono, parse_server_ts, utcnow, wait_event_or_stop
from sigbot.market.opening import ENDED, NOT_STARTED, RETRY_S
from sigbot.market.quotes import QuoteBoard, Source
from sigbot.ops.health import SigHealth

if TYPE_CHECKING:
    from sigbot.core.latency import LatencyStats
    from sigbot.discord.alerts import AlertSink
    from sigbot.ops.monitor import Monitor
    from sigbot.sigapi.realtime import RealtimeClient
    from sigbot.strategies.quoting import QuoteManager
    from sigbot.strategies.sweep import SweepLadder
    from sigbot.trading.engine import Engine

log = logging.getLogger(__name__)

GREEN, BLUE, ORANGE, RED, GREY = 0x2ECC71, 0x3498DB, 0xE67E22, 0xE74C3C, 0x95A5A6
LATENCY_ROWS = (
    (("sig", "rest"), "SIG API"),
    (("sig", "order"), "SIG orders"),
    (("sig", "feed"), "SIG live feed"),
    (("polymarket", "feed"), "Polymarket"),
    (("kalshi", "rest"), "Kalshi"),
)


def discord_time(at: datetime, style: str) -> str:
    """Rendered by Discord in the reader's own time zone (F: full date and time, R: relative)."""
    return f"<t:{int(at.timestamp())}:{style}>"


def duration(ms: float) -> str:
    return f"{ms / 1000:.1f} s" if ms >= 1000 else f"{ms:.0f} ms"


def uptime(seconds: float) -> str:
    days, rest = divmod(int(seconds // 60), 1440)
    hours, minutes = divmod(rest, 60)
    return f"{days}d {hours}h" if days else f"{hours}h {minutes}m" if hours else f"{minutes}m"


def field(name: str, value: str, *, inline: bool = True) -> dict[str, Any]:
    return {"name": name, "value": value or "-", "inline": inline}


class StatusDigest:
    def __init__(
        self,
        engine: Engine,
        monitor: Monitor,
        latency: LatencyStats,
        board: QuoteBoard,
        realtime: RealtimeClient,
        alerts: AlertSink,
        *,
        health: SigHealth | None = None,
        sweep: SweepLadder | None = None,
        quotes: QuoteManager | None = None,
        every_s: float = 900.0,
        first_after_s: float = 90.0,
    ) -> None:
        self._engine = engine
        self._monitor = monitor
        self._latency = latency
        self._board = board
        self._realtime = realtime
        self._alerts = alerts
        self._health = health
        self._sweep = sweep
        self._prev_sweep = (0.0, 0.0)
        self._quotes = quotes
        self._prev_quotes = 0.0
        self._every = every_s
        self._first_after = first_after_s
        self._started = now_mono()
        self._prev_counts = (0, 0, 0)
        self._prev_pushed = 0
        self._prev_equity: float | None = None
        self._prev_spikes: Counter[tuple[str, str]] = Counter()

    def counts(self) -> tuple[int, int, int]:
        """(fills, lost races, rejected signals) since start."""
        executor = self._engine.executor
        return executor.fills, executor.lost_races, self._engine.stats["rejected"]

    def equity_row(self, now: datetime) -> dict[str, Any]:
        return dict(zip(TABLES["equity"], self._monitor.snapshot(now)[0], strict=True))

    def post(self, now: datetime) -> None:
        try:
            eq = self.equity_row(now)
            self._alerts.status(self.build(now, eq))
        except Exception:
            log.exception("status post failed")
            return
        self._prev_counts, self._prev_equity = self.counts(), eq["equity_fair"]
        self._prev_spikes = Counter(self._latency.spike_minutes)
        self._prev_pushed = self._engine.books.pushed_books
        if self._sweep is not None:
            self._prev_sweep = (self._sweep.shares_filled, self._sweep.edge_filled)
        if self._quotes is not None:
            self._prev_quotes = self._quotes.shares_filled

    def build(self, now: datetime, eq: dict[str, Any]) -> dict[str, Any]:
        engine, account, risk = self._engine, self._engine.account, self._engine.risk
        equity, initial, pnl = eq["equity_fair"], eq["initial_balance"], eq["pnl"]
        mode = engine.executor.mode
        start, end = parse_server_ts(account.start_date), parse_server_ts(account.end_date)
        sig_state = eq.get("sig_state")
        if (end is not None and now > end) or sig_state == ENDED:
            state, color = "**Not trading**: the tournament has ended", GREY
        elif start is not None and now < start:
            state, color = (
                f"**Not trading**, will start {discord_time(start, 'F')} ({discord_time(start, 'R')})",
                BLUE,
            )
        elif sig_state == NOT_STARTED:
            state, color = f"**Not trading**: SIG hasn't opened yet; retrying every {RETRY_S:.0f}s", BLUE
        else:
            how = "automatic" if engine.tiers.auto_promote else "held by hand"
            state, color = f"**Trading {mode}** at tier {risk.tier} ({how})", GREEN

        risk.check_rate()
        day_start = engine.tiers.day_start_equity
        problems = [
            text
            for hit, text in (
                (risk.halted, "**HALTED**: no new trades until /resume"),
                (
                    bool(day_start) and equity < day_start * (1 - risk.daily_loss_frac),
                    "Daily loss limit hit: no new trades until 00:00 UTC",
                ),
                (risk.error_limited, "Error breaker tripped: paused while errors calm down"),
                (bool(risk.paused_strategies), "Paused: " + ", ".join(sorted(risk.paused_strategies))),
                (bool(risk.manual_paused), f"{len(risk.manual_paused)} markets on manual"),
            )
            if hit
        ]
        feeds = {
            "SIG API": self._health is None or self._health.ok,
            "SIG live feed": self._realtime.connected.is_set(),
            "Polymarket": self._board.source_ok(Source.POLYMARKET, grace_s=10.0),
            "Kalshi": self._board.source_ok(Source.KALSHI),
        }
        feed_texts = {name: "ok" if ok else "**DOWN**" for name, ok in feeds.items()}
        books = engine.books
        if feeds["SIG live feed"]:
            feed_texts["SIG live feed"] = f"ok ({books.pushed_books - self._prev_pushed:,} books pushed)"
        silent = feeds["SIG live feed"] and books.feed_broken
        if silent:
            feed_texts["SIG live feed"] = (
                f"**SILENT** for {uptime(books.feed_quiet_s)} (polling every {books.scan_interval():.0f}s)"
            )
        latency_lines, slow = self._latency_lines(now)
        if problems or not all(feeds.values()):
            color = RED
        elif (slow or silent) and color in (GREEN, BLUE):
            color = ORANGE

        fills, lost, rejected = (
            now_count - prev for now_count, prev in zip(self.counts(), self._prev_counts, strict=True)
        )
        since = f"{fills} fills · {lost} lost to speed · {rejected} signals rejected"
        if self._prev_equity is not None:
            since = f"{equity - self._prev_equity:+,.0f} P&L · {since}"
        rank = self._monitor.rank
        deployed = eq["deployed"]
        fields = [
            field("Balance", f"{eq['cash']:,.0f}"),
            field("Equity", f"{equity:,.0f}"),
            field("P&L", f"{pnl:+,.0f} ({pnl / initial:+.2%})" if pnl is not None and initial else "-"),
            field("Today", f"{equity - day_start:+,.0f}" if day_start else "-"),
            field("Realized / open", f"{eq['realized_pnl']:+,.0f} / {eq['unrealized_pnl']:+,.0f}"),
            field(
                "Deployed",
                f"{deployed:,.0f} ({deployed / equity:.0%}) · {len(account.holdings)} positions"
                if equity
                else "-",
            ),
            field("Rank", f"#{rank[0]:,} of {rank[1]:,}" if rank and rank[0] and rank[1] else "-"),
            field("Since last update", since, inline=False),
            field("Latency (last minute: median, p99)", "\n".join(latency_lines), inline=False),
            field(
                "Feeds",
                " · ".join(f"{name} {text}" for name, text in feed_texts.items())
                + (f"\nSIG errors: {self._health.summary()}" if self._health and not self._health.ok else ""),
                inline=False,
            ),
        ]
        if self._quotes is not None:
            fields.insert(7, field("Flow trading", self._quotes_line(), inline=False))
        if self._sweep is not None:
            fields.insert(7, field("Sweep ladder", self._sweep_line(), inline=False))
        return {
            "title": "Sigbot is alive" + ("" if mode == "LIVE" else " (shadow mode: paper trading)"),
            "description": "\n".join([state, *problems]),
            "color": color,
            "fields": fields,
            "footer": {"text": f"{mode} · up {uptime(now_mono() - self._started)} · every 15 min"},
            "timestamp": now.isoformat(),
        }

    def _quotes_line(self) -> str:
        assert self._quotes is not None
        st = self._quotes.stats()
        shares = self._quotes.shares_filled - self._prev_quotes
        text = (
            f"{st['live']} quotes on {st['exchanges']} of {st['scope']} markets · {st['reserved']:,} of "
            f"{st['budget']:,} collateral · inventory {st['inventory']:,} of {st['inventory_budget']:,} · "
            f"filled {shares:,.0f} sh since last update"
        )
        if st["unfunded"]:
            text += f" · {st['unfunded']} wanted quotes unfunded"
        return text

    def _sweep_line(self) -> str:
        assert self._sweep is not None
        stats = self._sweep.stats()
        shares = self._sweep.shares_filled - self._prev_sweep[0]
        edge = self._sweep.edge_filled - self._prev_sweep[1]
        text = (
            f"{stats['resting']} rungs on {stats['exchanges']} markets · {stats['reserved']:,} of "
            f"{stats['budget']:,} reserved · caught {shares:,.0f} sh (+{edge:,.0f} at fair) since last update"
        )
        if self._sweep.rejected:
            text += " · gate: " + ", ".join(f"{n} {why}" for why, n in sorted(self._sweep.rejected.items()))
        return text

    def _latency_lines(self, now: datetime) -> tuple[list[str], bool]:
        spikes = self._latency.spike_minutes - self._prev_spikes
        lines: list[str] = []
        for key, label in LATENCY_ROWS:
            last = self._latency.last.get(key)
            if last is None or now - last[0] > timedelta(seconds=self._every):
                if key != ("sig", "order"):
                    lines.append(f"{label}: no data")
                continue
            _, _, p50, p99 = last
            slow = f" · slow for {spikes[key]} min" if spikes[key] else ""
            lines.append(f"{label}: {duration(p50)} ({duration(p99)}){slow}")
        return lines, bool(spikes)

    async def run(self, stop: asyncio.Event) -> None:
        await wait_event_or_stop(stop, stop, self._first_after)
        while not stop.is_set():
            self.post(utcnow())
            delay = self._every - time.time() % self._every
            if delay < self._every / 10:
                delay += self._every
            await wait_event_or_stop(stop, stop, delay)
