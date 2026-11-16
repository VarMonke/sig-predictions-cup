"""Non-blocking analytics sink.

``Recorder.write()`` only enqueues; a background task COPYs batches into Postgres.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg
import msgspec
from asyncpg.pool import PoolConnectionProxy

from sigbot.core.util import utcnow

log = logging.getLogger(__name__)

TABLES: dict[str, tuple[str, ...]] = {
    "events": ("ts", "kind", "level", "message", "data"),
    "fair_values": ("ts", "exchange_id", "fair", "unc", "pm_mid", "kalshi_mid", "sources", "confirmed"),
    "sig_tob": ("ts", "exchange_id", "bid", "bid_qty", "ask", "ask_qty"),
    "sig_trades": ("ts", "exchange_id", "market_id", "price", "quantity", "server_ts"),
    "book_events": ("ts", "server_ts", "kind", "exchange_id", "market_id"),
    "intents": (
        "ts",
        "strategy",
        "race",
        "exchange_id",
        "side",
        "price",
        "quantity",
        "edge",
        "group_id",
        "decision",
        "trade_reason",
        "detail",
    ),
    "orders": (
        "ts",
        "idem_key",
        "order_id",
        "strategy",
        "group_id",
        "exchange_id",
        "side",
        "price",
        "quantity",
        "shadow",
        "status",
        "trade_reason",
        "qty_traded",
        "fill_price",
        "total_cost",
        "signal_to_send_ms",
        "send_to_resp_ms",
        "error",
    ),
    "fills": ("ts", "order_id", "exchange_id", "price", "quantity", "source"),
    "lost_races": (
        "ts",
        "strategy",
        "exchange_id",
        "side",
        "price",
        "requested",
        "filled",
        "signal_to_send_ms",
        "send_to_resp_ms",
        "book_age_ms",
    ),
    "equity": (
        "ts",
        "cash",
        "positions_fair",
        "positions_sig",
        "equity_fair",
        "equity_sig",
        "deployed",
        "national_delta",
        "tier",
        "halted",
        "realized_pnl",
        "unrealized_pnl",
        "pnl",
        "initial_balance",
        "sig_state",
        "sig_status",
    ),
    "positions": (
        "ts",
        "exchange_id",
        "quantity",
        "avg_cost",
        "cost_basis",
        "fair",
        "sig_price",
        "race",
        "party",
        "realized_pnl",
        "unrealized_pnl",
    ),
    "smart_score": ("ts", "market_type", "score", "decayed", "rank", "total", "elite", "min_elite"),
    "leaderboard": ("ts", "rank", "profile_id", "username", "pnl", "trades", "volume", "roi", "win_rate"),
    "ladder": ("ts", "from_tier", "to_tier", "reason"),
    "races": ("exchange_id", "market_id", "race", "party", "kind", "name"),
    "standing": ("ts", "rank", "total"),
    "logs": ("ts", "level", "logger", "message"),
    "latency": ("ts", "source", "metric", "samples", "p50_ms", "p95_ms", "max_ms", "p99_ms"),
}

_json = msgspec.json.Encoder()


def _read_migrations(directory: Path) -> list[tuple[str, str]]:
    return [(p.stem, p.read_text()) for p in sorted(directory.glob("*.sql"))]


async def migrate(conn: asyncpg.Connection | PoolConnectionProxy, directory: Path) -> None:
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations "
        "(version text PRIMARY KEY, applied_at timestamptz DEFAULT now())",
    )
    applied = {r["version"] for r in await conn.fetch("SELECT version FROM schema_migrations")}
    for version, sql in await asyncio.to_thread(_read_migrations, directory):
        if version in applied:
            continue
        async with conn.transaction():
            await conn.execute(sql)
            await conn.execute("INSERT INTO schema_migrations (version) VALUES ($1)", version)
        log.info("applied migration %s", version)


class Recorder:
    """Fire-and-forget writer. With no DSN every call is a cheap no-op."""

    def __init__(
        self,
        dsn: str | None,
        *,
        migrations: Path = Path("migrations"),
        max_queue: int = 200_000,
        batch: int = 2_000,
        flush_s: float = 0.5,
    ) -> None:
        self._dsn = dsn
        self._migrations = migrations
        self._queue: asyncio.Queue[tuple[str, tuple[Any, ...]]] = asyncio.Queue(max_queue)
        self._batch = batch
        self._flush_s = flush_s
        self._pool: asyncpg.Pool | None = None
        self._task: asyncio.Task[None] | None = None
        self.dropped = 0
        self.written = 0

    @property
    def enabled(self) -> bool:
        return self._dsn is not None

    async def start(self) -> None:
        if not self._dsn:
            log.warning("DATABASE_URL not set: analytics recording disabled")
            return
        self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=2, command_timeout=30)
        async with self._pool.acquire() as conn:
            await migrate(conn, self._migrations)
        self._task = asyncio.create_task(self._run(), name="recorder")

    def write(self, table: str, row: tuple[Any, ...]) -> None:
        if self._dsn is None:
            return
        try:
            self._queue.put_nowait((table, row))
        except asyncio.QueueFull:
            self.dropped += 1

    def event(self, kind: str, message: str, level: str = "info", data: dict[str, Any] | None = None) -> None:
        self.write("events", (utcnow(), kind, level, message, _json.encode(data).decode() if data else None))

    async def replace(self, table: str, rows: list[tuple[Any, ...]]) -> None:
        """Atomically replace a small dimension table (e.g. the race lookup) at startup."""
        if self._pool is None:
            return
        columns = TABLES[table]  # only known tables: the name below is never user input
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute(f"DELETE FROM {table}")
            await conn.copy_records_to_table(table, records=rows, columns=columns)

    async def day_start_equity(self, day: str, not_before: float = 0.0) -> float | None:
        """Fair equity at the start of UTC day ``day`` (YYYY-MM-DD) as recorded before a restart: the first
        equity row of that day at or after ``not_before`` (Unix seconds; a deliberate later reference). None
        without a database or a row. The daily-loss halt measures from this, not from whatever the first
        tick after a restart happens to show (2026-10-04: that tick ran before fair values loaded and set
        the reference 8k too low; a restart also used to re-base the day)."""
        if self._pool is None:
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT equity_fair FROM equity WHERE ts >= GREATEST($1::timestamptz, to_timestamp($2)) "
                "AND ts < $1::timestamptz + interval '1 day' AND equity_fair IS NOT NULL ORDER BY ts LIMIT 1",
                datetime.fromisoformat(day).replace(tzinfo=UTC),
                not_before,
            )
        return float(row["equity_fair"]) if row else None

    async def latest_realized(self) -> dict[str, float]:
        """Realized P&L per exchange from the newest position snapshot (survives bot restarts)."""
        if self._pool is None:
            return {}
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT ON (exchange_id) exchange_id, realized_pnl FROM positions "
                "WHERE realized_pnl IS NOT NULL ORDER BY exchange_id, ts DESC",
            )
        return {r["exchange_id"]: float(r["realized_pnl"]) for r in rows}

    async def _run(self) -> None:
        while True:
            first = await self._queue.get()
            rows: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
            rows[first[0]].append(first[1])
            deadline = asyncio.get_running_loop().time() + self._flush_s
            count = 1
            while count < self._batch:
                timeout = deadline - asyncio.get_running_loop().time()
                if timeout <= 0:
                    break
                try:
                    table, row = await asyncio.wait_for(self._queue.get(), timeout)
                except TimeoutError:
                    break
                rows[table].append(row)
                count += 1
            try:
                await self._flush(rows)
            except Exception as exc:  # e.g. no connection while Postgres restarts: keep the writer alive
                self.dropped += count
                log.error("recorder flush failed (%d rows dropped): %r", count, exc)
                await asyncio.sleep(1.0)

    async def _flush(self, rows: dict[str, list[tuple[Any, ...]]]) -> None:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            for table, records in rows.items():
                columns = TABLES[table]
                good, bad = [], []
                for record in records:
                    (good if len(record) == len(columns) else bad).append(record)
                if good:
                    try:
                        await conn.copy_records_to_table(table, records=good, columns=columns)
                        self.written += len(good)
                    except Exception as exc:  # never let analytics take the bot down
                        self.dropped += len(good)
                        log.error("recorder flush of %s failed (%d rows dropped): %r", table, len(good), exc)
                if bad:
                    self.dropped += len(bad)
                    log.error(
                        "recorder dropped %d malformed %s rows (column count != %d)",
                        len(bad),
                        table,
                        len(columns),
                    )

    async def close(self) -> None:
        if self._task is not None:
            # drain what is queued, then stop
            pending: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
            while not self._queue.empty():
                table, row = self._queue.get_nowait()
                pending[table].append(row)
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            if pending:
                try:
                    await self._flush(pending)
                except Exception as exc:
                    log.error("recorder final flush failed: %r", exc)
        if self._pool is not None:
            await self._pool.close()


def ts() -> datetime:
    return utcnow()
