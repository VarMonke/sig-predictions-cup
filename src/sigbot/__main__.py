"""Entry point (``sigbot``): wires everything together and runs until SIGINT/SIGTERM.

Shadow mode by default: no SIG orders are ever placed unless TRADING_ENABLED=true AND the user
approved it. SIGTERM triggers a graceful shutdown; if live, all open orders are cancelled.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from sigbot.config import Settings
from sigbot.core.db import Recorder
from sigbot.core.latency import LatencyStats
from sigbot.core.log import RecorderLogHandler, setup_logging, stream_logs
from sigbot.core.util import RequestBudget, utcnow
from sigbot.discord.alerts import PARTY_NAMES, AlertSink, format_fill
from sigbot.discord.digest import StatusDigest
from sigbot.discord.whales import WhaleWatch
from sigbot.feeds.kalshi import KalshiPoller
from sigbot.feeds.polymarket import PolymarketFeed
from sigbot.market.band import BandTracker
from sigbot.market.fair import FairConfig, FairEngine
from sigbot.market.mapping import load_mapping, validate_against_sig
from sigbot.market.opening import SigOpenState
from sigbot.market.quotes import QuoteBoard
from sigbot.market.state import AccountState, SigBooks, market_of
from sigbot.ops.control import ControlServer
from sigbot.ops.health import SigHealth
from sigbot.ops.monitor import Monitor
from sigbot.sigapi import SigApiError, SigClient
from sigbot.sigapi.client import SIG_TIMEOUT_S
from sigbot.sigapi.models import AccountBatch, MarketBatch
from sigbot.sigapi.realtime import RealtimeClient
from sigbot.strategies import ArbConfig, ArbStrategy, ExitConfig, ExitStrategy, SnipeStrategy
from sigbot.strategies.exits import entry_floor
from sigbot.strategies.quoting import QuoteManager, resolve_markets, wants_all
from sigbot.strategies.recycle import Recycler
from sigbot.strategies.snipe import SnipeConfig
from sigbot.strategies.sweep import SweepLadder, resolve_scope
from sigbot.trading.engine import Engine
from sigbot.trading.executor import ExecutionResult, Executor
from sigbot.trading.risk import Allocator, RiskGate, Tiers, build_tiers, describe_tiers, party_signs

log = logging.getLogger(__name__)


async def run(settings: Settings) -> None:
    listener = setup_logging(settings.log_dir, settings.log_level)
    stop = asyncio.Event()
    rec = Recorder(settings.database_url)
    await rec.start()
    if rec.enabled:
        stream_logs(listener, RecorderLogHandler(rec.write, asyncio.get_running_loop()))
    alerts = AlertSink(
        settings.discord_bot_token,
        settings.discord_alert_channel_id,
        settings.discord_activity_channel_id,
    )
    latency = LatencyStats(rec, on_alert=alerts.alert)
    health = SigHealth(on_alert=alerts.alert)

    budget = None
    if settings.sig_budget_per_min != 90:
        per_min = settings.sig_budget_per_min
        budget = RequestBudget(
            limit=per_min,
            data_limit=max(5, per_min - 10),
            trade_headroom=min(20, per_min // 3),
        )
        log.warning("SIG request budget limited to %d/min (SIG_BUDGET_PER_MIN)", per_min)
    try:
        async with SigClient(
            settings.sig_base_url,
            settings.api_key,
            budget=budget,
            observe=latency.observe,
            on_outcome=health.record,
        ) as sig:
            await boot(settings, sig, stop, rec, alerts, latency, health)
    finally:
        await rec.close()
        listener.stop()


BOOT_TIMEOUT_S = 60.0
"""Startup reads wait this long for SIG (on 2026-10-02 06:00 UTC it answered in 15-70 s or with 503s for
twenty minutes, and the 10 s trading timeout made the bot crash-loop through Compose 15 times)."""


async def patiently[T](what: str, call: Callable[[], Awaitable[T]], stop: asyncio.Event) -> T:
    """Keep trying a read-only startup call until SIG answers: nothing is at risk before the bot trades,
    and a bot that waits keeps its place in line for the recovery instead of restarting from scratch."""
    delay = 5.0
    while True:
        try:
            return await call()
        except SigApiError as exc:
            if stop.is_set():
                raise
            log.warning("startup: %s failed (%s); SIG may be down, trying again in %.0fs", what, exc, delay)
            await asyncio.sleep(delay)
            delay = min(60.0, delay * 1.5)


async def boot(
    settings: Settings,
    sig: SigClient,
    stop: asyncio.Event,
    rec: Recorder,
    alerts: AlertSink,
    latency: LatencyStats,
    health: SigHealth,
) -> None:
    sig.set_timeout(BOOT_TIMEOUT_S)
    tournament = await patiently("tournament", lambda: sig.tournament(settings.tournament_slug), stop)
    me = await patiently("account", sig.account, stop)
    sig.tournament_id = tournament.id
    mapping = load_mapping(settings.mapping_path)
    for problem in validate_against_sig(mapping, await patiently("markets", sig.all_markets, stop)):
        log.warning("mapping: %s", problem)
    log.info(
        "tournament %s status=%s members=%s markets=%s balance=%s",
        tournament.slug,
        tournament.status,
        tournament.member_count,
        tournament.market_count,
        tournament.my_balance,
    )

    exchanges = {o.sig_exchange_id: o.sig_market_id for r in mapping.races for o in r.outcomes}
    books = SigBooks(
        sig,
        exchanges,
        rec,
        scan_every_s=settings.sig_scan_every_s,
        observe=latency.observe,
        on_alert=alerts.alert,
    )
    await books.scan_all()
    opening = SigOpenState(on_alert=alerts.alert)
    account = AccountState(
        sig,
        settings.tournament_slug,
        rec,
        paper=not settings.trading_enabled,
        on_drift=alerts.alert,
        on_meta=opening.on_meta,
    )
    account.feed_silent = lambda: books.feed_silent
    await patiently("positions and orders", account.reconcile, stop)
    if settings.trading_enabled:
        account.realized.update(await rec.latest_realized())
    sig.set_timeout(SIG_TIMEOUT_S)

    board = QuoteBoard()
    pm = PolymarketFeed(mapping.pm_tokens(), board, observe=latency.observe)
    kalshi = KalshiPoller(
        mapping.kalshi_tickers(),
        board,
        max_rps=settings.kalshi_max_rps,
        observe=latency.observe,
    )
    fair = FairEngine(mapping, board, FairConfig(), rec)
    fair.on_sharp_move(lambda _ex, outcome: kalshi.boost(outcome.kalshi_tickers))

    limits = settings.risk
    tier_defs = build_tiers(
        limits.max_deployed_frac,
        limits.max_race_loss,
        limits.max_order_notional,
        limits.tier_deployed,
    )
    log.info("risk tiers: %s", describe_tiers(tier_defs))
    # SIG's own level per market (band.py): priced from the books without our own orders once the engine
    # exists (its view strips them); until then from the raw books
    band = BandTracker(lambda ex: books.books.get(ex))
    risk = RiskGate(
        account,
        books=books,
        carry_cap_frac=limits.carry_cap_frac,
        band_of=band.band,
        max_book_age_s=limits.max_book_age_s,
        daily_loss_frac=limits.daily_loss_frac,
        swing_loss_frac=limits.max_swing_loss_frac,
        party_sign=party_signs(mapping),
        max_orders_per_minute=limits.max_orders_per_minute,
        max_errors_per_minute=limits.max_errors_per_minute,
        tiers=tier_defs,
        min_order_cost=limits.min_order_cost,
        paused_strategies=set(limits.paused_strategies),
        race_loss_frac=limits.max_race_loss_frac,
        tail_loss_frac=limits.max_tail_loss_frac,
    )
    if limits.paused_strategies:
        log.warning("paused from startup (PAUSED_STRATEGIES): %s", ", ".join(limits.paused_strategies))
    if limits.carry_cap_frac < 1.0:
        log.info(
            "carry cap: at most %.0f%% of equity in positions bought at or above SIG's own level (band = "
            "%.0f-minute median mid; buys under it are round-trip material and free)",
            limits.carry_cap_frac * 100,
            band.window_s / 60,
        )
    tiers = Tiers(risk, drawdown_frac=limits.drawdown_step_down_frac)
    if limits.start_tier > 1:
        tiers.set_manual(limits.start_tier)
        rec.write("ladder", (utcnow(), 1, risk.tier, "start"))
        log.warning(
            "starting in tier %d (RISK_START_TIER): no automatic promotion, drawdowns still step down",
            risk.tier,
        )

    parties = {o.sig_exchange_id: o.party.value for r in mapping.races for o in r.outcomes}
    party_names = {ex: PARTY_NAMES.get(code, code) for ex, code in parties.items()}

    def fair_yes(exchange_id: str) -> int | None:
        fv = fair.get(exchange_id)
        return fv.value if fv is not None else None

    def on_fill(result: ExecutionResult) -> None:
        message = format_fill(result, parties, fair_yes, cash=account.cash)
        if message is not None:
            alerts.activity(message)

    executor = Executor(
        sig,
        account,
        risk,
        rec,
        live=settings.trading_enabled,
        on_fill=on_fill,
        on_alert=alerts.alert,
        opening=opening,
    )
    if settings.trading_enabled:
        log.warning("LIVE MODE: cancelling any open orders at startup")
        try:
            await sig.cancel_all()
        except SigApiError as exc:
            log.warning("startup cancel-all failed: %s (any open order expires within seconds)", exc)
        # the process we replaced may have spent this minute's writes: SIG's window does not restart with us
        sig.budget.assume_writes_spent()
    else:
        log.info("SHADOW MODE: no real orders will be placed")

    # a new position must out-earn the hurdle, or the hurdle exit would sell it straight back
    floor = entry_floor(settings.exit_hurdle)
    snipe_roc = max(settings.snipe_min_roc, floor)
    engine = Engine(
        mapping,
        books,
        fair,
        account,
        risk,
        tiers,
        Allocator(),  # snipes may use the whole deployed cap: arbs must clear a return on capital now
        executor,
        rec,
        [
            ArbStrategy(
                ArbConfig(
                    min_return_on_capital=max(settings.arb_min_roc, floor),
                    exit_hurdle=settings.exit_hurdle,
                    exit_hurdle_max_loss=settings.exit_hurdle_max_loss,
                ),
            ),
            SnipeStrategy(
                SnipeConfig(kelly_fraction=settings.snipe_kelly_fraction, min_return_on_capital=snipe_roc),
            ),
            ExitStrategy(
                ExitConfig(
                    recycle_lopsided=settings.exit_recycle_lopsided,
                    recycle_margin=settings.exit_recycle_margin,
                    hurdle=settings.exit_hurdle,
                    hurdle_max_loss=settings.exit_hurdle_max_loss,
                    hurdle_per_day=settings.exit_hurdle_per_day,
                    unwind_exchanges=settings.exit_unwind_exchanges,
                    unwind_after=settings.exit_unwind_after,
                    unwind_max_loss=settings.exit_unwind_max_loss,
                ),
                is_carry=risk.trapped,
            ),
        ],
    )
    band.book_of = engine.book
    books.subscribe(band.wake)
    if settings.exit_unwind_exchanges:
        log.warning(
            "UNWIND: positions on %s are sold at the inside down to %.1fc under cost from %s",
            ", ".join(settings.exit_unwind_exchanges),
            settings.exit_unwind_max_loss / 10,
            datetime.fromtimestamp(settings.exit_unwind_after, UTC).strftime("%Y-%m-%d %H:%M UTC")
            if settings.exit_unwind_after
            else "now",
        )
    if settings.exit_hurdle > 0 or settings.exit_hurdle_per_day > 0:
        log.warning(
            "capital hurdle on: holds and arb sets earning <= %.1f%% to settlement (or <= %.2f%% a day of "
            "trading left) are sold (up to %.1fc under cost, never past a market's booked gains); new snipes "
            "need %.1f%% on their capital, new arb sets %.1f%%",
            settings.exit_hurdle * 100,
            settings.exit_hurdle_per_day * 100,
            settings.exit_hurdle_max_loss / 10,
            snipe_roc * 100,
            max(settings.arb_min_roc, floor) * 100,
        )
    if settings.exit_recycle_lopsided:
        log.warning(
            "lopsided positions are recycled: sold whenever the market pays cost + %.1fc",
            settings.exit_recycle_margin / 10,
        )
    if settings.recycle.enabled:
        engine.recycler = Recycler(
            engine,
            settings.recycle,
            {ex: r for ex, (r, _) in mapping.by_exchange().items()},
            parties=party_names,
            on_activity=alerts.activity,
        )
        log.info(
            "capital recycling on: a trade the deployed cap rejects may sell slower positions at >= cost "
            "when it beats their forgone rate by %.0f%%",
            settings.recycle.margin * 100,
        )
        if settings.recycle.reserve_frac > 0:
            log.warning(
                "cash reserve on: below %.0f%% of equity in cash, positions earning <= %.0f%% to settlement "
                "are sold (up to %.1fc under cost), least edge first",
                settings.recycle.reserve_frac * 100,
                settings.recycle.reserve_max_rate * 100,
                settings.recycle.reserve_max_loss / 10,
            )

    def on_resync(topic: str) -> None:
        if topic.startswith("user:"):
            account.request_reconcile()
        elif (market_id := market_of(topic)) is not None:
            books.resync_market(market_id)
        else:
            books.resync()

    quote_races = resolve_markets(settings.quotes.markets, mapping.races)
    quote_all = wants_all(settings.quotes.markets)
    all_races = {ex: r for ex, (r, _) in mapping.by_exchange().items()}
    quotes = None
    if quote_races or quote_all:
        quotes = QuoteManager(
            engine,
            executor,
            settings.quotes,
            quote_races,
            parties=party_names,
            on_activity=alerts.activity,
            everything=quote_all,
            all_races=all_races,
        )
        books.subscribe(quotes.wake)
        fair.subscribe(quotes.wake)
        engine.quote_inventory = lambda ex: quotes.acquired.get(ex, 0.0) if quotes is not None else 0.0
        engine.quoting_live = quotes.is_quoting
        engine.own_resting.append(quotes.resting)  # the strategies must never take our quotes for liquidity

    sweep = None
    if settings.sweep.markets:
        scope, sweep_races = resolve_scope(settings.sweep.markets, mapping.races)
        if scope.named or scope.everything:
            sweep = SweepLadder(
                engine,
                executor,
                settings.sweep,
                scope,
                sweep_races,
                parties=party_names,
                on_activity=alerts.activity,
                band_of=band.band,
            )
            books.subscribe(sweep.wake)
            fair.subscribe(sweep.wake)
            engine.own_resting.append(sweep.resting)
            if quotes is not None:
                quotes.other_resting = sweep.resting  # rungs are ours: not competition, and they may fill
                sweep.foreign_orders = (
                    quotes.order_ids
                )  # never adopted as rungs, never cancelled as duplicates
                sweep.on_exchange_wiped = quotes.forget_exchange  # a cancel-all took the quotes there too

    whales = WhaleWatch(fair.get, band.band, all_races, parties, alerts.activity, rec=rec)

    def on_market_batch(topic: str, batch: MarketBatch) -> None:
        trades = books.on_market_batch(topic, batch)  # replayed prints taken out
        if trades:
            opening.on_trade_print()
            whales.on_trades(trades)
            if sweep is not None:
                sweep.on_trades(trades)
            if quotes is not None:
                quotes.on_trades(trades)

    def on_account_batch(batch: AccountBatch) -> None:
        account.on_account_batch(batch)
        if quotes is not None:
            quotes.on_account_batch(batch)
        if sweep is not None:
            sweep.on_account_batch(batch)

    market_topics = [
        f"tournament:{tournament.id}:market:{mk}" for mk in sorted(set(exchanges.values()), key=int)
    ]
    rt = RealtimeClient(
        sig,
        [f"user:{me.id}", *market_topics],
        on_market_batch=on_market_batch,
        on_account_batch=on_account_batch,
        on_resync=on_resync,
    )

    def halt(reason: str | None = None) -> None:
        risk.halted = True
        alerts.alert(f"HALT: {reason or 'manual'}")

    def resume() -> None:
        risk.halted = False
        alerts.alert("RESUMED")

    def pause(strategy: str, paused: bool = True) -> None:
        if paused:
            risk.paused_strategies.add(strategy)
        else:
            risk.paused_strategies.discard(strategy)
        alerts.alert(f"{'PAUSED' if paused else 'UNPAUSED'} {strategy}")

    def manual(exchange_id: str, paused: bool) -> None:
        if paused:
            risk.manual_paused.add(exchange_id)
        else:
            risk.manual_paused.discard(exchange_id)
        alerts.alert(f"MANUAL {'pause' if paused else 'unpause'} {exchange_id}")

    def set_tier(level: str) -> str:
        if level == "auto":
            tiers.auto_promote = True
            alerts.alert(f"TIER automatic again (now tier {risk.tier})")
            return "auto"
        if not level.isdigit() or not 1 <= int(level) <= len(risk.tiers):
            raise ValueError(f"tier must be 1-{len(risk.tiers)} or auto")
        before = risk.tier
        tiers.set_manual(int(level))
        rec.write("ladder", (utcnow(), before, risk.tier, "manual"))
        alerts.alert(f"TIER {before}->{risk.tier} set by hand (automatic promotion off; /tier auto to undo)")
        return level

    def status() -> dict:
        out = engine.status()
        if sweep is not None:
            out["sweep"] = sweep.stats()
        if quotes is not None:
            out["quotes"] = quotes.stats()
        return out

    control = ControlServer(
        settings.control_host,
        settings.control_port,
        os.environ.get("CONTROL_TOKEN", "shadow"),
        status,
        halt,
        resume,
        pause,
        manual,
        set_tier,
    )

    monitor = Monitor(engine, sig, settings.tournament_slug, alerts, opening=opening)
    await rec.replace("races", monitor.race_rows())
    # the daily-loss halt measures the UTC day, not the time since this restart: seed the reference from
    # what was recorded before we came up (or from RISK_DAILY_LOSS_FROM on, a deliberate re-base)
    today = utcnow().strftime("%Y-%m-%d")
    seed = await rec.day_start_equity(today, limits.daily_loss_from)
    if seed is not None:
        tiers.seed_day(today, seed)
        log.warning(
            "daily-loss reference for %s: %.0f (%s); halt below %.0f",
            today,
            seed,
            "RISK_DAILY_LOSS_FROM" if limits.daily_loss_from else "the day's first recorded equity",
            seed * (1 - limits.daily_loss_frac),
        )
    digest = StatusDigest(
        engine,
        monitor,
        latency,
        board,
        rt,
        alerts,
        health=health,
        sweep=sweep,
        quotes=quotes,
    )

    tasks = [
        asyncio.create_task(books.run(stop), name="books"),
        asyncio.create_task(account.run(stop), name="account"),
        asyncio.create_task(pm.run(stop), name="polymarket"),
        asyncio.create_task(kalshi.run(stop) if settings.kalshi_enabled else asyncio.sleep(0), name="kalshi"),
        asyncio.create_task(rt.run(stop), name="realtime"),
        asyncio.create_task(engine.run(stop), name="engine"),
        asyncio.create_task(monitor.run(stop), name="monitor"),
        asyncio.create_task(latency.run(stop), name="latency"),
        asyncio.create_task(control.run(stop), name="control"),
        asyncio.create_task(alerts.run(stop), name="alerts"),
        asyncio.create_task(digest.run(stop), name="status"),
        asyncio.create_task(health.run(stop), name="sig-health"),
    ]
    if quotes is not None:
        tasks.append(asyncio.create_task(quotes.run(stop), name="quotes"))
    if sweep is not None:
        tasks.append(asyncio.create_task(sweep.run(stop), name="sweep"))

    loop = asyncio.get_running_loop()
    for sig_name in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig_name, stop.set)

    def on_task_done(task: asyncio.Task[object]) -> None:
        # No task returns on its own while the bot runs: one that does (or crashes) would leave the bot
        # half-alive, e.g. recording and posting status without trading. Stop instead; Compose restarts.
        if stop.is_set() or task.cancelled() or (task.get_name() == "kalshi" and not settings.kalshi_enabled):
            return
        exc = task.exception()
        why = f"crashed: {exc!r}" if exc is not None else "exited unexpectedly"
        log.critical("task %s %s; shutting down", task.get_name(), why, exc_info=exc)
        alerts.alert(f"TASK {task.get_name()} {why}. Bot shutting down (expect a restart).")
        stop.set()

    for task in tasks:
        task.add_done_callback(on_task_done)

    await stop.wait()
    # the alert sink needs a moment to deliver a shutdown alert before its task is cancelled
    await asyncio.sleep(1.0)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    if settings.trading_enabled:
        log.info("live mode: cancelling all open orders on shutdown")
        try:
            await sig.cancel_all()
        except Exception:
            log.exception("final cancel-all failed (short expirations are the backup)")


def main() -> None:
    with contextlib.suppress(ImportError):
        import uvloop

        uvloop.install()
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run(Settings.from_env()))


if __name__ == "__main__":
    main()
