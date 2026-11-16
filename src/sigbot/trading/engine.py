"""The decision engine: SIG book / external quote events -> strategy intents -> risk -> executor.

Implements the strategies' View. Processes one dirty race at a time (coalesced), so intents are
always evaluated against consistent state, and never submits twice for the same exchange+price.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence

from sigbot.core.db import Recorder
from sigbot.core.util import now_mono, utcnow
from sigbot.market.fair import Fair, FairEngine
from sigbot.market.mapping import Mapping, Outcome, Race
from sigbot.market.state import AccountState, Book, Holding, OwnResting, SigBooks, without_own
from sigbot.sigapi import models as m
from sigbot.strategies import Strategy
from sigbot.strategies.base import Intent, View
from sigbot.strategies.recycle import TRIGGER, Recycler
from sigbot.trading.executor import ExecutionResult, Executor
from sigbot.trading.risk import Allocator, RiskGate, Tiers, race_cap_scale

log = logging.getLogger(__name__)


class Engine(View):
    def __init__(
        self,
        mapping: Mapping,
        books: SigBooks,
        fair: FairEngine,
        account: AccountState,
        risk: RiskGate,
        tiers: Tiers,
        allocator: Allocator,
        executor: Executor,
        rec: Recorder,
        strategies: Sequence[Strategy],
        *,
        min_requeue_s: float = 2.0,
        recycler: Recycler | None = None,
    ) -> None:
        self.mapping = mapping
        self.recycler = recycler
        self.quote_inventory: Callable[[str], float] | None = None
        """Set by main when flow trading is on: exchange -> shares the quoter bought net (see View.quoted)."""
        self.quoting_live: Callable[[str], bool] | None = None
        """Set by main when flow trading is on: whether a quote of ours rests on the exchange right now."""
        self.own_resting: list[OwnResting] = []
        """Sources of our own resting orders (the quoter's quotes, the ladder's rungs): ``book`` takes them
        out of what the strategies see, so none of them ever trades into our own order."""
        self.books = books
        self.fair_engine = fair
        self.account = account
        self.risk = risk
        self.tiers = tiers
        self.allocator = allocator
        self.executor = executor
        self.rec = rec
        self.strategies = list(strategies)
        self._dirty: set[str] = set()
        self._wake = asyncio.Event()
        self._inflight: set[str] = set()
        self._last_submit: dict[tuple[str, int], float] = {}
        self.min_requeue_s = min_requeue_s
        self.races: dict[str, Race] = {r.key: r for r in mapping.races}
        self.outcomes: dict[str, Outcome] = {o.sig_exchange_id: o for r in mapping.races for o in r.outcomes}
        books.subscribe(self.mark_exchange_dirty)
        fair.subscribe(self._on_fair_change)
        self.stats = {
            "intents": 0,
            "approved": 0,
            "rejected": 0,
            "submitted": 0,
            "shadow_fills": 0,
            "swaps": 0,
        }

    def book(self, exchange_id: str) -> Book:
        """SIG's book as the strategies should see it: other participants' liquidity only. The quoter and the
        ladder read ``raw_book`` and account for their own orders themselves."""
        raw = self.books.books[exchange_id]
        if not self.own_resting:
            return raw
        yes = [t for source in self.own_resting for t in source(exchange_id, m.Side.YES)]
        no = [t for source in self.own_resting for t in source(exchange_id, m.Side.NO)]
        return without_own(raw, yes, no) if yes or no else raw

    def raw_book(self, exchange_id: str) -> Book:
        return self.books.books[exchange_id]

    def is_quoting(self, exchange_id: str) -> bool:
        return self.quoting_live(exchange_id) if self.quoting_live is not None else False

    def fair(self, exchange_id: str) -> Fair | None:
        return self.fair_engine.get(exchange_id)

    def holding(self, exchange_id: str) -> Holding | None:
        return self.account.holdings.get(exchange_id)

    def quoted(self, exchange_id: str) -> float:
        return self.quote_inventory(exchange_id) if self.quote_inventory is not None else 0.0

    def realized(self, exchange_id: str) -> float:
        return self.account.realized.get(exchange_id, 0.0)

    def days_left(self, now_s: float | None = None) -> float:
        """Days until trading ends (at least 1); 31 when the tournament's end is unknown."""
        import time

        end = self._parse_iso(self.account.end_date) if self.account.end_date else None
        if end is None:
            return 31.0
        now_s = time.time() if now_s is None else now_s
        return max(1.0, (end - now_s) / 86_400)

    @property
    def equity(self) -> float:
        value = self.account.cash
        for holding in self.account.holdings.values():
            fv = self.fair_engine.get(holding.exchange_id)
            price = fv.value / 1000 if fv else (holding.sig_price or holding.avg_cost)
            value += abs(holding.qty) * (price if holding.qty > 0 else 1 - price)
        return value

    def fair_map(self, race: Race) -> dict[str, float]:
        out: dict[str, float] = {}
        for outcome in race.outcomes:
            fv = self.fair_engine.get(outcome.sig_exchange_id)
            if fv is not None:
                out[outcome.sig_exchange_id] = fv.value / 1000
        return out

    def all_fair_map(self) -> dict[str, float]:
        """Fair values for every mapped exchange that currently has one."""
        out: dict[str, float] = {}
        for exchange_id in self.outcomes:
            fv = self.fair_engine.get(exchange_id)
            if fv is not None:
                out[exchange_id] = fv.value / 1000
        return out

    def mark_exchange_dirty(self, exchange_id: str) -> None:
        if exchange_id not in self.outcomes:
            return
        for race in self.races.values():
            if any(o.sig_exchange_id == exchange_id for o in race.outcomes):
                self._dirty.add(race.key)
                self._wake.set()
                return

    def _on_fair_change(self, exchange_id: str, _fair: Fair) -> None:
        self.mark_exchange_dirty(exchange_id)

    def on_market_batch(self, topic: str, batch: m.MarketBatch) -> None:
        self.books.on_market_batch(topic, batch)

    def in_trading_window(self, now_s: float | None = None) -> bool:
        """True between tournament start and end (or always in shadow/dev)."""
        import time

        now_s = time.time() if now_s is None else now_s
        if not self.account.start_date or not self.account.end_date:
            return True  # unknown: let SIG reject; better feedback
        start = self._parse_iso(self.account.start_date)
        end = self._parse_iso(self.account.end_date)
        if start is None or end is None:
            return True
        return start <= now_s <= end

    @staticmethod
    def _parse_iso(value: str) -> float | None:
        from datetime import UTC, datetime

        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC).timestamp()
        except ValueError:
            return None

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            from sigbot.core.util import wait_event_or_stop

            await wait_event_or_stop(self._wake, stop, 1.0)
            self._wake.clear()
            if stop.is_set():
                break
            while self._dirty and not stop.is_set():
                race_key = self._dirty.pop()
                race = self.races.get(race_key)
                if race is None:
                    continue
                try:
                    await self.process_race(race)
                except Exception:
                    log.exception("processing race %s failed", race_key)
            try:
                await self.keep_reserve()
            except Exception:
                log.exception("cash reserve pass failed")
            if not self._dirty:
                # nothing pending: log a light heartbeat line periodically is handled by monitor
                pass

    async def keep_reserve(self) -> None:
        """Top the cash reserve back up (``RecycleConfig.reserve_frac``): the recycler picks the slowest
        positions, the gate checks each exit (halt, pause, stale book, rate limits) like any other."""
        if self.recycler is None or not self.in_trading_window() or not self.executor.opening.accepting():
            return
        equity = self.equity
        exits = self.recycler.reserve_exits(equity)
        if not exits:
            return
        fair_map = self.all_fair_map()
        for intent in exits:
            race = self.races.get(intent.race)
            if race is None:
                continue
            leg = intent.legs[0]
            if leg.exchange_id in self._inflight or self.executor.settling(leg.exchange_id):
                self._record_intent(intent, race, "rejected:settling")
                continue
            approved, reason = self.risk.check(intent, fair_map, equity, race=race)
            if approved is None:
                self._record_intent(intent, race, f"rejected:{reason}")
                continue
            self._record_intent(approved, race, "approved")
            await self._submit(approved, race)

    async def process_race(self, race: Race) -> None:
        if not self.in_trading_window() or not self.executor.opening.accepting():
            return
        now = now_mono()
        intents: list[Intent] = []
        for strategy in self.strategies:
            intents.extend(strategy.evaluate(race, self, now))
        if not intents:
            return
        fair_map = self.all_fair_map()
        equity = self.equity
        self.allocator.refresh(self.account.holdings)
        chosen = self.allocator.allocate(intents, self.risk.tier_def().deployed_frac * equity)
        for intent in chosen:
            self.stats["intents"] += 1
            for leg in intent.legs:
                inflight = leg.exchange_id in self._inflight
                if inflight or self.executor.settling(leg.exchange_id):
                    self.rec.write(
                        "intents",
                        (
                            utcnow(),
                            intent.strategy,
                            race.key,
                            leg.exchange_id,
                            leg.side.value,
                            leg.price / 1000,
                            leg.quantity,
                            intent.edge,
                            race.key,
                            "rejected_inflight" if inflight else "rejected:settling",
                            intent.reason.value,
                            intent.detail,
                        ),
                    )
                    break
            else:
                confidence = race_cap_scale((self.fair(leg.exchange_id) for leg in intent.legs), intent.edge)
                approved, reason = self.risk.check(intent, fair_map, equity, race=race, confidence=confidence)
                # the cap is full of slower positions: sell just enough of them, then ask again
                if (
                    approved is None
                    and reason == TRIGGER
                    and self.recycler is not None
                    and await self._make_room(intent, race, equity)
                ):
                    approved, reason = self.risk.check(
                        intent,
                        fair_map,
                        self.equity,
                        race=race,
                        confidence=confidence,
                    )
                    if approved is None:
                        self.recycler.refused(intent)
                if approved is None:
                    self.stats["rejected"] += 1
                    self._record_intent(intent, race, f"rejected:{reason}")
                    continue
                self.stats["approved"] += 1
                self._record_intent(approved, race, "approved")
                if await self._submit(approved, race):
                    self.tiers.on_trade()

    async def _make_room(self, intent: Intent, race: Race, equity: float) -> bool:
        """Run the swap valve for ``intent``; True when at least one exit filled."""
        assert self.recycler is not None
        freed = False
        for exit_intent in self.recycler.propose(intent, race, equity):
            exit_race = self.races.get(exit_intent.race)
            if exit_race is None:
                continue
            self._record_intent(exit_intent, exit_race, "approved")
            if await self._submit(exit_intent, exit_race):
                self.stats["swaps"] += 1
                freed = True
        return freed

    async def _submit(self, intent: Intent, race: Race) -> bool:
        # never double-submit the same exchange+price within the requeue window
        key = (intent.legs[0].exchange_id, intent.legs[0].price)
        last = self._last_submit.get(key, 0.0)
        if now_mono() - last < self.min_requeue_s:
            self._record_intent(intent, race, "rejected:requeue")
            return False
        for leg in intent.legs:
            self._inflight.add(leg.exchange_id)
        self._last_submit[key] = now_mono()
        flat = [leg.exchange_id for leg in intent.legs if self.account.position(leg.exchange_id) == 0]
        try:
            result: ExecutionResult = await self.executor.submit(intent)
        finally:
            for leg in intent.legs:
                self._inflight.discard(leg.exchange_id)
        self.stats["submitted"] += 1
        if result.ok:
            self.allocator.on_fill(intent.strategy, flat)
            if result.shadow:
                self.stats["shadow_fills"] += 1
        return result.ok

    def _record_intent(self, intent: Intent, race: Race, decision: str) -> None:
        leg = intent.legs[0]
        self.rec.write(
            "intents",
            (
                utcnow(),
                intent.strategy,
                race.key,
                leg.exchange_id,
                leg.side.value,
                leg.price / 1000,
                leg.quantity,
                intent.edge,
                race.key,
                decision,
                intent.reason.value,
                intent.detail,
            ),
        )

    def national_delta(self) -> float:
        """P&L per unit logit of a uniform pro-Dem swing: sum of party sign * p(1-p) * signed shares."""
        return self.risk._swing_delta(self.all_fair_map())

    def status(self) -> dict:
        return {
            "mode": self.executor.mode,
            "tier": self.risk.tier,
            "tier_auto": self.tiers.auto_promote,
            "halted": self.risk.halted,
            "cash": self.account.cash,
            "equity_fair": self.equity,
            "deployed": self.account.deployed,
            "positions": len(self.account.holdings),
            "national_delta": self.national_delta(),
            "stats": self.stats,
        }
