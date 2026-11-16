"""Order executor: the ONLY module allowed to talk to SIG's order endpoints.

Shadow mode (default; the only mode allowed until the user says otherwise) simulates every fill
against the live account state and records it. Live mode goes through SigClient with marketable
limits, immediate cancellation of unfilled remainders and a short expiration as backup.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import msgspec

from sigbot.core.db import Recorder
from sigbot.core.util import ONE, SIG_MIN_PRICE, from_mils, new_key, now_mono, sig_floor, utcnow
from sigbot.market.opening import SigOpenState, is_not_started
from sigbot.market.state import AccountState
from sigbot.sigapi import SigApiError, SigClient
from sigbot.sigapi import models as m
from sigbot.strategies.base import Intent, Leg
from sigbot.trading.risk import RiskGate

log = logging.getLogger(__name__)

EXPIRY_S = 30.0
"""Dead-man's switch on multi-leg orders, whose remainders are cancelled explicitly right after the response.
It must outlive SIG's admission latency: at the open (2026-10-01) order round trips ran 4-7 s and 8 s
expiries were rejected as "Invalid expirationDate" (already in the past)."""
MARKETABLE_EXPIRY_S = 15.0
"""Single marketable orders (snipes, exits, swaps) expire by themselves this soon, and their unfilled
remainder is left to it instead of being cancelled: SIG's 30 writes a minute are the binding constraint
(2026-10-03: a third of them went to cancels), and a remainder rests at a price that was good enough to take
a moment ago, so for 15 s it is a fine resting order. Order round trips run 0.5 s (p99 2.5 s, 9 s in the
busiest hour), so the expiry is always in the future at admission."""
SETTLE_S = MARKETABLE_EXPIRY_S + 1.0
"""After an order left a resting remainder, no new order on that exchange until the remainder has expired
(or filled: the account feed reports it meanwhile). Sending the same intent again while a remainder rests
would stack sells into an opening buy of the other side (SIG canonicalises a sell without holdings)."""


@dataclass(slots=True)
class ExecutionResult:
    intent: Intent
    shadow: bool
    filled: dict[str, float]  # exchange_id -> shares filled
    costs: dict[str, float]  # exchange_id -> total cost
    error: str = ""
    send_ms: float = 0.0
    resp_ms: float = 0.0
    not_open: bool = False
    """SIG answered "tournament has not started yet": neither an order nor an error."""
    not_sent: bool = False
    """Our request budget could not take the order in time (its price would have been stale): nothing
    reached SIG; neither an order nor an error."""

    @property
    def ok(self) -> bool:
        return not self.error and any(self.filled.values())


@dataclass(slots=True)
class PlacedQuote:
    order_id: int | None
    """Id of the resting order; None when nothing rests (filled at once). Negative in shadow mode."""
    filled: float = 0.0
    cost: float = 0.0


class Executor:
    def __init__(
        self,
        sig: SigClient,
        account: AccountState,
        risk: RiskGate,
        rec: Recorder,
        *,
        live: bool = False,
        on_fill: Callable[[ExecutionResult], None] | None = None,
        on_alert: Callable[[str], None] | None = None,
        opening: SigOpenState | None = None,
    ) -> None:
        self._sig = sig
        self._account = account
        self._risk = risk
        self._rec = rec
        self.live = live
        self._on_fill = on_fill
        self._on_alert = on_alert
        self.opening = opening or SigOpenState()
        self._locks: dict[str, asyncio.Lock] = {}
        self._settling: dict[str, float] = {}
        self.submitted = 0
        self.fills = 0
        self.lost_races = 0
        self._breaker_alerted = False
        self._shadow_quote_id = 0

    def settling(self, exchange_id: str) -> bool:
        """True while a remainder on this exchange may still be filling (see SETTLE_S)."""
        return self._settling.get(exchange_id, 0.0) > now_mono()

    async def submit(self, intent: Intent) -> ExecutionResult:
        """A lost race (order answered, nothing filled) counts as an order, not an error:
        only failed requests feed the error breaker."""
        signal_at = now_mono()
        result = await (self._submit_live(intent) if self.live else self._submit_shadow(intent))
        result.send_ms = (now_mono() - signal_at) * 1000
        if result.error and not (result.not_open or result.not_sent):
            self._risk.register_error()
            self._risk.check_rate()
            if self._risk.error_limited and not self._breaker_alerted:
                self._breaker_alerted = True
                self._alert(
                    "ERROR BREAKER tripped: too many failed orders in a minute; new trades pause until "
                    f"they age out (last: {result.error})",
                )
        elif not result.error:
            self._risk.register_order()
        if not self._risk.error_limited:
            self._breaker_alerted = False
        if result.ok:
            self.fills += 1
            if self._on_fill is not None:
                self._on_fill(result)
        self._record(result)
        return result

    def _alert(self, message: str) -> None:
        if self._on_alert is not None:
            self._on_alert(message)

    async def _submit_shadow(self, intent: Intent) -> ExecutionResult:
        filled: dict[str, float] = {}
        costs: dict[str, float] = {}
        for leg in intent.legs:
            filled[leg.exchange_id] = float(leg.quantity)
            costs[leg.exchange_id] = leg.price * leg.quantity / 1000
            self._account.apply_fill(
                leg.exchange_id,
                leg.side,
                float(leg.quantity),
                leg.price * leg.quantity / 1000,
            )
        return ExecutionResult(intent, True, filled, costs)

    async def _submit_live(self, intent: Intent) -> ExecutionResult:
        async with self._lock_for(intent):
            try:
                if len(intent.legs) == 1:
                    return await self._single(intent)
                return await self._multi(intent)
            except SigApiError as exc:
                if is_not_started(exc):
                    self.opening.on_not_started()
                    return ExecutionResult(intent, False, {}, {}, error=str(exc), not_open=True)
                if exc.not_sent:
                    return ExecutionResult(intent, False, {}, {}, error=str(exc), not_sent=True)
                if exc.outcome_unknown:
                    self._account.request_reconcile()
                    self._alert(f"order outcome UNKNOWN for {intent.race} ({intent.detail}): reconciling")
                return ExecutionResult(intent, False, {}, {}, error=str(exc))

    def _lock_for(self, intent: Intent) -> asyncio.Lock:
        # never two in-flight orders on the same exchange
        key = min(leg.exchange_id for leg in intent.legs)
        return self._locks.setdefault(key, asyncio.Lock())

    async def _single(self, intent: Intent) -> ExecutionResult:
        leg = intent.legs[0]
        t0 = now_mono()
        res = await self._sig.place_order(
            self._request(leg, MARKETABLE_EXPIRY_S),
            idempotency_key=new_key("o-"),
        )
        resp_ms = (now_mono() - t0) * 1000
        self.opening.on_order_accepted()
        filled_qty, filled_cost = self._apply_result(res, leg)
        filled = {leg.exchange_id: filled_qty}
        costs = {leg.exchange_id: filled_cost}
        result = ExecutionResult(intent, False, filled, costs, resp_ms=resp_ms)
        if (order_id := self._remainder(res)) is not None:
            log.info(
                "%s %s filled %s/%s -> remainder of order %s rests until it expires (%.0fs)",
                intent.strategy,
                leg.exchange_id,
                filled_qty,
                leg.quantity,
                order_id,
                MARKETABLE_EXPIRY_S,
            )
            # no cancel (a write): the remainder expires by itself; the exchange is held back meanwhile
            self._settling[leg.exchange_id] = now_mono() + SETTLE_S
            self.lost_races += 1
            self._rec.write(
                "lost_races",
                (
                    utcnow(),
                    intent.strategy,
                    leg.exchange_id,
                    leg.side.value,
                    from_mils(leg.price),
                    leg.quantity,
                    filled_qty,
                    0.0,
                    resp_ms,
                    0.0,
                ),
            )
        return result

    async def _multi(self, intent: Intent) -> ExecutionResult:
        """Placement is atomic (all legs or none), but each leg fills independently."""
        t0 = now_mono()
        res = await self._sig.place_multi_leg(
            [self._request(leg) for leg in intent.legs],
            idempotency_key=new_key("ml-"),
        )
        resp_ms = (now_mono() - t0) * 1000
        self.opening.on_order_accepted()
        filled = {leg.exchange_id: 0.0 for leg in intent.legs}
        costs = dict.fromkeys(filled, 0.0)
        remainders: list[tuple[int, str]] = []
        for item in res.results:
            leg = intent.legs[item.index]
            filled[leg.exchange_id], costs[leg.exchange_id] = self._apply_result(item.data, leg)
            if (order_id := self._remainder(item.data)) is not None:
                remainders.append((order_id, leg.exchange_id))
        for order_id, exchange_id in remainders:
            await self._cancel_remainder(order_id, exchange_id)
        if len(set(filled.values())) > 1:
            await self._complete_set(intent, filled, costs)
        if len(set(filled.values())) > 1:
            log.error("arb imbalance! %s filled legs=%s", intent.race, filled)
            self._rec.event(
                "arb_imbalance",
                f"{intent.race}: {filled}",
                level="error",
                data={"race": intent.race, "filled": filled},
            )
            self._alert(f"ARB IMBALANCE {intent.race}: filled {filled}")
        return ExecutionResult(intent, False, filled, costs, resp_ms=resp_ms)

    async def _complete_set(self, intent: Intent, filled: dict[str, float], costs: dict[str, float]) -> None:
        """One attempt to buy what the short legs of an uneven set are missing, at limits that keep the
        whole set at break-even or better (a YES set pays 1; NO on k exclusive outcomes pays at least k - 1).
        A set completed at break-even is still riskless; a lone leg is a directional bet. Updates
        ``filled``/``costs`` in place."""
        target = max(filled.values())
        payout = ONE if intent.legs[0].side is m.Side.YES else ONE * (len(intent.legs) - 1)

        def paid(leg: Leg) -> float:
            """Mils per share on a leg: its fill price, or its limit where nothing filled yet."""
            qty = filled[leg.exchange_id]
            return costs[leg.exchange_id] / qty * 1000 if qty > 0 else leg.price

        for leg in intent.legs:
            short = int(target - filled[leg.exchange_id])
            room = payout - sum(paid(other) for other in intent.legs if other is not leg)
            if short <= 0 or room < SIG_MIN_PRICE:
                continue
            fix = Intent(
                "arb_fix",
                intent.race,
                (Leg(leg.exchange_id, leg.side, sig_floor(int(room)), short),),
                0.0,
                intent.reason,
                f"complete {leg.side.value} {short} on {leg.exchange_id} at <= {sig_floor(int(room))}",
                now_mono(),
            )
            try:
                result = await self._single(fix)
            except SigApiError as exc:
                log.warning("completing arb leg %s failed: %s", leg.exchange_id, exc)
                self._risk.register_error()
                continue
            self._risk.register_order()
            self._record(result)
            got = result.filled.get(leg.exchange_id, 0.0)
            filled[leg.exchange_id] += got
            costs[leg.exchange_id] += result.costs.get(leg.exchange_id, 0.0)
            log.info("arb %s: completed %s of %s missing on %s", intent.race, got, short, leg.exchange_id)

    @staticmethod
    def _remainder(res: m.OrderResult) -> int | None:
        """Order id of a still-resting unfilled remainder, if any. ``open`` alone says the order rests:
        ``remainingQuantity`` is optional in SIG's response, and treating its absence as "nothing left"
        left remainders on the book until they expired (and filled late)."""
        return res.order_id if res.open and res.order_id else None

    async def _cancel_remainder(self, order_id: int, exchange_id: str) -> None:
        """Best effort; never loses the fill. The order also expires by itself within EXPIRY_S. Whatever the
        remainder does before the cancel lands arrives on the account feed (``AccountState.note_order``);
        the exchange is held back meanwhile."""
        self._settling[exchange_id] = now_mono() + SETTLE_S
        try:
            await self._sig.cancel_order(order_id, max_wait_s=EXPIRY_S)
        except SigApiError as exc:
            self._account.request_reconcile()
            if exc.status in (404, 409):
                log.info("order %s closed before the cancel (%s)", order_id, exc.code)
            else:
                log.warning("cancel of order %s failed: %s (expires within %.0fs)", order_id, exc, EXPIRY_S)
                self._risk.register_error()
        self._settling[exchange_id] = now_mono() + SETTLE_S

    async def place_quote(self, intent: Intent, expiry_s: float) -> PlacedQuote | None:
        """Rest one limit order (flow trading) that SIG expires by itself after ``expiry_s``. Any part that
        crosses fills at once. None when the order was not placed. Shadow mode sends nothing and records the
        quote under a negative id; it never fills."""
        leg = intent.legs[0]
        if not self.live:
            self._shadow_quote_id -= 1
            self._record_quote(intent, self._shadow_quote_id, "quoted", 0.0, 0.0, shadow=True)
            return PlacedQuote(self._shadow_quote_id)
        try:
            res = await self._sig.place_order(self._request(leg, expiry_s), idempotency_key=new_key("q-"))
        except SigApiError as exc:
            if is_not_started(exc):
                self.opening.on_not_started()
                return None
            if exc.not_sent:
                return None
            if exc.outcome_unknown:
                self._account.request_reconcile()
                self._alert(f"quote outcome UNKNOWN for {intent.race} ({intent.detail}): reconciling")
            self._risk.register_error()
            log.warning("quote %s %s failed: %s", leg.exchange_id, leg.side.value, exc)
            return None
        self._risk.register_order()
        self.opening.on_order_accepted()
        qty, cost = self._apply_result(res, leg)
        resting = self._remainder(res)
        self._record_quote(intent, res.order_id, "resting" if resting else "filled", qty, cost, shadow=False)
        return PlacedQuote(resting, qty, cost)

    async def place_ladder(
        self,
        intents: Sequence[Intent],
        expiry_s: float,
        *,
        max_wait_s: float | None = None,
    ) -> list[PlacedQuote | None] | None:
        """Rest several limit orders in one ``/orders/batch`` request (sweep catching). One result per intent,
        in order: None where that item was not placed (SIG reports per-item outcomes, and a 429 item means
        the trading engine rate limited it while the rest went through). A part that crosses fills at once.
        Returns None (no list) when the batch's outcome is unknown: SIG may well have placed it, so the
        caller must not assume the rungs are missing. Shadow mode sends nothing and records every rung
        under a negative id. ``max_wait_s``: not sent at all (every item None, no error counted) when the
        request budget could not take it within that long; quotes priced for now must not go out late."""
        if not self.live:
            out: list[PlacedQuote | None] = []
            for intent in intents:
                self._shadow_quote_id -= 1
                self._record_quote(intent, self._shadow_quote_id, "quoted", 0.0, 0.0, shadow=True)
                out.append(PlacedQuote(self._shadow_quote_id))
            return out
        requests = [self._request(intent.legs[0], expiry_s) for intent in intents]
        try:
            res = await self._sig.place_batch(requests, idempotency_key=new_key("sw-"), max_wait_s=max_wait_s)
        except SigApiError as exc:
            nothing: list[PlacedQuote | None] = [None] * len(intents)
            if exc.not_sent:
                log.debug("batch of %d not sent: %s", len(intents), exc.message)
                return nothing
            if is_not_started(exc):
                self.opening.on_not_started()
                return nothing
            self._risk.register_error()
            log.warning("ladder batch of %d failed: %s", len(intents), exc)
            for intent in intents:
                self._record_quote(intent, None, "failed", 0.0, 0.0, shadow=False, error=str(exc))
            if exc.outcome_unknown:
                self._account.request_reconcile()
                self._alert(
                    f"ladder batch outcome UNKNOWN ({len(intents)} rungs): SIG may have placed them; "
                    "reconciling and adopting whatever rests",
                )
                return None
            return nothing
        placed: list[PlacedQuote | None] = [None] * len(intents)
        rejected = 0
        for item in res.results:
            if not 0 <= item.index < len(intents):
                continue
            intent = intents[item.index]
            leg = intent.legs[0]
            order = _batch_order(item)
            if order is None:
                error = _batch_error(item)
                rejected += 1
                log.warning(
                    "ladder rung %s %s@%s rejected: %s",
                    leg.exchange_id,
                    leg.side.value,
                    leg.price,
                    error,
                )
                self._record_quote(intent, None, "failed", 0.0, 0.0, shadow=False, error=error)
                continue
            self.opening.on_order_accepted()
            qty, cost = self._apply_result(order, leg)
            resting = self._remainder(order)
            self._record_quote(
                intent,
                order.order_id,
                "resting" if resting else "filled",
                qty,
                cost,
                shadow=False,
            )
            placed[item.index] = PlacedQuote(resting, qty, cost)
        if any(p is not None for p in placed):
            # one request, one order for the rate breaker (SIG counts a batch as one write): per item, two
            # 25-quote batches a minute tripped "order rate limited" for every snipe and exit (2026-10-02)
            self._risk.register_order()
        if rejected:
            # one error per request, not per item: a batch of rungs bounced for insufficient balance is
            # one mistake of ours, and 25 of them must not trip the breaker that pauses every strategy
            self._risk.register_error()
        return placed

    async def cancel_quote(self, order_id: int) -> None:
        """Best effort: an order already filled or expired is fine, and every quote expires by itself."""
        if order_id < 0 or not self.live:
            return
        try:
            await self._sig.cancel_order(order_id, max_wait_s=2.0)
        except SigApiError as exc:
            if exc.not_sent:
                return  # the write budget is full: the quote expires by itself
            if exc.status not in (404, 409):
                log.warning("cancel of quote %s failed: %s", order_id, exc)
                self._risk.register_error()
            self._account.request_reconcile()

    async def cancel_exchange(self, exchange_id: str) -> None:
        """Cancel every one of our resting orders on one exchange in a single request (sweep ladders carry
        several rungs per exchange). Nothing to do in shadow mode."""
        if not self.live:
            return
        try:
            res = await self._sig.cancel_all(exchange_id=exchange_id)
        except SigApiError as exc:
            log.warning("cancel-all on %s failed: %s", exchange_id, exc)
            self._risk.register_error()
            self._account.request_reconcile()
            return
        if res.errors:
            log.warning("cancel-all on %s: %d orders not cancelled", exchange_id, len(res.errors))
            self._account.request_reconcile()

    async def cancel_everything(self) -> None:
        """Global cancel-all (one request). Nothing to do in shadow mode."""
        if not self.live:
            return
        try:
            await self._sig.cancel_all()
        except SigApiError as exc:
            log.warning("global cancel-all failed: %s (every resting order expires by itself)", exc)
            self._risk.register_error()
        self._account.request_reconcile()

    def _record_quote(
        self,
        intent: Intent,
        order_id: int | None,
        status: str,
        qty: float,
        cost: float,
        *,
        shadow: bool,
        error: str | None = None,
    ) -> None:
        leg = intent.legs[0]
        self._rec.write(
            "orders",
            (
                utcnow(),
                "",
                order_id,
                intent.strategy,
                intent.race,
                leg.exchange_id,
                leg.side.value,
                from_mils(leg.price),
                leg.quantity,
                shadow,
                status,
                intent.reason.value,
                qty,
                cost / qty if qty else None,
                cost,
                None,
                None,
                error,
            ),
        )

    def _request(self, leg: Leg, expiry_s: float = EXPIRY_S) -> m.OrderRequest:
        expiry = (datetime.now(UTC) + timedelta(seconds=expiry_s)).isoformat(timespec="seconds")
        return m.OrderRequest(
            exchange_id=leg.exchange_id,
            side=leg.side,
            action=m.Action.BUY,
            quantity=leg.quantity,
            price=from_mils(leg.price),
            expiration_date=expiry,
            tournament_id=self._sig.tournament_id,
        )

    def _apply_result(self, res: m.OrderResult, leg: Leg) -> tuple[float, float]:
        qty = float(res.quantity_traded or 0.0)
        cost = float(res.total_cost or 0.0) or qty * from_mils(leg.price)
        if qty > 0:
            self._account.apply_fill(leg.exchange_id, leg.side, qty, cost)
        if res.order_id:
            self._account.note_order(res.order_id, leg.exchange_id, leg.side, qty, quantity=leg.quantity)
        return qty, cost

    def _record(self, result: ExecutionResult) -> None:
        intent = result.intent
        now = utcnow()
        for leg in intent.legs:
            qty = result.filled.get(leg.exchange_id, 0.0)
            cost = result.costs.get(leg.exchange_id, 0.0)
            self._rec.write(
                "orders",
                (
                    now,
                    "",
                    None,
                    intent.strategy,
                    intent.race,
                    leg.exchange_id,
                    leg.side.value,
                    from_mils(leg.price),
                    leg.quantity,
                    result.shadow,
                    "filled" if qty > 0 else ("failed" if result.error else "unfilled"),
                    intent.reason.value,
                    qty,
                    cost / qty if qty else None,
                    cost,
                    result.send_ms,
                    result.resp_ms,
                    result.error or None,
                ),
            )
        leg = intent.legs[0]
        self._rec.write(
            "intents",
            (
                utcnow(),
                intent.strategy,
                intent.race,
                leg.exchange_id,
                leg.side.value,
                from_mils(leg.price),
                leg.quantity,
                intent.edge,
                intent.race,
                "submitted",
                intent.reason.value,
                intent.detail,
            ),
        )

    @property
    def mode(self) -> str:
        return "LIVE" if self.live else "SHADOW"


_order_decoder = msgspec.json.Decoder(m.OrderResult)
_item_error_decoder = msgspec.json.Decoder(m.BatchItemError)
_envelope_decoder = msgspec.json.Decoder(m.ErrorEnvelope)


def _batch_order(item: m.BatchItem) -> m.OrderResult | None:
    """The placed order inside a successful batch item; None for a failed one."""
    if not item.ok or not bytes(item.data):
        return None
    try:
        return _order_decoder.decode(item.data)
    except msgspec.DecodeError:
        return None


def _batch_error(item: m.BatchItem) -> str:
    """Failed items carry either ``{code, error}`` (engine rate limit, unknown outcome) or an error
    envelope; fall back to the raw payload."""
    if not bytes(item.data):
        return f"HTTP {item.status}"
    try:
        err = _item_error_decoder.decode(item.data)
        if err.code or err.error:
            return f"{item.status} {err.code}: {err.error}"
    except msgspec.DecodeError:
        pass
    try:
        info = _envelope_decoder.decode(item.data).error
        return f"{item.status} {info.code}: {info.message}"
    except msgspec.DecodeError:
        return f"{item.status} {bytes(item.data)[:200].decode(errors='replace')}"
