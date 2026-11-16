"""Risk gate, capital-tier ladder and intent allocator.

Every intent passes through the gate before the executor sees it. The gate:
  * blocks everything while halted / strategy paused / exchange manually paused,
  * caps per-order notional (tier-scaled) and total deployed capital,
  * caps the loss per race: every possible result of the race is checked (plus "none of the listed
    outcomes" when the race is not exhaustive), so YES-Dem + NO-Rep counts as one doubled bet while a hedged
    D/R set passes; each result's loss counts in proportion to its stressed fair probability (a coin flip in
    full, a 1% long shot 5%), a hard ceiling (share of equity) bounds the plain loss in any result, and the
    cap shrinks when fair value is less trustworthy,
  * scales down (never up) oversized quantities so a bad proposal can't exceed a limit,
  * reserves a request-budget headroom for orders and trips a breaker when errors pile up,
  * caps exposure to a uniform national swing across all races (Dem and Rep positions offset).
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field

from sigbot.core.util import is_sig_tick, now_mono
from sigbot.market.fair import Fair, fair_confidence
from sigbot.market.mapping import Mapping as RaceMapping
from sigbot.market.mapping import Party, Race
from sigbot.market.state import AccountState, Holding, SigBooks
from sigbot.sigapi.models import Side
from sigbot.strategies.base import Intent, Leg

CARRY_FLIP_MARGIN = 10
"""Mils under SIG's band a share must be bought to count as round-trip material rather than carry: the flip
sells at the band less a tick, and a tick of profit is not a trade."""

SWING_SHIFT = 0.3
"""The uniform national move (logit) the swing cap is sized for: ~7c on a 50c race, less on lopsided ones.
A YES share's P&L sensitivity to it is p*(1-p) per unit logit, signed +1 for Dem, -1 for Rep outcomes."""

PARTY_SIGN = {Party.D: 1, Party.R: -1, Party.I: 0}


def party_signs(mapping: RaceMapping) -> dict[str, int]:
    return {o.sig_exchange_id: PARTY_SIGN[o.party] for r in mapping.races for o in r.outcomes}


@dataclass(frozen=True, slots=True)
class TierDef:
    tier: int
    deployed_frac: float
    race_loss: float
    """Max worst-case loss per race in SUSQies, before confidence scaling."""
    max_order_notional: float


TIER_SCALE: tuple[tuple[float, float, float], ...] = (
    (1.0, 1.0, 1.0),
    (1.75, 1.75, 1.5),
    (2.5, 2.5, 2.0),
    (3.25, 3.25, 2.5),
)
"""Multipliers of tier 1's (deployed_frac, race_loss, max_order_notional) for each tier."""

EDGE_NOTIONAL_STEPS: tuple[tuple[float, float], ...] = ((200.0, 3.0), (100.0, 2.0), (60.0, 1.5))
"""(edge floor in mils per share or set, multiplier of the per-order cost cap): bigger edges may spend more of
the race's loss budget in one order. The per-race cap only grows for consensus edges (CONSENSUS_EDGE)."""

CONSENSUS_EDGE = 200.0
"""Edge (mils) at which a trade whose fair value is confirmed by both sources may use CONSENSUS_SCALE x the
race loss cap. At the open, pumped long shots traded 30-80c away from fair values both Polymarket and Kalshi
put near 1% or 99%, and the 1x cap turned the bot away 44 times in the first hour."""
CONSENSUS_SCALE = 3.0


def notional_multiplier(edge: float) -> float:
    return next((mult for floor, mult in EDGE_NOTIONAL_STEPS if edge >= floor), 1.0)


def race_cap_scale(fairs: Iterable[Fair | None], edge: float) -> float:
    """Share of the race loss cap an intent may use: the weakest leg's fair confidence, or CONSENSUS_SCALE
    when the edge is huge and every leg's fair value is confirmed by both sources."""
    fairs = list(fairs)
    confidence = min((fair_confidence(f) for f in fairs), default=0.25)
    consensus = all(f is not None and f.confirmed for f in fairs)
    return CONSENSUS_SCALE if edge >= CONSENSUS_EDGE and confidence >= 1.0 and consensus else confidence


def build_tiers(
    deployed_frac: float = 0.20,
    race_loss: float = 2_000.0,
    order_notional: float = 1_000.0,
    deployed: tuple[float, ...] | None = None,
) -> list[TierDef]:
    """Tier 1 limits scaled by TIER_SCALE; ``deployed`` (one share per tier) replaces the scaled deployed
    caps."""
    if deployed is not None and len(deployed) != len(TIER_SCALE):
        raise ValueError(f"need one deployed share per tier ({len(TIER_SCALE)}), got {deployed}")
    return [
        TierDef(i + 1, deployed[i] if deployed else deployed_frac * d, race_loss * r, order_notional * o)
        for i, (d, r, o) in enumerate(TIER_SCALE)
    ]


def describe_tiers(tiers: list[TierDef]) -> str:
    return " · ".join(
        f"tier {t.tier}: {t.deployed_frac:.0%} deployed, race loss {t.race_loss:,.0f}, "
        f"order {t.max_order_notional:,.0f}"
        for t in tiers
    )


TIERS = build_tiers()


def opening_qty(position: float, leg: Leg) -> float:
    """Shares of ``leg`` that add exposure; the rest nets against an opposite holding and pays out."""
    opposite = (position > 0 and leg.side is Side.NO) or (position < 0 and leg.side is Side.YES)
    return max(0.0, leg.quantity - abs(position)) if opposite else float(leg.quantity)


def race_scenarios(race: Race | None, intent: Intent) -> tuple[list[str], list[frozenset[str]]]:
    """(exchanges, scenarios); a scenario is the set of exchanges whose YES pays. Without race info
    each traded exchange is treated as its own market."""
    if race is None:
        exchanges = [leg.exchange_id for leg in intent.legs]
        exhaustive = False
    else:
        exchanges = [o.sig_exchange_id for o in race.outcomes]
        exhaustive = race.exhaustive
    scenarios = [frozenset({ex}) for ex in exchanges]
    if not exhaustive:
        scenarios.append(frozenset())
    return exchanges, scenarios


def leg_loss(leg: Leg, scenario: frozenset[str]) -> float:
    """What the leg loses (negative: wins) if ``scenario`` happens: cost minus payout."""
    wins = (leg.exchange_id in scenario) == (leg.side is Side.YES)
    return leg.quantity * leg.price / 1000 - (leg.quantity if wins else 0.0)


STRESS_MULT = 3.0
STRESS_FLOOR = 0.05
"""A scenario's loss counts against the race cap in proportion to its fair probability stressed 3x: a coin
flip counts in full, a 10% long shot winning counts 30%, a 1% one 5% (the floor: a mapping or label error
can make any result happen, so no scenario ever counts for nothing). On opening day the flat cap turned
away 47 snipes, 41k of edge, nearly all on 1-10% long shots both sources agreed on, while the same cap was
right for competitive races; the hard ceiling (``race_loss_frac`` of equity) bounds what the weighting
can unlock."""


def scenario_weight(probability: float | None) -> float:
    if probability is None:
        return 1.0
    return min(1.0, max(STRESS_FLOOR, STRESS_MULT * probability))


TAIL_LONGSHOT = 0.20
"""Fair value under which an outcome is a long shot for the wave cap (and over 1 - this, a favourite)."""
TAIL_FULL_WEIGHT = 0.10
"""A long shot this likely (or more) counts in full in the wave cap; a less likely one in proportion: a
systematic polling miss that flips the 15% races flips few of the 3% ones (TN, OK, NE, AR Senate are 20-40
point states), and counting both the same let 54k of 1-5% carry use up the cap on 2026-10-03."""


def tail_exposure(sign: int, side: Side, fair: float, exchange_sign: int) -> float:
    """Share (0-1) of a BUY of ``side`` on an outcome with this fair value and party that is lost in a wave
    for the party with ``sign``: NO on that party's long shot, or YES on the other party's favourite,
    weighted by how plausible the upset is (``TAIL_FULL_WEIGHT``). The per-race cap weights each upset by
    its own probability, but twenty long shots of one party are one bet on a polling miss, not twenty
    independent ones."""
    if exchange_sign == sign:
        upset = fair if side is Side.NO else 1.0
    else:
        upset = 1 - fair if side is Side.YES else 1.0
    if upset > TAIL_LONGSHOT:
        return 0.0
    return min(1.0, upset / TAIL_FULL_WEIGHT)


def scenario_probability(
    scenario: frozenset[str],
    exchanges: Sequence[str],
    fair_value: Mapping[str, float],
) -> float | None:
    """Fair probability of ``scenario`` (one outcome's YES paying, or none of them), None when a fair value
    is missing (the scenario then counts in full)."""
    if any(ex not in fair_value for ex in exchanges):
        return None
    if scenario:
        return sum(fair_value[ex] for ex in scenario)
    return max(0.0, 1.0 - sum(fair_value[ex] for ex in exchanges))


class RiskGate:
    def __init__(
        self,
        account: AccountState,
        *,
        halted: bool = False,
        paused_strategies: set[str] | None = None,
        manual_paused: set[str] | None = None,
        daily_loss_frac: float = 0.02,
        swing_loss_frac: float = 0.03,
        party_sign: dict[str, int] | None = None,
        max_orders_per_minute: int = 60,
        max_errors_per_minute: int = 10,
        books: SigBooks | None = None,
        max_book_age_s: float = 10.0,
        tiers: list[TierDef] | None = None,
        min_order_cost: float = 5.0,
        race_loss_frac: float = 0.12,
        tail_loss_frac: float = 0.5,
        carry_cap_frac: float = 1.0,
        band_of: Callable[[str], float | None] | None = None,
    ) -> None:
        """``swing_loss_frac``: max loss (share of equity) from a SWING_SHIFT national move.
        ``party_sign``: exchange -> +1 Dem / -1 Rep / 0 other (unknown exchanges count as +1).
        ``min_order_cost``: orders costing less (SUSQies) are dust: each one spends a request slot and
        seconds of SIG latency for nothing (1-share snipes and 5-share exits at the open).
        ``race_loss_frac``: hard ceiling on the plain (unweighted) loss in any one result of a race, as a
        share of equity; what the probability weighting can unlock on long shots stops here.
        ``tail_loss_frac``: cap, as a share of equity, on what one party's wave would cost across every
        race: the collateral in NO on its long shots plus YES on the other party's favourites
        (``tail_exposure``). The per-race cap and the swing cap (a local delta, tiny on 3% contracts) do
        not bound that sum.
        ``carry_cap_frac``: cap, as a share of equity, on capital in positions the venue will not pay cost
        for (``trapped_capital``), given ``band_of`` (exchange -> SIG's own YES level, see band.py). A buy
        at or above the band is carry: only settlement pays it back, SIG marks it under cost meanwhile,
        and on 2026-10-04 85% of the book sat in such holds while cash for the round trips was 1-10k."""
        self.account = account
        self.tiers = tiers or TIERS
        self.min_order_cost = min_order_cost
        self.race_loss_frac = race_loss_frac
        self.tail_loss_frac = tail_loss_frac
        self.carry_cap_frac = carry_cap_frac
        self.band_of = band_of
        self.halted = halted
        self.paused_strategies = paused_strategies or set()
        self.manual_paused = manual_paused or set()
        self.daily_loss_frac = daily_loss_frac
        self.swing_loss_frac = swing_loss_frac
        self.party_sign = party_sign or {}
        self.max_orders_per_minute = max_orders_per_minute
        self.max_errors_per_minute = max_errors_per_minute
        self._books = books
        self._max_book_age_s = max_book_age_s
        self._orders: deque[float] = deque()
        self._errors: deque[float] = deque()
        self.order_rate_limited = False
        self.error_limited = False
        self.tier = 1
        self._day_start_equity = 0.0  # Tiers sets the real value on the first tick of the day

    def tier_def(self) -> TierDef:
        return self.tiers[self.tier - 1]

    def set_tier(self, tier: int) -> None:
        self.tier = max(1, min(len(self.tiers), tier))

    def register_order(self) -> None:
        self._orders.append(now_mono())
        self.order_rate_limited = False

    def register_error(self) -> None:
        self._errors.append(now_mono())

    def errors_recent(self) -> int:
        self._prune()
        return len(self._errors)

    def _prune(self) -> None:
        cutoff = now_mono() - 60.0
        while self._orders and self._orders[0] <= cutoff:
            self._orders.popleft()
        while self._errors and self._errors[0] <= cutoff:
            self._errors.popleft()

    def _books_fresh(self, intent: Intent) -> bool:
        """Fail closed: every leg's SIG book must exist and have been updated recently."""
        return all(self.book_is_fresh(leg.exchange_id) for leg in intent.legs)

    def book_is_fresh(self, exchange_id: str) -> bool:
        if self._books is None:
            return True  # book-freshness not wired (unit tests); still enforced by the engine
        book = self._books.books.get(exchange_id)
        return book is not None and now_mono() - book.ts <= self._max_book_age_s

    def check_rate(self) -> None:
        self._prune()
        self.order_rate_limited = len(self._orders) >= self.max_orders_per_minute
        self.error_limited = len(self._errors) >= self.max_errors_per_minute

    def check(
        self,
        intent: Intent,
        fair_value: dict[str, float],
        equity: float,
        *,
        race: Race | None = None,
        confidence: float = 1.0,
        resting: bool = False,
    ) -> tuple[Intent | None, str]:
        """Return the (possibly resized) intent and a reason. reason "" = approved.

        ``race`` enables the cross-outcome worst-case check (without it each exchange stands alone);
        ``confidence`` (see ``fair_confidence``) scales the race loss cap. ``resting`` orders (sweep rungs)
        skip the deployed-capital cap: their collateral is budgeted by their manager, and a cap meant for
        filled positions would otherwise let only the cheapest far rungs through once snipes have used the
        room, distorting the ladder; the race-loss, order-size, swing and daily-loss rules still apply.
        """
        if self.halted:
            return None, "halted"
        if intent.strategy in self.paused_strategies:
            return None, f"{intent.strategy} paused"
        for leg in intent.legs:
            if leg.exchange_id in self.manual_paused:
                return None, f"{leg.exchange_id} manual paused"
            if not (is_sig_tick(leg.price) and leg.quantity > 0):
                return None, f"invalid leg {leg.exchange_id}@{leg.price}x{leg.quantity}"
        if intent.capital < self.min_order_cost:
            return None, "dust"
        if not self._books_fresh(intent):
            return None, "stale book"
        self.check_rate()
        if self.order_rate_limited:
            return None, "order rate limited"
        if self.error_limited:
            return None, "error breaker"

        td = self.tier_def()
        # per-order notional, larger for larger edges: net opposite holdings (a buy that closes pays out)
        cost = self._opening_cost(intent)
        cap = td.max_order_notional * notional_multiplier(intent.edge)
        if cost > cap and not intent.reduces_risk:
            intent = intent.with_quantity(int(intent.quantity * cap / cost))
            if intent.quantity <= 0:
                return None, "resized to zero"
            cost = self._opening_cost(intent)

        if not intent.reduces_risk:
            fitted = self._fit_race_loss(
                intent,
                race,
                td.race_loss * confidence,
                fair_value,
                self.race_loss_frac * equity,
            )
            if fitted is None:
                return None, "race loss cap"
            if fitted.quantity != intent.quantity:
                intent, cost = fitted, self._opening_cost(fitted)
            # daily loss: no new risk for the rest of the day; exits and arb closes still go through
            if equity < self._day_start_equity * (1 - self.daily_loss_frac):
                return None, "daily loss halt"

        # national swing delta (block new directional risk only)
        if not intent.reduces_risk and intent.strategy != "arb" and fair_value:
            fitted = self._fit_swing(intent, fair_value, self.swing_cap(equity))
            if fitted is None:
                return None, "swing cap reached"
            intent = fitted
            fitted = self._fit_tail(intent, fair_value, self.tail_loss_frac * equity)
            if fitted is None:
                return None, "wave cap reached"
            intent = fitted
        if not intent.reduces_risk and self.carry_cap_frac < 1.0:
            fitted = self._fit_carry(intent, self.carry_cap_frac * equity)
            if fitted is None:
                return None, "carry cap reached"
            intent = fitted
        # deployed capital, last: its rejection opens the swap valve (recycle.TRIGGER), which sells holds to
        # make room, so it must mean "every other rule passed". Checked first, it let the valve sell carry
        # for snipes the swing or wave cap then turned away (2026-10-04 04:15, SEN-TN D NO sold for SEN-DE).
        if not intent.reduces_risk and not resting:
            cost = self._opening_cost(intent)
            if self.account.deployed + cost > td.deployed_frac * equity:
                return None, "deployed cap reached"
        return intent, ""

    def swing_cap(self, equity: float) -> float:
        return self.swing_loss_frac * equity / SWING_SHIFT

    def _fit_swing(self, intent: Intent, fair_value: dict[str, float], cap: float) -> Intent | None:
        """Largest share of ``intent`` keeping |national delta| within ``cap``; if it is already over,
        it may only shrink."""
        before = self._swing_delta(fair_value)
        added = 0.0
        for leg in intent.legs:
            fv = fair_value.get(leg.exchange_id)
            if fv is not None:
                signed = leg.quantity if leg.side is Side.YES else -leg.quantity
                added += self._sign(leg.exchange_id) * self._swing_weight(fv) * signed
        limit = max(cap, abs(before))
        if abs(before + added) <= limit:
            return intent
        scale = ((limit - before) if added > 0 else (limit + before)) / abs(added)
        quantity = int(intent.quantity * max(0.0, scale))
        return intent.with_quantity(quantity) if quantity >= 1 else None

    def _sign(self, exchange_id: str) -> int:
        return self.party_sign.get(exchange_id, 1)

    def tail_loss(self, fair_value: Mapping[str, float], sign: int) -> float:
        """What a wave for the party with ``sign`` costs on current holdings (see ``tail_exposure``)."""
        loss = 0.0
        for ex, h in self.account.holdings.items():
            fv = fair_value.get(ex)
            if fv is None or h.qty == 0:
                continue
            side = Side.YES if h.qty > 0 else Side.NO
            loss += h.cost_basis * tail_exposure(sign, side, fv, self._sign(ex))
        return loss

    def _fit_tail(self, intent: Intent, fair_value: Mapping[str, float], cap: float) -> Intent | None:
        """Largest share of ``intent`` keeping either party's wave loss within ``cap``; a wave already over
        the cap may not get worse."""
        scale = 1.0
        for sign in (1, -1):
            added = 0.0
            for leg in intent.legs:
                fv = fair_value.get(leg.exchange_id)
                if fv is None:
                    continue
                weight = tail_exposure(sign, leg.side, fv, self._sign(leg.exchange_id))
                added += weight * opening_qty(self.account.position(leg.exchange_id), leg) * leg.price / 1000
            if added <= 0:
                continue
            base = self.tail_loss(fair_value, sign)
            if base + added > max(cap, base):
                scale = min(scale, max(0.0, cap - base) / added)
        if scale >= 1.0:
            return intent
        quantity = int(intent.quantity * scale)
        return intent.with_quantity(quantity) if quantity >= 1 else None

    def _opening_cost(self, intent: Intent) -> float:
        return sum(
            opening_qty(self.account.position(leg.exchange_id), leg) * leg.price / 1000 for leg in intent.legs
        )

    def trapped(self, exchange_id: str, yes: bool, price: float) -> bool:
        """Whether a share of this side bought at ``price`` (its own terms, mils) is carry: the venue's band
        (unknown counts as no band) does not pay ``CARRY_FLIP_MARGIN`` over it, so only settlement will."""
        band = self.band_of(exchange_id) if self.band_of is not None else None
        if band is None:
            return True
        side_band = band if yes else 1000 - band
        return side_band - CARRY_FLIP_MARGIN < price

    def trapped_capital(self) -> float:
        """Cost basis of the holdings the venue will not pay cost for right now."""
        return sum(
            h.cost_basis
            for h in self.account.holdings.values()
            if h.qty != 0 and self.trapped(h.exchange_id, h.qty > 0, h.avg_cost * 1000)
        )

    def _fit_carry(self, intent: Intent, cap: float) -> Intent | None:
        """Largest share of ``intent`` keeping trapped capital within ``cap``: legs that buy under the band
        (round-trip material) are free, legs at or above it count in full; carry already over the cap may
        not grow."""
        added = sum(
            opening_qty(self.account.position(leg.exchange_id), leg) * leg.price / 1000
            for leg in intent.legs
            if self.trapped(leg.exchange_id, leg.side is Side.YES, leg.price)
        )
        if added <= 0:
            return intent
        base = self.trapped_capital()
        if base + added <= max(cap, base):
            return intent
        quantity = int(intent.quantity * max(0.0, cap - base) / added)
        return intent.with_quantity(quantity) if quantity >= 1 else None

    def _holding_losses(self, exchanges: Sequence[str], scenarios: list[frozenset[str]]) -> list[float]:
        losses = []
        for scenario in scenarios:
            loss = 0.0
            for ex in exchanges:
                h = self.account.holdings.get(ex)
                if h is not None:
                    loss += h.cost_basis - (max(h.qty, 0.0) if ex in scenario else max(-h.qty, 0.0))
            losses.append(loss)
        return losses

    def race_worst_loss(self, race: Race) -> float:
        """The most any one result of ``race`` costs on current holdings (plain, unweighted): what resting
        orders that would add to it if they filled are budgeted against (``race_loss_frac`` of equity)."""
        exchanges = [o.sig_exchange_id for o in race.outcomes]
        scenarios = [frozenset({ex}) for ex in exchanges]
        if not race.exhaustive:
            scenarios.append(frozenset())
        return max(0.0, *self._holding_losses(exchanges, scenarios))

    def _fit_race_loss(
        self,
        intent: Intent,
        race: Race | None,
        cap: float,
        fair_value: Mapping[str, float] = {},
        hard: float = float("inf"),
    ) -> Intent | None:
        """Largest share of ``intent`` keeping every scenario's weighted loss within ``cap`` (see
        ``scenario_weight``) and its plain loss within ``hard``. A scenario that is already over its limit
        (e.g. after a tier demotion) may not get worse; hedges always fit."""
        exchanges, scenarios = race_scenarios(race, intent)
        scale = 1.0
        for base, scenario in zip(self._holding_losses(exchanges, scenarios), scenarios, strict=True):
            added = sum(leg_loss(leg, scenario) for leg in intent.legs)
            if added <= 0:
                continue
            weight = scenario_weight(scenario_probability(scenario, exchanges, fair_value))
            limit = min(cap / weight, hard)
            if base + added > max(limit, base):
                scale = min(scale, max(0.0, limit - base) / added)
        if scale >= 1.0:
            return intent
        quantity = int(intent.quantity * scale)
        return intent.with_quantity(quantity) if quantity >= 1 else None

    def _swing_delta(self, fair_value: dict[str, float]) -> float:
        """P&L per unit logit of a uniform pro-Dem swing (positive: we gain if Democrats outperform)."""
        total = 0.0
        for ex, holding in self.account.holdings.items():
            fv = fair_value.get(ex)
            if fv is not None:
                total += self._sign(ex) * self._swing_weight(fv) * holding.qty
        return total

    @staticmethod
    def _swing_weight(fv: float) -> float:
        p = max(0.001, min(0.999, fv))
        return p * (1 - p)

    def set_day_start(self, equity: float) -> None:
        self._day_start_equity = equity


class Allocator:
    """Ranks intents by profit per capital and caps each strategy's share of the deployed-capital limit.

    ``shares``: strategy -> fraction of the tier's deployed cap it may use (no entry: no extra cap).
    A position counts toward the strategy whose fill opened it; exits always pass.
    """

    def __init__(self, shares: dict[str, float] | None = None) -> None:
        self.shares = shares or {}
        self.owner: dict[str, str] = {}
        self.deployed_by_strategy: dict[str, float] = {}

    def on_fill(self, strategy: str, opened: Iterable[str]) -> None:
        for exchange_id in opened:
            self.owner[exchange_id] = strategy

    def refresh(self, holdings: Mapping[str, Holding]) -> None:
        deployed: dict[str, float] = {}
        for exchange_id, holding in holdings.items():
            owner = self.owner.get(exchange_id)
            if owner is not None:
                deployed[owner] = deployed.get(owner, 0.0) + holding.cost_basis
        self.deployed_by_strategy = deployed

    def allocate(self, intents: list[Intent], deployed_cap: float) -> list[Intent]:
        ordered = sorted(intents, key=lambda i: -(i.expected_profit / max(1e-9, i.capital)))
        used = dict(self.deployed_by_strategy)
        out: list[Intent] = []
        for intent in ordered:
            share = self.shares.get(intent.strategy)
            if not intent.reduces_risk and share is not None:
                room = share * deployed_cap - used.get(intent.strategy, 0.0)
                if room < intent.capital:
                    quantity = int(intent.quantity * max(0.0, room) / intent.capital)
                    if quantity < 1:
                        continue
                    intent = intent.with_quantity(quantity)
                used[intent.strategy] = used.get(intent.strategy, 0.0) + intent.capital
            out.append(intent)
        return out


@dataclass
class Tiers:
    """Promotes/demotes the capital tier and enforces the daily loss halt."""

    risk: RiskGate
    peak_equity: float = 0.0
    day_start_equity: float | None = None
    tier_start: float = field(default_factory=now_mono)
    tier_orders: int = 0
    day: str = ""
    drawdown_frac: float = 0.01
    """Step down one tier when equity falls this far below the day's peak."""
    auto_promote: bool = True
    """False after a manual tier choice: no automatic promotion (drawdown step-downs still apply)."""
    confirm_ticks: int = 3
    """Consecutive ticks (one a minute) below the drawdown line before stepping down. A peak likewise needs
    two ticks. On 2026-10-01 SIG's balance lagged its own positions for one reconcile, equity showed +4k
    for a single tick, and the next two ticks demoted 3 -> 2 -> 1 against that phantom peak."""
    demote_cooldown_s: float = 300.0
    """At most one step-down per this long; the peak is re-based after a step-down, so the next one needs
    a further ``drawdown_frac`` of losses, not the same one counted twice."""
    _last_equity: float | None = field(default=None, init=False, repr=False)
    _below: int = field(default=0, init=False, repr=False)
    _last_demotion: float = field(default=float("-inf"), init=False, repr=False)

    def set_manual(self, tier: int) -> None:
        self.risk.set_tier(tier)
        self.auto_promote = False
        self.tier_start, self.tier_orders = now_mono(), 0

    def seed_day(self, day: str, equity: float) -> None:
        """The day's reference equity as recorded before a restart (``Recorder.day_start_equity``): the
        daily-loss halt measures the day, not the time since the last restart."""
        self.day = day
        self.day_start_equity = self.peak_equity = equity
        self._last_equity, self._below = equity, 0
        self.risk.set_day_start(equity)

    def on_trade(self) -> None:
        self.tier_orders += 1

    def tick(self, equity: float, day: str, *, fair_ready: bool = True) -> list[tuple[int, int, str]]:
        """``fair_ready``: every holding has a fair value, so ``equity`` is the real thing. The first tick
        after a restart runs before the feeds have spoken and values positions at SIG's marks (2026-10-04
        14:15: 113.4k instead of 121.7k), which must not become the day's reference."""
        events: list[tuple[int, int, str]] = []
        if self.day != day:
            if not fair_ready:
                return events  # not yet a day start worth measuring from
            self.day = day
            self.day_start_equity = equity
            self.peak_equity = equity
            self._last_equity, self._below = equity, 0
            self.risk.set_day_start(equity)
        assert self.day_start_equity is not None
        # a level counts as the peak only once it has held for two consecutive ticks
        held = min(equity, self._last_equity if self._last_equity is not None else equity)
        self.peak_equity = max(self.peak_equity, held)
        self._last_equity = equity
        self.risk.set_day_start(self.day_start_equity)

        # promote (proxy for the full ladder criteria: 30 min in tier, 30 trades,
        # +0.5% on the day, at/near peak equity)
        elapsed = now_mono() - self.tier_start
        if (
            self.auto_promote
            and self.risk.tier < len(self.risk.tiers)
            and elapsed >= 1800
            and self.tier_orders >= 30
            and equity > self.day_start_equity * 1.005
            and equity >= self.peak_equity - 0.005 * equity
        ):
            self.risk.set_tier(self.risk.tier + 1)
            self.tier_start, self.tier_orders = now_mono(), 0
            events.append((self.risk.tier - 1, self.risk.tier, "promote"))

        # demote on a drawdown from the peak that persists, at most one step per cooldown
        if self.risk.tier > 1 and equity < self.peak_equity * (1 - self.drawdown_frac):
            self._below += 1
            if (
                self._below >= self.confirm_ticks
                and now_mono() - self._last_demotion >= self.demote_cooldown_s
            ):
                self.risk.set_tier(self.risk.tier - 1)
                self.tier_start, self.tier_orders = now_mono(), 0
                self._last_demotion, self._below = now_mono(), 0
                self.peak_equity = equity  # the next step needs a further drawdown
                events.append((self.risk.tier + 1, self.risk.tier, "drawdown"))
        else:
            self._below = 0
        return events
