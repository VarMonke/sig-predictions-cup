from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

# NOTE: defaults live at module level because dataclasses(slots=True) turns class-attribute
# defaults into member descriptors, so `cls.default` is not a usable fallback.
DEFAULT_SIG_BASE_URL = "https://sig.thesuper.market/api/v1"
DEFAULT_TOURNAMENT_SLUG = "midterm-elections"
DEFAULT_LOG_DIR = "logs"
DEFAULT_MAPPING_PATH = "sig-api/mapping.json"
DEFAULT_CONTROL_HOST = "127.0.0.1"


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_bool(name: str, default: bool) -> bool:
    value = _env(name)
    return default if value is None else value.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    value = _env(name)
    return default if value is None else float(value)


def _env_int(name: str, default: int) -> int:
    value = _env(name)
    return default if value is None else int(value)


def _env_floats(name: str) -> tuple[float, ...] | None:
    value = _env(name)
    return None if value is None else tuple(float(v) for v in value.split(","))


def _env_time(name: str) -> float:
    """An ISO-8601 instant (UTC when no offset is given) as Unix seconds; 0 when unset."""
    value = _env(name)
    if value is None:
        return 0.0
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def build_dsn() -> str | None:
    """Postgres DSN from PG_* parts with the password URL-encoded (raw interpolation breaks on
    special characters: asyncpg then treats part of the password as the hostname)."""
    password = _env("PG_PASSWORD")
    if not password:
        return None
    host = _env("PG_HOST") or "db"
    port = _env("PG_PORT") or "5432"
    user = _env("PG_USER") or "sigbot"
    database = _env("PG_DATABASE") or "sigbot"
    return f"postgresql://{user}:{quote(password, safe='')}@{host}:{port}/{database}"


@dataclass(frozen=True, slots=True)
class RiskConfig:
    """Hard limits enforced by the risk gate. Money values are in SUSQies. The first three are tier 1;
    higher tiers scale them (see risk.TIER_SCALE)."""

    max_deployed_frac: float = 0.20
    max_race_loss: float = 2_000.0
    """Loss cap per race result, weighted by the result's stressed probability (a coin flip counts in full,
    a 1% long shot 5%), before scaling by fair-value confidence (x0.5 one source, x0.25 shaky)."""
    max_race_loss_frac: float = 0.12
    """Hard ceiling on the plain loss in any one result of a race, as a share of equity."""
    max_tail_loss_frac: float = 0.5
    """Cap on what one party's wave would cost across every race (NO on its long shots + YES on the other
    party's favourites, at cost), as a share of equity (RISK_MAX_TAIL_LOSS_FRAC)."""
    max_order_notional: float = 1_000.0
    max_swing_loss_frac: float = 0.03
    """Max loss (share of equity) if every race moves ~7c (a 0.3 logit national swing) against us."""
    daily_loss_frac: float = 0.02
    drawdown_step_down_frac: float = 0.01
    max_orders_per_minute: int = 60
    max_errors_per_minute: int = 10
    max_book_age_s: float = 10.0
    """Fail closed: never trade on a SIG book older than this (seconds)."""
    min_order_cost: float = 5.0
    """Orders costing less than this (SUSQies) are dust and not sent."""
    tier_deployed: tuple[float, ...] | None = None
    """Deployed-capital share per tier (1-4), e.g. (0.30, 0.35, 0.50, 0.65); None scales from tier 1."""
    paused_strategies: tuple[str, ...] = ()
    """Strategies paused from startup (``/pause`` lives in memory and a restart clears it)."""
    start_tier: int = 1
    """Tier to start in, as if set with ``/tier`` (automatic promotion off; drawdowns still step down). Every
    restart on 2026-10-02 came up in tier 1 (30% deployed, under what was already held) until ``/tier 4``."""
    carry_cap_frac: float = 1.0
    """Cap, as a share of equity, on capital in positions bought at or above SIG's own level, which only
    settlement pays back (RISK_CARRY_CAP_FRAC; 1 = off, 0 = never buy at or above the band). See band.py
    and ``RiskGate.trapped_capital``."""
    daily_loss_from: float = 0.0
    """Unix time: the daily-loss reference is the first recorded equity at or after this instead of the
    day's first (RISK_DAILY_LOSS_FROM, ISO-8601 UTC): a deliberate re-base after a loss whose cause has been
    removed. Only affects the day it falls in."""

    @classmethod
    def from_env(cls) -> RiskConfig:
        return cls(
            max_deployed_frac=_env_float("RISK_MAX_DEPLOYED_FRAC", 0.20),
            max_race_loss=_env_float("RISK_MAX_RACE_LOSS", 2_000.0),
            max_race_loss_frac=_env_float("RISK_MAX_RACE_LOSS_FRAC", 0.12),
            max_tail_loss_frac=_env_float("RISK_MAX_TAIL_LOSS_FRAC", 0.5),
            max_order_notional=_env_float("RISK_MAX_ORDER_NOTIONAL", 1_000.0),
            max_swing_loss_frac=_env_float("RISK_MAX_SWING_LOSS_FRAC", 0.03),
            daily_loss_frac=_env_float("RISK_DAILY_LOSS_FRAC", 0.02),
            drawdown_step_down_frac=_env_float("RISK_DRAWDOWN_STEP_DOWN_FRAC", 0.01),
            max_orders_per_minute=_env_int("RISK_MAX_ORDERS_PER_MINUTE", 60),
            max_errors_per_minute=_env_int("RISK_MAX_ERRORS_PER_MINUTE", 10),
            max_book_age_s=_env_float("RISK_MAX_BOOK_AGE_S", 10.0),
            min_order_cost=_env_float("RISK_MIN_ORDER_COST", 5.0),
            tier_deployed=_env_floats("RISK_TIER_DEPLOYED"),
            paused_strategies=tuple(
                s.strip() for s in (_env("PAUSED_STRATEGIES") or "").split(",") if s.strip()
            ),
            start_tier=max(1, _env_int("RISK_START_TIER", 1)),
            carry_cap_frac=min(1.0, max(0.0, _env_float("RISK_CARRY_CAP_FRAC", 1.0))),
            daily_loss_from=_env_time("RISK_DAILY_LOSS_FROM"),
        )


MAX_ACTIONS_PER_S = 0.25
"""Quoter writes per second at most: half of SIG's 30 writes/min."""


@dataclass(frozen=True, slots=True)
class QuoteConfig:
    """Flow trading (resting quotes inside SIG's spread). Off unless ``markets`` names something to quote."""

    markets: tuple[str, ...] = ()
    """Race keys (every outcome of the race), exchange ids, or ``all`` (every mapped exchange whose fair
    value is at least ``band`` from 0 and 1: collateral per quote is moderate there and the spread is
    worth earning)."""
    band: int = 150
    top_n: int = 0
    """With ``all``: quote only the ``top_n`` most active band markets (decayed notional of recent prints),
    after the named ones; 0 = every band market. Fills come from where the flow is, and SIG's 30 writes a
    minute go further on 25 busy markets with big clips than on 200 quiet ones (the leaders' engine is
    ~10k-notional clips at the inside of the busiest markets, 2026-10-02)."""
    half_spread: int = 30
    """Quote the anchor +/- this many mils (improving on the inside when the spread allows)."""
    min_half_spread: int = 5
    """Join the inside when it is at least this far from the anchor (a 1c-wide market is joined, a 1-tick
    one is not). A live quote is cancelled only once it sits at or through the anchor."""
    max_spread: int = 80
    """Anchor on SIG's own mid when the others' spread is at most this wide (mils); else on fair value. SIG's
    lopsided books clear 5-12c from Polymarket/Kalshi fair for days (long shots rich): quoting around fair
    there fills one side at once and the other never, while the venue's flow trades around SIG's mid."""
    fair_guard: int = 50
    """Per side: a quote that would buy a side more than this many mils above what it is worth (fair for
    YES, 1 - fair for NO) is placed only to close a position held the other way (sized to it). The side
    buying below worth is quoted in full, however far the venue sits from fair."""
    rich_frac: float = 0.5
    """Size multiplier for a quote that buys a side above its fair value by at most ``fair_guard`` (the
    venue's price disagrees with fair that way): we lean toward fair, with the full size on the other side."""
    size: int = 100
    max_inventory: int = 500
    """Shares per exchange in either direction; at the cap only the side that reduces it is quoted."""
    requote_move: int = 10
    """Place a fresh quote once the wanted price has moved this many mils from the live one. The live one is
    left to expire unless it sits at or through the anchor (then it is cancelled)."""
    expiry_s: float = 60.0
    """SIG cancels every quote by itself after this long (dead-man's switch)."""
    refresh_s: float = 45.0
    """Replace a quote this old, before it expires."""
    actions_per_s: float = 0.25
    """Batch placements + cancels per second. SIG allows 30 writes a minute per key (a batch is one write), so
    the quoter is held to at most ``MAX_ACTIONS_PER_S`` (15/min) to leave the snipes, arbs, exits and the
    ladder their share; a full re-quote of 200 quotes is 8 batches."""
    batch_size: int = 25
    """Quotes per /orders/batch request (SIG allows 50)."""
    budget_frac: float = 0.15
    """Collateral resting in quotes at most, as a share of equity (a resting quote reserves its cost)."""
    cash_multiple: float = 1.0
    """Resting quotes may reserve up to this multiple of cash (SIG does not lock collateral at placement: it
    resizes an order to the balance on admission, see api spec ``remainingQuantity``; ~900 rungs and 300
    quotes worth several times our cash rested without one balance rejection on 2026-10-02). 1 = never more
    than the cash; raise once fill-time behaviour beyond cash is understood."""
    inventory_frac: float = 0.15
    """Positions on quoted markets at most, as a share of equity (cost basis); above it only the side that
    reduces a market's position is quoted. Quotes skip the deployed-capital cap (it is for directional
    positions and would switch market making off whenever the book is full), so this is their own limit."""

    @classmethod
    def from_env(cls) -> QuoteConfig:
        markets = _env("MM_MARKETS") or ""
        expiry_s = _env_float("MM_EXPIRY_S", 60.0)
        return cls(
            markets=tuple(m.strip() for m in markets.split(",") if m.strip()),
            band=round(_env_float("MM_BAND_C", 15.0) * 10),
            top_n=max(0, _env_int("MM_TOP_N", 0)),
            half_spread=round(_env_float("MM_HALF_SPREAD_C", 3.0) * 10),
            min_half_spread=max(5, round(_env_float("MM_MIN_HALF_SPREAD_C", 0.5) * 10)),
            max_spread=round(_env_float("MM_MAX_SPREAD_C", 8.0) * 10),
            fair_guard=round(_env_float("MM_FAIR_GUARD_C", 5.0) * 10),
            rich_frac=_env_float("MM_RICH_SIDE_FRAC", 0.5),
            size=_env_int("MM_SIZE", 100),
            max_inventory=_env_int("MM_MAX_INVENTORY", 500),
            expiry_s=expiry_s,
            refresh_s=max(15.0, expiry_s - 15.0),
            actions_per_s=min(MAX_ACTIONS_PER_S, _env_float("MM_ACTIONS_PER_S", 0.25)),
            batch_size=max(1, min(50, _env_int("MM_BATCH_SIZE", 25))),
            budget_frac=_env_float("MM_BUDGET_FRAC", 0.15),
            cash_multiple=_env_float("MM_CASH_MULTIPLE", 1.0),
            inventory_frac=_env_float("MM_INVENTORY_FRAC", 0.15),
        )


@dataclass(frozen=True, slots=True)
class SweepConfig:
    """Sweep catching (resting limits far from fair, first in line inside the crowd's deep levels). Off unless
    ``markets`` names something. See sweep.py."""

    markets: tuple[str, ...] = ()
    """Race keys, exchange ids, or ``all`` (every exchange whose fair value is within ``band`` of 0 or 1)."""
    band: int = 200
    """With ``all``: only exchanges with fair <= band or >= 1 - band (mils); named markets ignore the band."""
    floor: int = 300
    """Never rest closer to fair value than this (mils): below it is snipe/exit territory."""
    offsets: tuple[int, ...] = (300, 500, 700)
    """Secondary rungs at these distances from fair (mils), for partial sweeps; only inside the primary."""
    size: int = 10_000
    """Shares on the primary rung (one tick inside the crowd's first wall)."""
    secondary_size: int = 2_000
    """Shares on each secondary rung."""
    wall_qty: int = 20_000
    """A resting level this big is a wall of the crowd, which the primary sits inside; smaller orders in
    the gap are competitors' rungs we sit behind (jumping inside each would walk the primary to the floor)."""
    level_cost: float = 1_200.0
    """Collateral per rung at most (SUSQies): quantity is cut to level_cost / price."""
    max_rung_cost: int = 200
    """Collateral per share at most (mils, in the bought side's terms): a NO rung rests at a YES price of at
    least 1 - this, a YES rung at most this. The sweep money lands at the extremes (0.985-0.995 prints were
    most of the value moved on 2026-10-01), and a rung at fair + 30c on a 1% contract costs 69c a share for
    the same catch; with the deployed cap binding, cheap rungs are the only ones worth the collateral."""
    budget_frac: float = 0.25
    cash_multiple: float = 1.0
    """Resting rungs may reserve up to this multiple of cash (see QuoteConfig.cash_multiple)."""
    """Collateral resting in rungs at most, as a share of equity (never cash the snipes may still need)."""
    relevel_move: int = 50
    """Rebuild an exchange's ladder once fair value has moved this far (mils) from where it was built."""
    requote_min_s: float = 20.0
    """Move a rung after the crowd at most this often per exchange (damps penny-jumping wars)."""
    expiry_s: float = 1_800.0
    """SIG cancels every rung by itself after this long (dead-man's switch); renewed before expiry."""
    refresh_s: float = 1_680.0
    batch_size: int = 10
    """Rungs per /orders/batch request (SIG allows 50; small batches keep clear of engine order limits)."""
    actions_per_s: float = 0.5
    """Batch placements + cancels per second, to stay inside SIG's request budget."""
    depth: int = 60
    """Book levels per side to fetch for exchanges in scope (the crowd sits behind the near-fair layer)."""
    depth_per_s: float = 0.34
    """Depth fetches per second for books whose sizes are unknown (about 20 requests a minute)."""
    roc_levels: tuple[float, ...] = ()
    """Dump catching, on when set (it replaces the far-from-fair geometry above): BUY the favourite's side of
    lopsided markets at the prices that return these shares of their cost to settlement at fair, first level
    first (0.08 puts a 95c favourite's bid at 87.5c, and a 5% long shot's NO bid at 87.5c, a YES ask at
    12.5c). See ``sweep.value_rungs``."""
    roc_sizes: tuple[int, ...] = ()
    """Shares per ``roc_levels`` entry (the last one repeats); empty: ``size``, then ``secondary_size``."""
    far: bool = False
    """With ``roc_levels``: also place the far-from-fair geometry (slots ``sweep.FAR_SLOT`` and up) beside the
    value rungs (SWEEP_FAR). On 2026-10-03 spikes of 500k+ shares reached 25-62c on 2c long shots several
    times a day and reverted within the hour; the value rungs at the band never see them."""
    far_frac: float = 0.5
    """Share of the ladder's collateral budget the far rungs may use when both geometries run
    (SWEEP_FAR_FRAC); the value rungs get the rest."""
    behind_walls: bool = False
    """Place far secondaries even behind the crowd's walls (SWEEP_BEHIND_WALLS). Off: a secondary behind a
    wall is skipped (right while the zone beyond the band was empty); on: the book is dense on every tick and
    a spike eats 20 of them, so a rung behind a wall is reached too."""
    dump_offsets: tuple[int, ...] = ()
    """Dump catching priced off SIG's own level instead of fair (SWEEP_DUMP_OFFSETS_C, mils): one rung per
    entry that far under the band on the favourite's side (a YES bid on a favourite; on a long shot a NO bid,
    i.e. a YES ask that far above the band). Whole-bankroll dumps knock favourites 12-20c under the band and
    the bid is back within minutes (band.py); rungs priced off fair sat above the band on the favourites SIG
    discounts for days (SEN-DE D at 87.8c, fair 98.9c) and bought carry. Needs a band (see band.py)."""
    dump_min_edge: int = 20
    """A band-relative rung still rests at least this far (mils) on the right side of fair value: a venue
    level above fair is not a reason to buy above fair (SWEEP_DUMP_MIN_EDGE_C)."""
    flip: bool = True
    """Rest a sell of every position on a ladder market at the better of the band less ``flip_margin`` and
    cost plus ``flip_min_profit`` (SWEEP_FLIP): the round trip for dump fills, and a no-loss exit for carry
    whenever the venue dips to it. Marketable when the inside already pays it."""
    flip_margin: int = 10
    """Mils under the band a flip sells at (SWEEP_FLIP_MARGIN_C): the first bids back after a dump."""
    flip_min_profit: int = 10
    """Mils over average cost a flip must book (SWEEP_FLIP_MIN_PROFIT_C): a tick is not a trade, and
    a band that fell to our cost means the dump was a repricing, so the position waits for a better one."""
    longshot_offsets: tuple[int, ...] = ()
    """The carry bucket (SWEEP_LONGSHOT_OFFSETS_C, mils): on long shots worth at most ``carry_max_fair``
    the YES asks rest these many cents above the band instead of ``dump_offsets`` (e.g. 1,3,6: the first one
    is lifted by the crowd's ordinary long-shot buying, not only by spikes). A NO bought that way on a 1%
    contract pays ~0.99 at settlement; with ``hold_frac`` it is kept rather than flipped. Empty: the dump
    offsets apply to both sides."""
    carry_max_fair: int = 30
    """Long shots worth at most this (mils) get ``longshot_offsets`` (SWEEP_CARRY_MAX_FAIR_C): true long
    shots only, where a wave cannot make the YES pay."""
    hold_frac: float = 0.0
    """Hold instead of flip (SWEEP_HOLD_FRAC): while the capital in positions is at most this share of
    equity, fills on sides worth at least ``sweep.HOLD_SIDE_FAIR`` are kept to settlement (a favourite caught
    3c under the band is worth 15c more at settlement than at the flip; the capital was idle anyway).
    Above it the flips resume and recycle. 0 = always flip."""

    @classmethod
    def from_env(cls) -> SweepConfig:
        markets = _env("SWEEP_MARKETS") or ""
        expiry_s = _env_float("SWEEP_EXPIRY_S", 1_800.0)
        offsets = _env("SWEEP_SECONDARY_C")
        sizes = _env_floats("SWEEP_ROC_SIZES") or ()
        dump_offsets = _env_floats("SWEEP_DUMP_OFFSETS_C") or ()
        longshot_offsets = _env_floats("SWEEP_LONGSHOT_OFFSETS_C") or ()
        return cls(
            markets=tuple(m.strip() for m in markets.split(",") if m.strip()),
            band=round(_env_float("SWEEP_BAND_C", 20.0) * 10),
            floor=round(_env_float("SWEEP_FLOOR_C", 30.0) * 10),
            offsets=tuple(round(float(c) * 10) for c in offsets.split(",") if c.strip())
            if offsets is not None
            else (300, 500, 700),
            size=_env_int("SWEEP_SIZE", 10_000),
            secondary_size=_env_int("SWEEP_SECONDARY_SIZE", 2_000),
            wall_qty=_env_int("SWEEP_WALL", 20_000),
            level_cost=_env_float("SWEEP_LEVEL_COST", 1_200.0),
            max_rung_cost=round(_env_float("SWEEP_MAX_RUNG_COST_C", 20.0) * 10),
            budget_frac=_env_float("SWEEP_BUDGET_FRAC", 0.25),
            cash_multiple=_env_float("SWEEP_CASH_MULTIPLE", 1.0),
            relevel_move=round(_env_float("SWEEP_RELEVEL_C", 5.0) * 10),
            requote_min_s=_env_float("SWEEP_REQUOTE_MIN_S", 20.0),
            expiry_s=expiry_s,
            refresh_s=max(60.0, expiry_s - 120.0),
            batch_size=max(1, min(50, _env_int("SWEEP_BATCH_SIZE", 10))),
            actions_per_s=_env_float("SWEEP_ACTIONS_PER_S", 0.5),
            depth=max(20, min(200, _env_int("SWEEP_DEPTH", 60))),
            depth_per_s=_env_float("SWEEP_DEPTH_PER_S", 0.34),
            roc_levels=tuple(r for r in _env_floats("SWEEP_ROC_LEVELS") or () if r > 0),
            roc_sizes=tuple(int(s) for s in sizes),
            far=_env_bool("SWEEP_FAR", False),
            far_frac=min(1.0, max(0.0, _env_float("SWEEP_FAR_FRAC", 0.5))),
            behind_walls=_env_bool("SWEEP_BEHIND_WALLS", False),
            dump_offsets=tuple(round(c * 10) for c in dump_offsets if c > 0),
            dump_min_edge=round(_env_float("SWEEP_DUMP_MIN_EDGE_C", 2.0) * 10),
            flip=_env_bool("SWEEP_FLIP", True),
            flip_margin=round(_env_float("SWEEP_FLIP_MARGIN_C", 1.0) * 10),
            flip_min_profit=round(_env_float("SWEEP_FLIP_MIN_PROFIT_C", 1.0) * 10),
            longshot_offsets=tuple(round(c * 10) for c in longshot_offsets if c > 0),
            carry_max_fair=round(_env_float("SWEEP_CARRY_MAX_FAIR_C", 3.0) * 10),
            hold_frac=min(1.0, max(0.0, _env_float("SWEEP_HOLD_FRAC", 0.0))),
        )


@dataclass(frozen=True, slots=True)
class RecycleConfig:
    """Capital recycling under pressure (see recycle.py): when the deployed cap rejects a trade, sell just
    enough of the slowest position, at or above cost, to let it through."""

    enabled: bool = True
    margin: float = 0.05
    """The rejected trade's return on capital must beat the position's forgone rate by this much."""
    min_free: float = 50.0
    """Smallest swap worth an order (SUSQies freed)."""
    cooldown_s: float = 30.0
    """No second swap out of the same exchange within this long."""
    max_per_minute: int = 6
    reserve_frac: float = 0.0
    """Keep at least this share of equity in cash (0 = off): below it, sell the positions with the least edge
    left per SUSQie they tie up, cheapest first, until the cash is back. Resting dump-catcher rungs only pay
    when there is cash to fill them (2026-10-03: the leaders' biggest gains came from 2-3M-share dumps of
    favourites 4-15% under fair; we had 1.5k of cash)."""
    reserve_max_rate: float = 0.10
    """The reserve never sells a position still earning more than this to settlement (forgone rate)."""
    reserve_max_loss: float = 0.0
    """Mils under cost the reserve may sell at (0 = never a loss)."""
    reserve_every_s: float = 10.0
    turnover_days: float = 0.0
    """Days a new position is expected to take to be sold again (RECYCLE_TURNOVER_DAYS; 0 = compare returns
    to settlement as before). A swap then weighs the trade's return once per turnover it can make before
    settlement against the position's single return to settlement (see ``plan``)."""
    swap_max_loss: float = 0.0
    """Mils under cost a swap may sell at (RECYCLE_SWAP_MAX_LOSS_C; 0 = never a loss). A market's booked gains
    cap what a losing sale may give back (``candidates``)."""

    @classmethod
    def from_env(cls) -> RecycleConfig:
        return cls(
            enabled=_env_bool("RECYCLE_ENABLED", True),
            margin=_env_float("RECYCLE_MARGIN", 0.05),
            min_free=_env_float("RECYCLE_MIN_FREE", 50.0),
            cooldown_s=_env_float("RECYCLE_COOLDOWN_S", 30.0),
            max_per_minute=_env_int("RECYCLE_MAX_PER_MINUTE", 6),
            reserve_frac=min(0.9, max(0.0, _env_float("RECYCLE_RESERVE_FRAC", 0.0))),
            reserve_max_rate=max(0.0, _env_float("RECYCLE_RESERVE_MAX_RATE", 0.10)),
            reserve_max_loss=max(0.0, _env_float("RECYCLE_RESERVE_MAX_LOSS_C", 0.0)) * 10,
            reserve_every_s=max(1.0, _env_float("RECYCLE_RESERVE_EVERY_S", 10.0)),
            turnover_days=max(0.0, _env_float("RECYCLE_TURNOVER_DAYS", 0.0)),
            swap_max_loss=max(0.0, _env_float("RECYCLE_SWAP_MAX_LOSS_C", 0.0)) * 10,
        )


@dataclass(frozen=True, slots=True)
class Settings:
    api_key: str
    sig_base_url: str = DEFAULT_SIG_BASE_URL
    tournament_slug: str = DEFAULT_TOURNAMENT_SLUG
    trading_enabled: bool = False
    sig_max_rps: float = 8.0
    sig_budget_per_min: int = 90
    """Requests per minute this process may send SIG (the key allows ~100 and locks out for 60 s above
    that). Lower it for a second, read-only instance on the same key (a shadow run beside the live bot)."""
    sig_scan_every_s: float = 5.0
    """Full poll of every SIG book; pushed books keep busy ones fresh, so this mainly serves quiet ones."""
    kalshi_enabled: bool = True
    kalshi_max_rps: float = 12.0
    snipe_kelly_fraction: float = 0.25
    exit_recycle_lopsided: bool = False
    """Sell lopsided positions (held to settlement otherwise) whenever the market pays at least cost +
    ``exit_recycle_margin``: frees settlement-locked capital for the quoter (EXIT_RECYCLE_LOPSIDED)."""
    exit_recycle_margin: float = 0.0
    """Mils above cost a lopsided recycle exit must realize (EXIT_RECYCLE_MARGIN_C, cents)."""
    """Fraction of the Kelly stake a snipe may take (0.35 agreed 2026-10-01; the risk gate's order and
    race caps usually bind first)."""
    exit_hurdle: float = 0.0
    """Sell any share whose remaining return to settlement is at most this share of the cash its sale frees
    (EXIT_HURDLE, e.g. 0.05; 0 = off). New snipes and arbs must then return at least the hurdle plus
    ``exits.HURDLE_ENTRY_MARGIN`` on their capital."""
    exit_hurdle_max_loss: float = 0.0
    """Mils below cost a hurdle exit may sell at (EXIT_HURDLE_MAX_LOSS_C, cents)."""
    exit_hurdle_per_day: float = 0.0
    """Sell any hold whose remaining return to settlement per day of trading left is at most this
    (EXIT_HURDLE_PER_DAY, e.g. 0.01; 0 = off)."""
    snipe_min_roc: float = 0.08
    """Edge per unit of capital a snipe needs at least (SNIPE_MIN_ROC); raised to the hurdle's entry floor."""
    arb_min_roc: float = 0.0
    """Profit per set over its cost an arb needs (ARB_MIN_ROC); raised to the hurdle's entry floor."""
    exit_unwind_exchanges: tuple[str, ...] = ()
    """Positions to close at the inside even under cost once ``exit_unwind_after`` has passed
    (EXIT_UNWIND_EXCHANGES: exchange ids, or ``carry`` for every position the venue's band does not pay
    cost for). A deliberate, user-approved liquidation; the flips sell at cost or better until then."""
    exit_unwind_after: float = 0.0
    """Unix time the unwind may start taking losses (EXIT_UNWIND_AFTER, ISO-8601 UTC; empty = at once)."""
    exit_unwind_max_loss: float = 0.0
    """Mils under cost an unwind exit may sell at (EXIT_UNWIND_MAX_LOSS_C, cents)."""
    database_url: str | None = None
    log_dir: Path = Path(DEFAULT_LOG_DIR)
    log_level: str = "INFO"
    mapping_path: Path = Path(DEFAULT_MAPPING_PATH)
    control_host: str = DEFAULT_CONTROL_HOST
    control_port: int = 8081
    discord_bot_token: str | None = None
    discord_alert_channel_id: str | None = None
    discord_activity_channel_id: str | None = None
    discord_admin_user_id: str | None = None
    risk: RiskConfig = field(default_factory=RiskConfig)
    quotes: QuoteConfig = field(default_factory=QuoteConfig)
    sweep: SweepConfig = field(default_factory=SweepConfig)
    recycle: RecycleConfig = field(default_factory=RecycleConfig)

    @classmethod
    def from_env(cls) -> Settings:
        api_key = _env("API_KEY")
        if not api_key:
            raise RuntimeError("API_KEY is not set (load it from .env)")
        return cls(
            api_key=api_key,
            sig_base_url=_env("SIG_BASE_URL") or DEFAULT_SIG_BASE_URL,
            tournament_slug=_env("TOURNAMENT_SLUG") or DEFAULT_TOURNAMENT_SLUG,
            trading_enabled=_env_bool("TRADING_ENABLED", False),
            sig_max_rps=_env_float("SIG_MAX_RPS", 8.0),
            sig_budget_per_min=max(10, _env_int("SIG_BUDGET_PER_MIN", 90)),
            sig_scan_every_s=_env_float("SIG_SCAN_EVERY_S", 5.0),
            kalshi_enabled=_env_bool("KALSHI_ENABLED", True),
            kalshi_max_rps=_env_float("KALSHI_MAX_RPS", 12.0),
            snipe_kelly_fraction=min(1.0, max(0.0, _env_float("SNIPE_KELLY_FRACTION", 0.25))),
            exit_recycle_lopsided=_env_bool("EXIT_RECYCLE_LOPSIDED", False),
            exit_recycle_margin=_env_float("EXIT_RECYCLE_MARGIN_C", 0.0) * 10,
            exit_hurdle=max(0.0, _env_float("EXIT_HURDLE", 0.0)),
            exit_hurdle_max_loss=max(0.0, _env_float("EXIT_HURDLE_MAX_LOSS_C", 0.0)) * 10,
            exit_hurdle_per_day=max(0.0, _env_float("EXIT_HURDLE_PER_DAY", 0.0)),
            snipe_min_roc=max(0.0, _env_float("SNIPE_MIN_ROC", 0.08)),
            arb_min_roc=max(0.0, _env_float("ARB_MIN_ROC", 0.0)),
            exit_unwind_exchanges=tuple(
                s.strip() for s in (_env("EXIT_UNWIND_EXCHANGES") or "").split(",") if s.strip()
            ),
            exit_unwind_after=_env_time("EXIT_UNWIND_AFTER"),
            exit_unwind_max_loss=max(0.0, _env_float("EXIT_UNWIND_MAX_LOSS_C", 0.0)) * 10,
            database_url=_env("DATABASE_URL") or build_dsn(),
            log_dir=Path(_env("LOG_DIR") or DEFAULT_LOG_DIR),
            log_level=(_env("LOG_LEVEL") or "INFO").upper(),
            mapping_path=Path(_env("MAPPING_PATH") or DEFAULT_MAPPING_PATH),
            control_host=_env("CONTROL_HOST") or DEFAULT_CONTROL_HOST,
            control_port=_env_int("CONTROL_PORT", 8081),
            discord_bot_token=_env("DISCORD_BOT_TOKEN"),
            discord_alert_channel_id=_env("DISCORD_ALERT_CHANNEL_ID"),
            discord_activity_channel_id=_env("DISCORD_ACTIVITY_CHANNEL_ID"),
            discord_admin_user_id=_env("DISCORD_ADMIN_USER_ID"),
            risk=RiskConfig.from_env(),
            quotes=QuoteConfig.from_env(),
            sweep=SweepConfig.from_env(),
            recycle=RecycleConfig.from_env(),
        )
