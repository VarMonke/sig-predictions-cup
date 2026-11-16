"""msgspec wire models for the SIG (Super Market) participant API.

Unknown JSON fields are ignored by msgspec, so these only declare what we use.
Prices on the wire are YES-normalized floats in [0, 1].
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

import msgspec


class Side(StrEnum):
    YES = "yes"
    NO = "no"


class Action(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderStatusFilter(StrEnum):
    OPEN = "open"
    CLOSED = "closed"
    EXPIRED = "expired"
    ALL = "all"


class Wire(msgspec.Struct, rename="camel", kw_only=True, gc=False):
    """Base for decoded responses (treated as read-only)."""


class Body(msgspec.Struct, rename="camel", kw_only=True, omit_defaults=True):
    """Base for encoded request bodies (None/default fields are omitted)."""


class ErrorInfo(Wire):
    code: str = "UNKNOWN"
    message: str = ""
    details: dict[str, Any] | None = None


class ErrorEnvelope(Wire):
    error: ErrorInfo


class Pagination(Wire):
    limit: int | None = None
    has_more: bool = False
    next_cursor: str | None = None
    total: int | None = None
    offset: int | None = None


class Coverage(Wire):
    complete: bool = True
    projected_through_sequence: int | None = None


class Account(Wire):
    id: str
    username: str | None = None
    created_at: str = ""
    balance: float | None = None


class Tournament(Wire):
    id: str
    slug: str
    name: str
    status: str
    description: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    initial_balance: float = 0.0
    currency_name: str = ""
    my_balance: float = 0.0
    member_count: int | None = None
    market_count: int | None = None
    joined_at: str | None = None
    is_pending_enrolment: bool = False


class TournamentList(Wire):
    data: list[Tournament]
    pagination: Pagination | None = None


class Exchange(Wire):
    id: str
    option: str | None = None
    latest_price: float | None = None
    initial_price: float | None = None


class TournamentRef(Wire):
    id: str
    slug: str
    name: str = ""
    currency_name: str = ""
    is_ongoing_play: bool = False


class ContextExchange(Wire):
    id: str
    latest_price: float | None = None


class MarketContext(Wire):
    type: str
    tournament: TournamentRef | None = None
    exchanges: list[ContextExchange] = []


class Market(Wire):
    id: str
    title: str
    status: str
    settlement_date: str | None = None
    settled_with: str | None = None
    settled_on: str | None = None
    categories: list[str] = []
    is_composite: bool = False
    is_multi_outcome: bool = False
    exchanges: list[Exchange] = []
    contexts: list[MarketContext] = []


class MarketPage(Wire):
    data: list[Market]
    pagination: Pagination


class BookLevel(Wire):
    price: float
    quantity: float


class BookVersion(Wire):
    """``asOf``: the engine version a book was read at; compare by sequence, then time."""

    sequence: int | None = None
    at: str | None = None


class ExchangeOrderbook(Wire):
    exchange_id: str
    market_id: str
    bids: list[BookLevel] = []
    asks: list[BookLevel] = []
    depth: int = 0
    best_bid: float | None = None
    best_ask: float | None = None
    spread: float | None = None
    as_of: BookVersion | None = None


class CombinedBookExchange(Wire):
    exchange_id: str
    option: str | None = None
    implied_probability: float | None = None
    bids: list[BookLevel] = []
    asks: list[BookLevel] = []


class MarketOrderbook(Wire):
    exchanges: list[CombinedBookExchange] = []
    overround: float | None = None
    has_arbitrage_opportunity: bool = False


class ExchangePrice(Wire):
    exchange_id: str
    market_id: str
    option: str | None = None
    latest_price: float | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    spread: float | None = None


class BulkPrices(Wire):
    data: list[ExchangePrice]
    missing_ids: list[str] = []


class ExchangeTrade(Wire):
    id: str
    created_at: str
    size: int
    side: str
    price: float | None = None
    volume: float | None = None


class ExchangeTradePage(Wire):
    exchange_id: str
    market_id: str
    data: list[ExchangeTrade] = []
    pagination: Pagination | None = None


class Candle(Wire):
    time: str | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None


class PriceHistory(Wire):
    data: list[Candle] = []


class OrderRequest(Body):
    exchange_id: str
    side: Side
    action: Action
    quantity: int
    price: float | None = None
    expiration_date: str | None = None
    tournament_id: str | None = None


class SingleOrderRequest(OrderRequest):
    idempotency_key: str = ""


class BatchOrderRequest(Body):
    idempotency_key: str
    orders: list[OrderRequest]


class MultiLegRequest(Body):
    idempotency_key: str
    legs: list[OrderRequest]
    relationship_constraint: str | None = None


class OrderResult(Wire):
    exchange_id: str
    open: bool
    order_id: int | None = None
    remaining_quantity: float | None = None
    action: Action | None = None
    side: Side | None = None
    price: float | None = None
    quantity: int | None = None
    terminal_reason_code: str | None = None
    quantity_traded: float = 0.0
    total_cost: float = 0.0
    fill_price: float | None = None


class Order(Wire):
    id: int
    exchange_id: str
    side: Side
    action: Action
    quantity: float
    open: bool
    created_at: str
    price_limit: float | None = None
    expiration_date: str | None = None


class OrderPage(Wire):
    data: list[Order]
    pagination: Pagination


class CancelResult(Wire):
    order_id: int
    tournament_id: str | None = None
    message: str = ""


class CancelAllError(Wire):
    order_id: int
    error: str = ""


class CancelAllResult(Wire):
    cancelled: int = 0
    errors: list[CancelAllError] = []


class CancelAllBody(Body):
    exchange_id: str | None = None
    market_id: str | None = None
    tournament_id: str | None = None


class BatchItem(Wire):
    index: int
    ok: bool = False
    status: int = 0
    data: msgspec.Raw = msgspec.Raw()
    """The placed order or the item's error, decoded by the caller (``msgspec.Raw`` cannot be in a union:
    ``Raw | None`` only ever accepted null, and the first live batch on 2026-10-01 failed to decode)."""


class BatchItemError(Wire):
    """``data`` of a failed batch item: ``{code, error}`` for engine rate limits / unknown outcomes."""

    code: str = ""
    error: str = ""


class BatchResult(Wire):
    results: list[BatchItem]


class MultiLegItem(Wire):
    index: int
    data: OrderResult


class MultiLegResult(Wire):
    results: list[MultiLegItem]


class OrderFill(Wire):
    id: int
    quantity: float
    side: Side
    filled_at: str
    price: float | None = None


class OrderFills(Wire):
    order_id: int
    exchange_id: str
    data: list[OrderFill] = []
    total_quantity_filled: float = 0.0
    avg_fill_price: float | None = None


class PositionLot(Wire):
    lot_id: str
    side: str
    quantity: float
    entry_price: float
    opened_at: str


class Position(Wire):
    exchange_id: str
    market_id: str
    market_title: str = ""
    option: str | None = None
    settled: bool = False
    quantity: float = 0.0
    avg_cost: float = 0.0
    current_price: float | None = None
    market_value: float = 0.0
    cost_basis: float = 0.0
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    money_earned: float = 0.0
    lots: list[PositionLot] = []


class PositionsSummary(Wire):
    total_market_value: float = 0.0
    total_cost_basis: float = 0.0
    total_unrealized_pnl: float = 0.0


class Positions(Wire):
    positions: list[Position]
    summary: PositionsSummary | None = None


class Pnl(Wire):
    period: str
    period_end: str = ""
    period_start: str | None = None
    period_pnl: float | None = None
    unrealized_pnl: float = 0.0
    total_account_value: float = 0.0
    total_holdings_value: float = 0.0
    total_cost_basis: float = 0.0
    roi: float | None = None
    sharpe: float | None = None


class Fill(Wire):
    id: int
    exchange_id: str
    quantity: float
    side: Side
    filled_at: str
    order_id: int | None = None
    market_id: str = ""
    price: float | None = None


class FillPage(Wire):
    data: list[Fill]
    pagination: Pagination | None = None


class SettlementEntry(Wire):
    id: str
    payout: float = 0.0
    type: str | None = None
    created_at: str | None = None
    market_id: str | None = None
    market_title: str | None = None
    exchange_id: str | None = None
    settlement_option: str | None = None
    settlement_outcome: str | None = None
    shares: float | None = None
    avg_entry_price: float | None = None
    cost_basis: float | None = None
    realized_pnl: float | None = None


class SettlementPage(Wire):
    data: list[SettlementEntry]
    pagination: Pagination | None = None


class HistoryPoint(Wire):
    time: str
    total_value: float
    holdings_value: float | None = None
    cash_value: float | None = None
    interpolated: bool = False


class PortfolioHistory(Wire):
    interval: str
    data: list[HistoryPoint] = []


class SmartScore(Wire):
    market_type: str
    smart_score: float
    smart_score_decayed: float
    is_elite: bool
    rank: int
    total_traders: int
    minimum_elite_score: float
    calculation_as_of: str
    selected_via: str | None = None


class LeaderboardEntry(Wire):
    rank: int
    profile_id: str
    username: str | None = None
    pnl: float = 0.0
    trades_count: int = 0
    volume: float = 0.0
    win_rate: float | None = None
    roi: float = 0.0


class Leaderboard(Wire):
    leaderboard: list[LeaderboardEntry]
    total: int = 0
    period: str = ""
    limit: int = 0
    offset: int = 0
    my_rank: int | None = None


class Collateral(Wire):
    component_id: str
    outstanding_advance: float = 0.0
    covered_exchange_ids: list[int] = []
    relationship_ids: list[str] = []
    guaranteed_payout_floor: float | None = None


class CollateralList(Wire):
    data: list[Collateral]


class RelationshipNode(Wire):
    exchange_id: str
    role: str
    market_id: str | None = None
    market_title: str | None = None
    outcome: str | None = None
    current_price: float | None = None


class Relationship(Wire):
    id: str
    type: str
    status: str
    is_exhaustive: bool = False
    label: str | None = None
    nodes: list[RelationshipNode] = []


class RelationshipPage(Wire):
    data: list[Relationship]
    pagination: Pagination | None = None


class CorrectiveTrade(Wire):
    exchange_id: str
    outcome_side: str
    action: str
    rationale: str = ""
    current_price: float | None = None


class Constraint(Wire):
    relationship_id: str
    type: str
    violation_amount: float
    evaluation_status: str
    suggested_corrective_trades: list[CorrectiveTrade] = []
    tournament_id: str | None = None


class ConstraintList(Wire):
    data: list[Constraint]
    violations_count: int = 0
    computed_at: str = ""


class RealtimeToken(Wire):
    token: str
    expires_at: str
    supabase_url: str
    anon_key: str
    channels: dict[str, str] = {}


class Delivery(Wire):
    revision: int | None = None
    previous_revision: int | None = None
    correlation_id: str | None = None


class RtTrade(Wire):
    exchange_id: str
    market_id: str
    price: float
    quantity: float
    executed_at: str = ""
    tournament_id: str | None = None
    id: str | None = None
    sequence: int | None = None
    """Engine event sequence of the match; null means resync that exchange over REST."""


class RtBookDirty(Wire):
    exchange_id: str
    market_id: str
    tournament_id: str | None = None
    at: str | None = None


class RtMarketSettled(Wire):
    market_id: str
    tournament_id: str | None = None
    settled_with: str | None = None
    at: str | None = None


class RtBook(Wire):
    """Complete order book pushed on ``tournament:{id}:market:{id}`` topics (YES-normalized, no depth cap)."""

    exchange_id: int | str
    as_of: BookVersion | None = None
    next_expiry_at: str | None = None
    bids: list[BookLevel] = []
    asks: list[BookLevel] = []


class MarketBatch(Wire):
    trades: list[RtTrade] = []
    book_dirty: list[RtBookDirty] = []
    market_settled: list[RtMarketSettled] = []
    delivery: Delivery | None = None
    books: list[RtBook] = []
    resync_required: bool = False
    """Too large for one message: apply ``books``, then resync the market over REST."""


class RtFill(Wire):
    exchange_id: str
    quantity: float
    price: float | None = None
    """YES-denominated; null when the fill carried no price (spec 2026-10-03): resync that position over
    REST instead of accounting for it here."""
    order_id: int | None = None
    market_id: str = ""
    executed_at: str = ""
    tournament_id: str | None = None


class RtOrderUpdate(Wire):
    order_id: int
    exchange_id: str
    open: bool
    market_id: str = ""
    quantity_traded: float = 0.0
    total_cost: float = 0.0
    latest_trade_price: float | None = None
    tournament_id: str | None = None
    at: str | None = None


class RtSettlement(Wire):
    market_id: str
    exchange_id: str
    payout: float = 0.0
    shares: float = 0.0
    outcome_side: str | None = None
    cost_basis: float | None = None
    realized_pnl: float | None = None
    settlement_outcome: str | None = None
    tournament_id: str | None = None
    at: str | None = None


class RtRefund(Wire):
    market_id: str
    exchange_id: str
    shares: float = 0.0
    refund_amount: float = 0.0
    tournament_id: str | None = None
    at: str | None = None


class AccountBatch(Wire):
    fills: list[RtFill] = []
    order_updates: list[RtOrderUpdate] = []
    settlements: list[RtSettlement] = []
    refunds: list[RtRefund] = []
    delivery: Delivery | None = None
