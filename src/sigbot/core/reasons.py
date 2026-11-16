"""Why a trade goes through.

Every intent carries exactly one reason; it flows into logs, the DB and alerts.
"""

from __future__ import annotations

from enum import StrEnum


class TradeReason(StrEnum):
    CROSS_MARKET_ARB = "CROSS_MARKET_ARB"
    """Set arbitrage across a race's D/R(/I) markets (overpriced, underpriced or early close)."""
    MISPRICED_ORDER = "MISPRICED_ORDER"
    """Taking a stale or wrong resting order (snipe) against fair value."""
    MARKET_MAKE = "MARKET_MAKE"
    """Passive quoting to capture spread / mistake market orders (supervised phase)."""
    INVENTORY_REBALANCE = "INVENTORY_REBALANCE"
    """Reducing exposure or freeing capital when fair value / bids allow profitable recycling."""
    FAIR_VALUE_UPDATE = "FAIR_VALUE_UPDATE"
    """Fair value moved enough that acting on the new price is warranted (re-trade, reposition)."""
    EXIT_RISK = "EXIT_RISK"
    """Closing or hedging a position to reduce risk (exits, arb leg protection)."""
