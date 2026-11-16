"""Snipe mispriced and stale orders against fair value.

Takes existing liquidity only: BUY YES below fair, BUY NO when the YES bid is above fair. Walks the book
level by level while each level's own edge clears the threshold, taking shares at a level only while the
total, including shares of that side we already hold, stays within the fractional Kelly size at that level's
price ("marginal Kelly"); the order's limit is the last level taken, and SIG fills better prices first. Cost
caps live in the risk gate (bigger for bigger edges); ``max_order`` is only a sanity bound in shares. Sharp,
unconfirmed moves demand extra edge and are sized down.
"""

from __future__ import annotations

from dataclasses import dataclass

from sigbot.core.reasons import TradeReason
from sigbot.core.util import Mils, sig_floor
from sigbot.market.fair import Fair
from sigbot.market.mapping import Race
from sigbot.market.state import Level
from sigbot.sigapi.models import Side

from .base import Intent, Leg, View


@dataclass(frozen=True, slots=True)
class SnipeConfig:
    min_edge: Mils = 30
    """Minimum price-vs-fair edge to act (mils)."""
    unconfirmed_extra_edge: Mils = 20
    kelly_fraction: float = 0.25
    unconfirmed_size_factor: float = 0.5
    max_order: int = 25_000
    """Sanity bound in shares; the risk gate's cost caps decide size (this binds only on cheap contracts)."""
    min_return_on_capital: float = 0.08
    """Edge divided by what a share costs, at least. Capital is locked until settlement, so a 4c edge on
    a NO at 0.95 (4%) is a worse use of it than a 5c edge at 0.50 (10%): on 2026-10-01 such clips parked
    ~22k in 1-5% positions until the deployed cap turned better trades away."""


def kelly_fraction_yes(fair: float, price: float) -> float:
    """Optimal stake fraction for a YES contract at ``price`` with true probability ``fair`` (0..1000)."""
    if fair <= price or price <= 0:
        return 0.0
    return (fair - price) / (1000 - price)


def kelly_fraction_no(fair: float, bid: float) -> float:
    """Optimal stake fraction for a NO contract at ``1000 - bid`` with YES probability ``fair``.

    NO wins with probability (1 - fair) and costs (1 - bid); Kelly f* = (bid - fair) / bid.
    """
    if fair >= bid or bid <= 0:
        return 0.0
    return (bid - fair) / bid


class SnipeStrategy:
    name = "snipe"

    def __init__(self, cfg: SnipeConfig | None = None) -> None:
        self.cfg = cfg or SnipeConfig()

    def evaluate(self, race: Race, view: View, now: float) -> list[Intent]:
        equity = max(0.0, view.equity)
        out: list[Intent] = []
        for outcome in race.outcomes:
            fair = view.fair(outcome.sig_exchange_id)
            if fair is None:
                continue
            book = view.book(outcome.sig_exchange_id)
            if book.settled or "label_risk" in race.flags:
                continue  # settlement label uncertain: no directional trades here
            holding = view.holding(outcome.sig_exchange_id)
            position = holding.qty if holding is not None else 0.0
            out.extend(
                self._asks(race, outcome.sig_exchange_id, book.asks, fair, equity, now, max(0.0, position)),
            )
            out.extend(
                self._bids(race, outcome.sig_exchange_id, book.bids, fair, equity, now, max(0.0, -position)),
            )
        return out

    def _asks(
        self,
        race: Race,
        ex: str,
        asks: list[Level],
        fair: Fair,
        equity: float,
        now: float,
        held: float,
    ) -> list[Intent]:
        taken = self._walk(asks, fair, equity, held, yes=True)
        if taken is None:
            return []
        qty, avg, limit = taken
        top = asks[0].price
        detail = (
            f"ask {top} vs fair {fair.value}" if limit == top else f"asks {top}-{limit} vs fair {fair.value}"
        )
        return [
            Intent(
                self.name,
                race.key,
                (Leg(ex, Side.YES, sig_floor(limit), qty),),
                fair.value - avg,
                TradeReason.MISPRICED_ORDER,
                detail,
                now,
            ),
        ]

    def _bids(
        self,
        race: Race,
        ex: str,
        bids: list[Level],
        fair: Fair,
        equity: float,
        now: float,
        held: float,
    ) -> list[Intent]:
        taken = self._walk(bids, fair, equity, held, yes=False)
        if taken is None:
            return []
        qty, avg, limit = taken
        top = bids[0].price
        detail = (
            f"bid {top} vs fair {fair.value}" if limit == top else f"bids {top}-{limit} vs fair {fair.value}"
        )
        return [
            Intent(
                self.name,
                race.key,
                (Leg(ex, Side.NO, sig_floor(1000 - limit), qty),),  # a NO share costs 1 - bid
                avg - fair.value,
                TradeReason.MISPRICED_ORDER,
                detail,
                now,
            ),
        ]

    def _walk(
        self,
        levels: list[Level],
        fair: Fair,
        equity: float,
        held: float,
        *,
        yes: bool,
    ) -> tuple[int, float, Mils] | None:
        """(shares, average YES price, worst YES price taken) across the levels worth taking, or None.

        A level is taken while its own edge clears the threshold, and only up to the Kelly size at that
        level's price minus what better levels already gave and the ``held`` shares of this side we already
        own (otherwise a level that nobody else takes would be sniped again on every pass). A level of
        unknown size is assumed to hold the rest and ends the walk (nothing is known about deeper levels)."""
        threshold = self._threshold(fair)
        qty, cost, limit = 0, 0.0, None
        owned = int(held)
        for lv in levels:
            edge = fair.value - lv.price if yes else lv.price - fair.value
            cost_price = lv.price if yes else 1000 - lv.price
            # deeper levels only get worse on both counts, so the first miss ends the walk
            if edge < threshold or edge < self.cfg.min_return_on_capital * cost_price:
                break
            kelly = (
                kelly_fraction_yes(fair.value, lv.price) if yes else kelly_fraction_no(fair.value, lv.price)
            )
            target = min(self.cfg.max_order, self._size(cost_price, fair, equity, kelly))
            room = target - owned - qty
            take = room if lv.qty < 0 else min(int(lv.qty), room)
            if take <= 0:
                break
            qty, cost, limit = qty + take, cost + take * lv.price, lv.price
            if lv.qty < 0:
                break
        if qty <= 0 or limit is None:
            return None
        return qty, cost / qty, limit

    def _threshold(self, fair: Fair) -> Mils:
        extra = self.cfg.unconfirmed_extra_edge if not fair.confirmed else 0
        return int(self.cfg.min_edge + fair.unc + extra)

    def _size(self, price: Mils, fair: Fair, equity: float, kelly: float) -> int:
        """Shares for a stake of (fractional Kelly x equity) SUSQies at ``price`` mils per share."""
        cfg = self.cfg
        factor = cfg.kelly_fraction * (cfg.unconfirmed_size_factor if not fair.confirmed else 1.0)
        stake_frac = min(1.0, factor * kelly)
        stake = equity * stake_frac
        return max(1, min(cfg.max_order, int(stake * 1000 / price)))
