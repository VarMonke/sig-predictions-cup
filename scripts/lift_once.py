"""
I wasn't available to watch the news for like 10h so I just settled whatever was almost confirmed
(but wow surprising elections lol)

One-off: buy a fixed whitelist of near-certain sides at or under a limit and hold to settlement
Usage: set -a && . ./deploy/.env && set +a && uv run python scripts/lift_once.py [--live]
"""

from __future__ import annotations

import asyncio
import math
import os
import sys
from datetime import UTC, datetime, timedelta

from sigbot.core.util import RequestBudget, new_key
from sigbot.sigapi import SigApiError, SigClient
from sigbot.sigapi import models as m

BUDGET = 13_200.0
# (label, exchange, side bought, limit price of that side, max shares)
# Round 1 (08:57 UTC): SEN-WY R YES 4,013 @ 0.86, SEN-TN D NO 15,023 @ 0.855; GOV-SD / GOV-ID unfilled.
LEGS = [
    ("SEN-AL R YES", "942", m.Side.YES, 0.88, 9815),
    ("SEN-DE R NO", "1075", m.Side.NO, 0.875, 5190),
]


async def main(live: bool) -> None:
    base = os.environ.get("SIG_BASE_URL", "https://sig.thesuper.market/api/v1")
    budget = RequestBudget(limit=10, data_limit=5, trade_headroom=4, write_limit=10)
    async with SigClient(base, os.environ["API_KEY"], budget=budget) as sig:
        sig.tournament_id = (await sig.tournament(os.environ.get("TOURNAMENT_SLUG", "midterm-elections"))).id
        left = BUDGET
        for label, ex, side, px, cap in LEGS:
            qty = min(cap, math.floor(left / px))
            if qty < 100:
                print(f"{label}: skipped, {left:.0f} left")
                continue
            print(f"{label}: BUY {side.value} {qty} @ <= {px} (~{qty * px:,.0f})", flush=True)
            if not live:
                left -= qty * px
                continue
            expiry = (datetime.now(UTC) + timedelta(seconds=15)).isoformat(timespec="seconds")
            order = sig.order(ex, side, m.Action.BUY, qty, px, expiration_date=expiry)
            try:
                res = await sig.place_order(order, idempotency_key=new_key("lift-"))
            except SigApiError as exc:
                print(f"  ERROR {exc} -> stopping (outcome may be unknown; check positions)")
                return
            cost = res.total_cost or res.quantity_traded * px
            left -= cost
            print(
                f"  filled {res.quantity_traded:,.0f} for {cost:,.0f} (avg {res.fill_price}); "
                f"resting={res.open} remaining={res.remaining_quantity}",
                flush=True,
            )
        print(f"done; budget left {left:,.0f}")


if __name__ == "__main__":
    asyncio.run(main("--live" in sys.argv))
