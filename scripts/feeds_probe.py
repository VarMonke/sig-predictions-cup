"""Run the Polymarket WS feed + Kalshi poller for N seconds and summarize the quote board. Public data only.

Usage: uv run python scripts/feeds_probe.py [seconds]
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

from sigbot.feeds.kalshi import KalshiPoller
from sigbot.feeds.polymarket import PolymarketFeed
from sigbot.market.mapping import load_mapping
from sigbot.market.quotes import QuoteBoard, Source


async def main(seconds: float) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    mapping = load_mapping(Path("sig-api/mapping.json"))
    board = QuoteBoard()
    changes = {Source.POLYMARKET: 0, Source.KALSHI: 0}
    board.subscribe(lambda src, _inst: changes.__setitem__(src, changes[src] + 1))
    pm = PolymarketFeed(mapping.pm_tokens(), board)
    ks = KalshiPoller(mapping.kalshi_tickers(), board, max_rps=8)
    stop = asyncio.Event()
    tasks = [asyncio.create_task(pm.run(stop)), asyncio.create_task(ks.run(stop))]
    await asyncio.sleep(seconds)
    stop.set()
    await asyncio.gather(*tasks)
    pm_have = sum(1 for t in mapping.pm_tokens() if board.get(t))
    k_have = sum(1 for t in mapping.kalshi_tickers() if board.get(t))
    print(
        f"polymarket: {pm_have}/{len(mapping.pm_tokens())} tokens quoted, "
        f"{changes[Source.POLYMARKET]} top-of-book changes, {pm.messages} frames",
    )
    print(
        f"kalshi: {k_have}/{len(mapping.kalshi_tickers())} tickers quoted, {changes[Source.KALSHI]} changes, "
        f"{ks.errors} errors",
    )
    for race in mapping.races[:6]:
        for o in race.outcomes:
            pmq = [board.get(t) for t in o.pm_tokens]
            kq = [board.get(t) for t in o.kalshi_tickers]
            fmt = lambda qs: ",".join(f"{q.bid}/{q.ask}" for q in qs if q) or "-"  # noqa: E731
            print(f"  {race.key:12} {o.party} pm={fmt(pmq):<14} kalshi={fmt(kq)}")


if __name__ == "__main__":
    asyncio.run(main(float(sys.argv[1]) if len(sys.argv) > 1 else 20))
