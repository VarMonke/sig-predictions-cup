"""Connect to SIG realtime (tournament + user channels) and print what arrives. Read-only.

Usage: set -a && . ./deploy/.env && set +a && uv run python scripts/realtime_probe.py [seconds]
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from sigbot.sigapi import SigClient
from sigbot.sigapi.realtime import RealtimeClient


async def main(seconds: float) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    slug = os.environ.get("TOURNAMENT_SLUG", "midterm-elections")
    async with SigClient(
        os.environ.get("SIG_BASE_URL", "https://sig.thesuper.market/api/v1"),
        os.environ["API_KEY"],
    ) as sig:
        t = await sig.tournament(slug)
        acct = await sig.account()
        channels = [f"tournament:{t.id}", f"user:{acct.id}"]
        stop = asyncio.Event()
        rt = RealtimeClient(
            sig,
            channels,
            on_market_batch=lambda topic, b: print(
                f"market_batch {topic}: trades={len(b.trades)} "
                f"dirty={len(b.book_dirty)} settled={len(b.market_settled)}",
            ),
            on_account_batch=lambda b: print(
                f"account_batch fills={len(b.fills)} updates={len(b.order_updates)}",
            ),
            on_resync=lambda topic: print(f"resync requested for {topic}"),
            on_other=lambda topic, ev, payload: print(f"other {topic} {ev}: {payload[:200]!r}"),
        )
        task = asyncio.create_task(rt.run(stop))
        try:
            await asyncio.wait_for(rt.connected.wait(), timeout=20)
            print("CONNECTED; joined:", sorted(rt.joined))
        except TimeoutError:
            print("did not join all channels within 20s; joined:", sorted(rt.joined))
        await asyncio.sleep(seconds)
        stop.set()
        await task


if __name__ == "__main__":
    asyncio.run(main(float(sys.argv[1]) if len(sys.argv) > 1 else 15))
