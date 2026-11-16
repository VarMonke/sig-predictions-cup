"""Exercise the SIG state layer live (read-only) within the request budget: bulk top-of-book scan,
on-demand depth for one race, realtime hints -> coalesced refresh, and an account reconcile.
Never places orders. Uses roughly 10-20 SIG requests.

Usage: set -a && . ./deploy/.env && set +a && uv run python scripts/state_probe.py [seconds]
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from pathlib import Path

from sigbot.core.db import Recorder
from sigbot.market.mapping import load_mapping
from sigbot.market.state import AccountState, SigBooks
from sigbot.sigapi import SigClient
from sigbot.sigapi.realtime import RealtimeClient


async def main(seconds: float) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    slug = os.environ.get("TOURNAMENT_SLUG", "midterm-elections")
    mapping = load_mapping(Path("sig-api/mapping.json"))
    exchanges = {o.sig_exchange_id: o.sig_market_id for r in mapping.races for o in r.outcomes}
    rec = Recorder(None)
    base = os.environ.get("SIG_BASE_URL", "https://sig.thesuper.market/api/v1")
    async with SigClient(base, os.environ["API_KEY"]) as sig:
        tour = await sig.tournament(slug)
        me = await sig.account()
        sig.tournament_id = tour.id
        books = SigBooks(sig, exchanges, rec)
        account = AccountState(sig, slug, rec)
        changes: list[str] = []
        books.subscribe(changes.append)

        t0 = time.perf_counter()
        await books.scan_all()
        with_bid = sum(1 for b in books.books.values() if b.bids)
        with_ask = sum(1 for b in books.books.values() if b.asks)
        print(
            f"scan_all: {len(books.books)} books in {time.perf_counter() - t0:.1f}s "
            f"({books.bulk_requests} requests); with bid {with_bid}, with ask {with_ask}",
        )

        race = mapping.races[0]
        for o in race.outcomes:
            b = await books.fetch_depth(o.sig_exchange_id)
            print(f"  depth {race.key}/{o.party} x{b.exchange_id}: bids={b.bids[:2]} asks={b.asks[:2]}")

        await account.reconcile()
        print(
            f"account: cash={account.cash} status={account.tournament_status} "
            f"holdings={len(account.holdings)} open_orders={len(account.open_orders)}",
        )

        def on_resync(topic: str) -> None:
            if topic.startswith("tournament:"):
                books.resync()
            else:
                account.request_reconcile()

        changes.clear()
        stop = asyncio.Event()
        rt = RealtimeClient(
            sig,
            [f"tournament:{tour.id}", f"user:{me.id}"],
            on_market_batch=books.on_market_batch,
            on_account_batch=account.on_account_batch,
            on_resync=on_resync,
        )
        tasks = [
            asyncio.create_task(books.run(stop)),
            asyncio.create_task(account.run(stop)),
            asyncio.create_task(rt.run(stop)),
        ]
        await asyncio.sleep(seconds)
        stop.set()
        await asyncio.gather(*tasks)
        used_all, used_data = sig.budget.used()
        print(
            f"after {seconds:.0f}s live: book changes={len(changes)} ({len(set(changes))} exchanges); "
            f"bulk requests={books.bulk_requests} depth requests={books.depth_requests}; "
            f"sig requests={sig.requests} rate_limited={sig.rate_limited}; "
            f"budget window used={used_all} (data {used_data})",
        )


if __name__ == "__main__":
    asyncio.run(main(float(sys.argv[1]) if len(sys.argv) > 1 else 25))
