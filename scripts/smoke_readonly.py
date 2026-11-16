"""Read-only smoke test of every participant endpoint the bot uses. Never places orders.

Usage: set -a && . ./deploy/.env && set +a && uv run python scripts/smoke_readonly.py
"""

from __future__ import annotations

import asyncio
import os
import time
from urllib.parse import urlparse

from sigbot.sigapi import SigApiError, SigClient


async def main() -> None:
    slug = os.environ.get("TOURNAMENT_SLUG", "midterm-elections")
    async with SigClient(
        os.environ.get("SIG_BASE_URL", "https://sig.thesuper.market/api/v1"),
        os.environ["API_KEY"],
    ) as sig:

        async def check(name: str, coro):  # type: ignore[no-untyped-def]
            t0 = time.perf_counter()
            try:
                res = await coro
                print(f"OK   {name:<28} {1000 * (time.perf_counter() - t0):6.0f} ms")
                return res
            except SigApiError as e:
                print(f"FAIL {name:<28} {e}")
                return None

        acct = await check("account", sig.account())
        t = await check("tournament", sig.tournament(slug))
        if t:
            sig.tournament_id = t.id
            print(
                f"     status={t.status} members={t.member_count} markets={t.market_count} "
                f"balance={t.my_balance}",
            )
        if acct:
            print(f"     user={acct.username} balance={acct.balance}")
        markets = await check("all_markets", sig.all_markets())
        if not markets:
            return
        ex_ids = [mk.exchanges[0].id for mk in markets if mk.exchanges]
        print(f"     {len(markets)} markets, {len(ex_ids)} exchanges")
        prices = await check("bulk_prices", sig.bulk_prices(ex_ids))
        if prices:
            quoted = sum(1 for p in prices.data if p.best_bid is not None or p.best_ask is not None)
            print(
                f"     {len(prices.data)} quotes, {quoted} with a bid or ask, "
                f"missing={len(prices.missing_ids)}",
            )
        book = await check("exchange_orderbook", sig.exchange_orderbook(ex_ids[0], depth=50))
        if book:
            print(
                f"     x{book.exchange_id}: bids={[(b.price, b.quantity) for b in book.bids[:3]]} "
                f"asks={[(a.price, a.quantity) for a in book.asks[:3]]}",
            )
        await check("market_orderbook", sig.market_orderbook(markets[0].id))
        await check("exchange_price", sig.exchange_price(ex_ids[0]))
        await check("exchange_trades", sig.exchange_trades(ex_ids[0], limit=5))
        pos = await check("positions", sig.positions(slug))
        if pos:
            print(f"     {len(pos.positions)} positions")
        await check("pnl", sig.pnl(slug))
        orders = await check("list_orders(open)", sig.list_orders())
        if orders:
            print(f"     {len(orders.data)} open orders")
        await check("fills", sig.fills(slug, limit=5))
        await check("settlements", sig.settlements(slug, limit=5))
        ss = await check("smart_score", sig.smart_score(slug))
        if ss is not None:
            print(f"     {len(ss)} smart-score rows")
        await check("tournament_leaderboard", sig.tournament_leaderboard(slug, limit=5))
        await check("leaderboards", sig.leaderboards(tournament_slug=slug, limit=5))
        await check("relationships", sig.relationships())
        await check("constraints", sig.constraints())
        await check("collateral", sig.collateral())
        tok = await check("realtime_token", sig.realtime_token())
        if tok:
            print(
                f"     supabase host={urlparse(tok.supabase_url).hostname} expires={tok.expires_at} "
                f"channels={tok.channels} token_len={len(tok.token)}",
            )


if __name__ == "__main__":
    asyncio.run(main())
