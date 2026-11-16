"""Alerting: rate-limited, batched Discord REST posts (separate channels for alerts and activity)."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import aiohttp

from sigbot.core.reasons import TradeReason
from sigbot.core.util import wait_event_or_stop
from sigbot.sigapi.models import Side
from sigbot.strategies.exits import HURDLE, RECYCLE, UNWIND

if TYPE_CHECKING:
    from sigbot.trading.executor import ExecutionResult

log = logging.getLogger(__name__)

DISCORD_API = "https://discord.com/api/v10"
PARTY_NAMES = {"D": "Dem", "R": "Rep", "I": "Ind"}


def cents(mils: float) -> str:
    return f"{round(mils / 10, 1):g}c"


def activity_line(
    strategy: str,
    action: str,
    market: str,
    trade: str,
    *info: str,
    pnl: float | None = None,
    cash: float | None = None,
    shadow: bool = False,
) -> str:
    """The one shape every fill is reported in: ``STRATEGY BUY|SELL market · trade · info [+pnl]``, from
    the position's point of view (a SELL is a position reduced, whatever side the order bought), e.g.
    ``SWEEP SELL SEN-RI Dem · 6,000 YES @ 86c · cost 79c · cash 31,430 [+420]``. ``pnl`` is what the
    trade booked; ``cash`` the balance after it (how much ammunition is left)."""
    parts = [f"{'[shadow] ' if shadow else ''}{strategy.upper()} {action} {market}", trade, *info]
    if cash is not None:
        parts.append(f"cash {cash:,.0f}")
    line = " · ".join(p for p in parts if p)
    return f"{line} [{pnl:+,.0f}]" if pnl is not None else line


def market_name(race: str, exchange_id: str, parties: Mapping[str, str]) -> str:
    return f"{race} {PARTY_NAMES.get(parties.get(exchange_id, ''), exchange_id)}"


def format_fill(
    result: ExecutionResult,
    parties: Mapping[str, str],
    fair_yes: Callable[[str], int | None],
    cash: float | None = None,
) -> str | None:
    """One ``activity_line`` per execution; None if nothing filled. Exits, recycles, hurdle and unwind
    sales (the order buys the opposite side) read as SELL of the held side at the price it fetched, with
    the P&L booked; opening trades read as BUY with the edge to fair."""
    intent = result.intent
    fills = [
        (leg, result.filled.get(leg.exchange_id, 0.0), result.costs.get(leg.exchange_id, 0.0))
        for leg in intent.legs
    ]
    if not any(qty > 0 for _, qty, _ in fills):
        return None

    def size(wanted: int, qty: float) -> str:
        return f"{qty:,.0f}" if qty >= wanted else f"{qty:,.0f}/{wanted:,}"

    def paid(qty: float, cost: float, limit: int) -> float:
        """Average price actually paid for the bought side (mils)."""
        return cost / qty * 1000 if qty else float(limit)

    closing = intent.reduces_risk or intent.detail.startswith((RECYCLE, HURDLE, UNWIND))
    why = next((w for w in (UNWIND, RECYCLE, HURDLE) if intent.detail.startswith(w)), "")
    if len(fills) == 1:
        leg, qty, cost = fills[0]
        market = market_name(intent.race, leg.exchange_id, parties)
        if closing:
            # the pair nets for 1: the held side was sold at 1 - what the opposite side cost; the edge is
            # what that realizes per share over the position's average cost
            held = "NO" if leg.side is Side.YES else "YES"
            sold_at = 1000 - paid(qty, cost, leg.price)
            avg_cost = (1000 - leg.price) - intent.edge
            return activity_line(
                intent.strategy,
                "SELL",
                market,
                f"{size(leg.quantity, qty)} {held} @ {cents(sold_at)}",
                f"cost {cents(avg_cost)}",
                why,
                pnl=intent.edge * qty / 1000,
                cash=cash,
                shadow=result.shadow,
            )
        fair = fair_yes(leg.exchange_id)
        price = paid(qty, cost, leg.price)
        fair_text = ""
        if fair is not None:
            side_fair = fair if leg.side is Side.YES else 1000 - fair
            fair_text = f"fair {cents(side_fair)} ({round((side_fair - price) / 10, 1):+g}c edge)"
        return activity_line(
            intent.strategy,
            "BUY",
            market,
            f"{size(leg.quantity, qty)} {leg.side.name} @ {cents(price)}",
            fair_text,
            f"cost {cost:,.0f}",
            shadow=result.shadow,
        )
    legs = " + ".join(
        f"{size(leg.quantity, qty)} {leg.side.name} "
        f"{PARTY_NAMES.get(parties.get(leg.exchange_id, ''), leg.exchange_id)} "
        f"@ {cents(paid(qty, cost, leg.price))}"
        for leg, qty, cost in fills
    )
    sets = min(qty for _, qty, _ in fills)
    total = sum(c for _, _, c in fills)
    if closing:
        return activity_line(
            intent.strategy,
            "SELL",
            intent.race,
            f"{sets:,.0f} sets closed by buying {legs}",
            why,
            pnl=intent.edge * sets / 1000,
            cash=cash,
            shadow=result.shadow,
        )
    locked = (
        f"locks {intent.edge * sets / 1000:+,.0f}" if intent.reason is TradeReason.CROSS_MARKET_ARB else ""
    )
    return activity_line(
        intent.strategy,
        "BUY",
        intent.race,
        legs,
        f"cost {total:,.0f}",
        locked,
        shadow=result.shadow,
    )


@dataclass
class AlertSink:
    """Posts to Discord channels without blocking callers. A token-less sink just logs."""

    bot_token: str | None
    alert_channel: str | None
    activity_channel: str | None
    _queues: dict[str, deque[str]] = field(default_factory=lambda: defaultdict(deque))
    _embeds: dict[str, deque[dict[str, Any]]] = field(default_factory=lambda: defaultdict(deque))
    _wake: asyncio.Event = field(default_factory=asyncio.Event)
    _session: aiohttp.ClientSession | None = None

    def alert(self, message: str) -> None:
        if self.bot_token and self.alert_channel:
            self._queues[self.alert_channel].append(message)
            self._wake.set()
        else:
            log.warning("ALERT %s", message)

    def activity(self, message: str) -> None:
        if self.bot_token and self.activity_channel:
            self._queues[self.activity_channel].append(message)
            self._wake.set()
        else:
            log.info("ACTIVITY %s", message)

    def status(self, embed: dict[str, Any]) -> None:
        """A Discord embed for the activity channel (the periodic status post)."""
        if self.bot_token and self.activity_channel:
            self._embeds[self.activity_channel].append(embed)
            self._wake.set()
        else:
            log.info("STATUS %s", embed.get("description", ""))

    def batch(self, channel: str, message: str) -> None:
        self._queues[channel].append(message)
        self._wake.set()

    async def run(self, stop: asyncio.Event) -> None:
        if not self.bot_token:
            await stop.wait()  # nothing to post; messages went to the log
            return
        async with aiohttp.ClientSession(headers={"Authorization": f"Bot {self.bot_token}"}) as session:
            self._session = session
            while not stop.is_set():
                await wait_event_or_stop(self._wake, stop, 0.5)
                self._wake.clear()
                for channel in list(self._queues):
                    queue = self._queues[channel]
                    if not queue:
                        continue
                    burst = "\n\n".join(queue.popleft() for _ in range(min(5, len(queue))))
                    if not await self._post(channel, {"content": burst[:2000]}):
                        queue.appendleft(burst)
                    if queue:
                        self._wake.set()  # more left: keep going on the next tick
                for channel, embeds in list(self._embeds.items()):
                    if embeds and await self._post(channel, {"embeds": [embeds[0]]}):
                        embeds.popleft()
                    if embeds:
                        self._wake.set()
            self._session = None

    async def _post(self, channel: str, payload: dict[str, Any]) -> bool:
        """False only when rate limited (after waiting it out), so the caller retries the same payload."""
        assert self._session is not None
        try:
            async with self._session.post(f"{DISCORD_API}/channels/{channel}/messages", json=payload) as resp:
                if resp.status == 429:
                    retry = float(resp.headers.get("Retry-After", "2"))
                    log.warning("discord rate limited; waiting %.1fs", retry)
                    await asyncio.sleep(retry)
                    return False
                if resp.status >= 400:
                    body = await resp.text()
                    log.error("discord post failed %s: %s", resp.status, body[:300])
        except (aiohttp.ClientError, TimeoutError) as exc:
            log.error("discord post error: %r", exc)
        return True
