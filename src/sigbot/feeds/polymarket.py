"""Polymarket CLOB market websocket (public, no auth) -> QuoteBoard."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable, Iterable

import aiohttp
import msgspec

from sigbot.core.util import to_mils
from sigbot.market.quotes import QuoteBoard, Source

log = logging.getLogger(__name__)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_S = 10.0
TOKENS_PER_CONNECTION = 150


class Level(msgspec.Struct, gc=False):
    price: str
    size: str


class PriceChange(msgspec.Struct, gc=False):
    asset_id: str
    price: str
    size: str
    side: str
    best_bid: str | None = None
    best_ask: str | None = None


class Event(msgspec.Struct, gc=False):
    event_type: str = ""
    asset_id: str = ""
    bids: list[Level] = []
    asks: list[Level] = []
    price_changes: list[PriceChange] = []
    timestamp: str = ""
    """Server time in epoch ms. On price_change it is the send time; on a book snapshot, the last change."""


_events_decoder = msgspec.json.Decoder(list[Event] | Event)


class _Book:
    __slots__ = ("asks", "bids")

    def __init__(self) -> None:
        self.bids: dict[int, float] = {}
        self.asks: dict[int, float] = {}

    def replace(self, bids: list[Level], asks: list[Level]) -> None:
        self.bids = {to_mils(float(lv.price)): float(lv.size) for lv in bids if float(lv.size) > 0}
        self.asks = {to_mils(float(lv.price)): float(lv.size) for lv in asks if float(lv.size) > 0}

    def apply(self, change: PriceChange) -> None:
        side = self.bids if change.side == "BUY" else self.asks
        price, size = to_mils(float(change.price)), float(change.size)
        if size > 0:
            side[price] = size
        else:
            side.pop(price, None)

    def top(self) -> tuple[int, int, float, float]:
        bid = max(self.bids) if self.bids else 0
        ask = min(self.asks) if self.asks else 1000
        return bid, ask, self.bids.get(bid, 0.0), self.asks.get(ask, 0.0)


class PolymarketFeed:
    def __init__(
        self,
        tokens: Iterable[str],
        board: QuoteBoard,
        *,
        observe: Callable[[str, str, float], None] | None = None,
    ) -> None:
        self._tokens = sorted(set(tokens))
        self._board = board
        self._observe = observe
        self._books: dict[str, _Book] = {}
        self._connected: dict[int, bool] = {}
        self.messages = 0

    async def run(self, stop: asyncio.Event) -> None:
        chunks = [
            self._tokens[i : i + TOKENS_PER_CONNECTION]
            for i in range(0, len(self._tokens), TOKENS_PER_CONNECTION)
        ]
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(
                *(self._connection(session, idx, chunk, stop) for idx, chunk in enumerate(chunks)),
            )

    def _set_connected(self, idx: int, ok: bool) -> None:
        self._connected[idx] = ok
        self._board.set_source_ok(Source.POLYMARKET, bool(self._connected) and all(self._connected.values()))

    async def _connection(
        self,
        session: aiohttp.ClientSession,
        idx: int,
        tokens: list[str],
        stop: asyncio.Event,
    ) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                async with session.ws_connect(WS_URL, heartbeat=None, max_msg_size=32 * 1024 * 1024) as ws:
                    await ws.send_str(msgspec.json.encode({"assets_ids": tokens, "type": "market"}).decode())
                    self._set_connected(idx, True)
                    backoff = 1.0
                    log.info("polymarket ws #%d connected (%d tokens)", idx, len(tokens))
                    await self._pump(ws, stop)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("polymarket ws #%d error: %r", idx, exc)
            finally:
                self._set_connected(idx, False)
            if stop.is_set():
                break
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=backoff)
            backoff = min(30.0, backoff * 2)

    async def _pump(self, ws: aiohttp.ClientWebSocketResponse, stop: asyncio.Event) -> None:
        async def pinger() -> None:
            while True:
                await asyncio.sleep(PING_S)
                await ws.send_str("PING")

        ping_task = asyncio.create_task(pinger())
        stop_task = asyncio.create_task(stop.wait())
        try:
            while True:
                recv = asyncio.create_task(ws.receive())
                done, _ = await asyncio.wait(
                    {recv, stop_task, ping_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task in done or ping_task in done:
                    recv.cancel()
                    if ping_task in done and ping_task.exception() is not None:
                        raise ping_task.exception()  # type: ignore[misc]
                    return
                msg = recv.result()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    if msg.data != "PONG":
                        self._handle(msg.data)
                elif msg.type in (
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.ERROR,
                ):
                    raise ConnectionError(f"closed ({ws.close_code})")
        finally:
            for task in (ping_task, stop_task):
                task.cancel()

    def _handle(self, data: str) -> None:
        try:
            decoded = _events_decoder.decode(data)
        except msgspec.DecodeError:
            log.debug("polymarket: undecodable frame %r", data[:200])
            return
        events = decoded if isinstance(decoded, list) else [decoded]
        self.messages += 1
        for ev in events:
            if ev.event_type == "book":
                book = self._books.setdefault(ev.asset_id, _Book())
                book.replace(ev.bids, ev.asks)
                self._publish(ev.asset_id, book)
            elif ev.event_type == "price_change":
                if self._observe is not None and ev.timestamp.isdigit():
                    self._observe("polymarket", "feed", time.time() * 1000 - int(ev.timestamp))
                touched: set[str] = set()
                for change in ev.price_changes:
                    book = self._books.get(change.asset_id)
                    if book is None:
                        continue
                    book.apply(change)
                    touched.add(change.asset_id)
                for asset in touched:
                    self._publish(asset, self._books[asset])

    def _publish(self, asset: str, book: _Book) -> None:
        bid, ask, bid_size, ask_size = book.top()
        self._board.update(Source.POLYMARKET, asset, bid, ask, bid_size, ask_size)
