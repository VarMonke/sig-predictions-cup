"""Minimal Supabase Realtime (Phoenix channels, vsn 1.0.0) client for SIG's private broadcast feeds.

Realtime is best-effort: every (re)join and every revision gap triggers ``on_resync(topic)`` so the
caller can reconcile against REST, which remains the source of truth.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Iterable
from datetime import UTC, datetime
from urllib.parse import urlparse

import aiohttp
import msgspec

from . import models as m
from .client import SigClient

log = logging.getLogger(__name__)

HEARTBEAT_S = 25.0
TOKEN_REFRESH_MARGIN_S = 15 * 60
MAX_CHANNELS_PER_CONNECTION = 90
"""SIG's Realtime rejects joins past 100 channels on one socket ("ChannelRateLimitReached")."""
JOIN_SPACING_S = 0.02


def chunked(channels: list[str], size: int) -> list[list[str]]:
    return [channels[i : i + size] for i in range(0, len(channels), size)]


class PhxMessage(msgspec.Struct, gc=False):
    topic: str
    event: str
    payload: msgspec.Raw = msgspec.Raw(b"{}")
    ref: str | None = None


class BroadcastPayload(msgspec.Struct, gc=False):
    event: str = ""
    type: str = ""
    payload: msgspec.Raw = msgspec.Raw(b"{}")


class JoinReply(msgspec.Struct, gc=False):
    status: str = ""
    response: dict[str, object] = {}


_phx_decoder = msgspec.json.Decoder(PhxMessage)
_bcast_decoder = msgspec.json.Decoder(BroadcastPayload)
_reply_decoder = msgspec.json.Decoder(JoinReply)
_market_batch_decoder = msgspec.json.Decoder(m.MarketBatch)
_account_batch_decoder = msgspec.json.Decoder(m.AccountBatch)
_encoder = msgspec.json.Encoder()

type MarketHandler = Callable[[str, m.MarketBatch], None]
type AccountHandler = Callable[[m.AccountBatch], None]
type ResyncHandler = Callable[[str], Awaitable[None] | None]
type RawHandler = Callable[[str, str, bytes], None]


class RealtimeClient:
    def __init__(
        self,
        sig: SigClient,
        channels: Iterable[str],
        *,
        on_market_batch: MarketHandler | None = None,
        on_account_batch: AccountHandler | None = None,
        on_resync: ResyncHandler | None = None,
        on_other: RawHandler | None = None,
        max_channels_per_connection: int = MAX_CHANNELS_PER_CONNECTION,
    ) -> None:
        """Channels are spread over as many sockets as needed, at most ``max_channels_per_connection`` each;
        each socket reconnects on its own."""
        self._sig = sig
        self._channels = list(dict.fromkeys(channels))
        self._per_connection = max_channels_per_connection
        self._on_market = on_market_batch
        self._on_account = on_account_batch
        self._on_resync = on_resync
        self._on_other = on_other
        self._ref = 0
        self._pending_joins: dict[str, str] = {}
        self._revisions: dict[str, int] = {}
        self._joined: set[str] = set()
        self.connected = asyncio.Event()
        self.last_message_mono = 0.0

    @property
    def joined(self) -> frozenset[str]:
        return frozenset(self._joined)

    async def run(self, stop: asyncio.Event) -> None:
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(
                *(
                    self._connection(session, idx, chunk, stop)
                    for idx, chunk in enumerate(chunked(self._channels, self._per_connection))
                ),
            )

    async def _connection(
        self,
        session: aiohttp.ClientSession,
        idx: int,
        channels: list[str],
        stop: asyncio.Event,
    ) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                await self._session_loop(session, stop, channels)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("realtime #%d disconnected: %r; reconnecting in %.1fs", idx, exc, backoff)
            finally:
                self.connected.clear()
                self._joined.difference_update(channels)
                for ref in [ref for ref, channel in self._pending_joins.items() if channel in channels]:
                    del self._pending_joins[ref]
            if stop.is_set():
                break
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=backoff)
            backoff = min(30.0, backoff * 2)

    async def _session_loop(
        self,
        session: aiohttp.ClientSession,
        stop: asyncio.Event,
        channels: list[str],
    ) -> None:
        tok = await self._sig.realtime_token()
        host = urlparse(tok.supabase_url).hostname
        url = f"wss://{host}/realtime/v1/websocket?apikey={tok.anon_key}&vsn=1.0.0"
        async with session.ws_connect(url, heartbeat=None, max_msg_size=16 * 1024 * 1024) as ws:
            reader = asyncio.create_task(self._reader(ws), name="rt-reader")
            tasks = {reader}
            try:
                for channel in channels:
                    if reader.done():
                        break
                    await self._join(ws, channel, tok.token)
                    if len(channels) > 1:
                        await asyncio.sleep(JOIN_SPACING_S)
                tasks |= {
                    asyncio.create_task(self._heartbeat(ws), name="rt-heartbeat"),
                    asyncio.create_task(self._refresh(ws, tok.expires_at, channels), name="rt-refresh"),
                    asyncio.create_task(stop.wait(), name="rt-stop"),
                }
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task.get_name() != "rt-stop" and task.exception() is not None:
                        raise task.exception()  # type: ignore[misc]
            finally:
                for task in tasks:
                    task.cancel()
                for task in tasks:
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task

    async def _join(self, ws: aiohttp.ClientWebSocketResponse, channel: str, token: str) -> None:
        """The join is registered before it is sent, so even an instant reply finds it."""
        self._ref += 1
        ref = str(self._ref)
        self._pending_joins[ref] = channel
        frame = {
            "topic": f"realtime:{channel}",
            "event": "phx_join",
            "payload": {
                "config": {
                    "broadcast": {"ack": False, "self": False},
                    "presence": {"key": "", "enabled": False},
                    "postgres_changes": [],
                    "private": True,
                },
                "access_token": token,
            },
            "ref": ref,
            "join_ref": ref,
        }
        await ws.send_str(_encoder.encode(frame).decode())

    async def _reader(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        loop = asyncio.get_running_loop()
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                self.last_message_mono = loop.time()
                await self._handle(msg.data.encode())
            elif msg.type == aiohttp.WSMsgType.BINARY:
                self.last_message_mono = loop.time()
                await self._handle(msg.data)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break
        raise ConnectionError(f"websocket closed (code={ws.close_code})")

    async def _send(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        topic: str,
        event: str,
        payload: object,
    ) -> str:
        self._ref += 1
        ref = str(self._ref)
        frame: dict[str, object] = {"topic": topic, "event": event, "payload": payload, "ref": ref}
        await ws.send_str(_encoder.encode(frame).decode())
        return ref

    async def _heartbeat(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_S)
            await self._send(ws, "phoenix", "heartbeat", {})

    async def _refresh(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        expires_at: str,
        channels: list[str],
    ) -> None:
        while True:
            expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            wait = (expiry - datetime.now(UTC)).total_seconds() - TOKEN_REFRESH_MARGIN_S
            await asyncio.sleep(max(30.0, wait))
            tok = await self._sig.realtime_token()
            expires_at = tok.expires_at
            for channel in channels:
                await self._send(ws, f"realtime:{channel}", "access_token", {"access_token": tok.token})
            log.info("realtime token refreshed for %d channels (expires %s)", len(channels), expires_at)
            for channel in channels:
                await self._resync(channel)

    async def _handle(self, raw: bytes) -> None:
        try:
            msg = _phx_decoder.decode(raw)
        except msgspec.DecodeError:
            log.debug("realtime: undecodable frame %r", raw[:200])
            return
        topic = msg.topic.removeprefix("realtime:")
        event = msg.event
        if event == "broadcast":
            bcast = _bcast_decoder.decode(msg.payload)
            await self._dispatch(topic, bcast.event, bytes(bcast.payload))
        elif event == "phx_reply":
            channel = self._pending_joins.pop(msg.ref, None) if msg.ref is not None else None
            if channel is None:
                return
            reply = _reply_decoder.decode(msg.payload)
            if reply.status == "ok":
                self._joined.add(channel)
                log.debug("realtime joined %s", channel)
                if self._joined.issuperset(self._channels) and not self.connected.is_set():
                    log.info("realtime joined all %d channels", len(self._channels))
                    self.connected.set()
                await self._resync(channel)
            else:
                log.error("realtime join %s failed: %s", channel, reply.response)
                raise ConnectionError(f"join {channel} failed: {reply.response}")
        elif event in ("phx_error", "phx_close"):
            self._joined.discard(topic)
            raise ConnectionError(f"channel {topic} {event}")
        elif event == "system":
            log.info("realtime system %s: %s", topic, bytes(msg.payload)[:300].decode(errors="replace"))
        elif self._on_other is not None:
            self._on_other(topic, event, bytes(msg.payload))

    async def _dispatch(self, topic: str, event: str, payload: bytes) -> None:
        try:
            if event == "market_batch" and self._on_market is not None:
                batch = _market_batch_decoder.decode(payload)
            elif event == "account_batch" and self._on_account is not None:
                batch = _account_batch_decoder.decode(payload)
            else:
                if self._on_other is not None:
                    self._on_other(topic, event, payload)
                return
        except msgspec.DecodeError as exc:
            # a payload our models do not expect (a new nullable, a new field type) must cost one REST
            # resync of that topic, not the socket and its 90 channels
            # an exception here would tear the connection down and reconnect it, batch after batch
            log.warning(
                "realtime %s on %s does not decode (%s): resyncing; payload %r",
                event,
                topic,
                exc,
                payload[:200],
            )
            await self._resync(topic)
            return
        await self._check_gap(topic, batch.delivery)
        if isinstance(batch, m.MarketBatch):
            assert self._on_market is not None
            self._on_market(topic, batch)
        else:
            assert self._on_account is not None
            self._on_account(batch)

    async def _check_gap(self, topic: str, delivery: m.Delivery | None) -> None:
        if delivery is None or delivery.revision is None:
            return
        last = self._revisions.get(topic)
        if last is not None and delivery.revision <= last:
            return
        self._revisions[topic] = delivery.revision
        if last is not None and delivery.previous_revision != last:
            log.warning("realtime gap on %s (had %s, prev %s)", topic, last, delivery.previous_revision)
            await self._resync(topic)

    async def _resync(self, topic: str) -> None:
        if self._on_resync is None:
            return
        result = self._on_resync(topic)
        if asyncio.iscoroutine(result):
            await result
