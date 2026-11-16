"""Internal control API (aiohttp): status, halt, resume, pause strategies, manual exchange flags.

Reachable only inside the compose network; protected by a shared token. Read-only by default —
every mutating endpoint is still gated by the same halts, and no order placement is exposed here.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
from collections.abc import Callable

from aiohttp import web

log = logging.getLogger(__name__)


class ControlServer:
    def __init__(
        self,
        host: str,
        port: int,
        token: str,
        get_status: Callable[[], dict],
        on_halt: Callable[[str | None], None],
        on_resume: Callable[[], None],
        on_pause: Callable[[str, bool], None],
        on_manual: Callable[[str, bool], None],
        on_tier: Callable[[str], str] | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self._token = token
        self._get_status = get_status
        self._on_halt = on_halt
        self._on_resume = on_resume
        self._on_pause = on_pause
        self._on_manual = on_manual
        self._on_tier = on_tier
        self._app = web.Application()
        self._app.router.add_get("/status", self._status)
        self._app.router.add_post("/halt", self._halt)
        self._app.router.add_post("/resume", self._resume)
        self._app.router.add_post("/pause", self._pause)
        self._app.router.add_post("/manual", self._manual)
        self._app.router.add_post("/tier", self._tier)

    def _authorized(self, request: web.Request) -> bool:
        supplied = request.headers.get("X-Control-Token", "")
        return hmac.compare_digest(supplied, self._token)

    def _check(self, request: web.Request) -> web.Response | None:
        if not self._authorized(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        return None

    async def _status(self, request: web.Request) -> web.Response:
        denied = self._check(request)
        if denied is not None:
            return denied
        return web.json_response(self._get_status())

    async def _halt(self, request: web.Request) -> web.Response:
        denied = self._check(request)
        if denied is not None:
            return denied
        data = await request.json()
        self._on_halt(data.get("reason"))
        return web.json_response({"ok": True, "halted": True})

    async def _resume(self, request: web.Request) -> web.Response:
        denied = self._check(request)
        if denied is not None:
            return denied
        self._on_resume()
        return web.json_response({"ok": True, "halted": False})

    async def _pause(self, request: web.Request) -> web.Response:
        """{"strategy": "snipe"} pauses a strategy; add "paused": false to let it trade again."""
        denied = self._check(request)
        if denied is not None:
            return denied
        data = await request.json()
        self._on_pause(str(data.get("strategy", "")), bool(data.get("paused", True)))
        return web.json_response({"ok": True})

    async def _manual(self, request: web.Request) -> web.Response:
        denied = self._check(request)
        if denied is not None:
            return denied
        data = await request.json()
        self._on_manual(str(data.get("exchangeId", "")), bool(data.get("paused", True)))
        return web.json_response({"ok": True})

    async def _tier(self, request: web.Request) -> web.Response:
        """{"tier": "1".."4"} sets and holds a tier; {"tier": "auto"} re-enables automatic promotion."""
        denied = self._check(request)
        if denied is not None:
            return denied
        if self._on_tier is None:
            return web.json_response({"error": "not supported"}, status=404)
        data = await request.json()
        try:
            result = self._on_tier(str(data.get("tier", "")))
        except ValueError as exc:
            return web.json_response({"error": str(exc)}, status=400)
        return web.json_response({"ok": True, "tier": result})

    async def run(self, stop: asyncio.Event) -> None:
        runner = web.AppRunner(self._app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        log.info("control API on %s:%s", self.host, self.port)
        await stop.wait()
        await runner.cleanup()


def default_token(seed: str) -> str:
    """Deterministic control token derived from a seed (not a secret by itself; set CONTROL_TOKEN in prod)."""
    return hashlib.sha256(seed.encode()).hexdigest()[:32]
