"""Thin async wrapper over the SIG (Super Market) participant REST API.

Retry rules follow the API reference: only 429, 409 REQUEST_IN_FLIGHT, 502 ORDER_STATUS_UNKNOWN,
503 (TX_CONFLICT / SERVICE_UNAVAILABLE) and transport errors are retried, and order placement always
reuses the same idempotency key so a retry can never double-place.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from typing import Any

import aiohttp
import msgspec

from sigbot.core.util import RequestBudget, new_key

from . import models as m

log = logging.getLogger(__name__)

SIG_TIMEOUT_S = 10.0
"""Default total timeout per request while trading: a snipe answered later than this is stale anyway."""
ORDER_MAX_WAIT_S = 2.0
"""An order that would wait longer than this for request budget is not sent: its price would be stale."""
BATCH_TIMEOUT_S = 40.0
"""A batch of resting orders may take SIG this long; the usual 10 s produced 409 REQUEST_IN_FLIGHT retries."""
BATCH_IN_FLIGHT_WAIT_S = 120.0
"""How long place_batch keeps re-asking with the same key while SIG still executes the first attempt."""
BATCH_IN_FLIGHT_POLL_S = 15.0
RETRYABLE_STATUS = frozenset({429, 502, 503})
ENGINE_THROTTLE_PAUSE_S = 5.0
"""Pause after a 429 that carries no ``Retry-After``: the account budget's 429 always says ``Retry-After: 60``
(spec, and measured 2026-09-28), so one without the header is the trading engine throttling order placement
(spec 2026-10-03: "that header may be absent"), and a minute's global pause for it would be far too long."""
RETRYABLE_CODES = frozenset(
    {"RATE_LIMITED", "REQUEST_IN_FLIGHT", "ORDER_STATUS_UNKNOWN", "TX_CONFLICT", "SERVICE_UNAVAILABLE"},
)


class SigApiError(Exception):
    def __init__(
        self,
        status: int,
        code: str,
        message: str = "",
        details: dict[str, Any] | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.details = details
        self.retry_after = retry_after

    @property
    def retryable(self) -> bool:
        return self.status in RETRYABLE_STATUS or self.code in RETRYABLE_CODES

    @property
    def outcome_unknown(self) -> bool:
        """The engine may have accepted the order; reconcile before assuming it failed."""
        return self.code in {"ORDER_STATUS_UNKNOWN", "REQUEST_IN_FLIGHT", "BAD_RESPONSE"} or self.status == 0

    @property
    def not_sent(self) -> bool:
        """Our own request budget refused to send it (``max_wait_s``): nothing reached SIG, not an error."""
        return self.code == "NOT_SENT"


_error_decoder = msgspec.json.Decoder(m.ErrorEnvelope)
_encoder = msgspec.json.Encoder()


def _clean(params: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in params.items():
        if value is None:
            continue
        out[key] = ("true" if value else "false") if isinstance(value, bool) else str(value)
    return out


class SigClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        budget: RequestBudget | None = None,
        timeout: float = SIG_TIMEOUT_S,
        tournament_id: str | None = None,
        observe: Callable[[str, str, float], None] | None = None,
        on_outcome: Callable[[int, str, str], None] | None = None,
    ) -> None:
        """``observe(source, metric, ms)`` receives every HTTP round trip ("sig", "order"|"rest", ms);
        ``on_outcome(status, code, message)`` every attempt's result (status 0: transport error)."""
        self.base_url = base_url.rstrip("/")
        self.tournament_id = tournament_id
        self._api_key = api_key
        self._observe = observe
        self._on_outcome = on_outcome
        # One budget for the whole key: market data can never starve orders/cancels,
        # and a 429 pauses everything until its Retry-After instead of digging a deeper lockout.
        self.budget = budget or RequestBudget()
        self.requests = 0
        self.rate_limited = 0
        self._timeout = aiohttp.ClientTimeout(total=timeout, sock_connect=5)
        self._session: aiohttp.ClientSession | None = None
        self._decoders: dict[Any, msgspec.json.Decoder[Any]] = {}

    async def __aenter__(self) -> SigClient:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def set_timeout(self, total_s: float) -> None:
        """Change the default total timeout for later requests (startup reads get a patient one while SIG
        is degraded; trading calls go back to the short one afterwards)."""
        self._timeout = aiohttp.ClientTimeout(total=total_s, sock_connect=5)

    async def start(self) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                connector=aiohttp.TCPConnector(limit=32, ttl_dns_cache=300),
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Accept": "application/json",
                    "User-Agent": "sigbot/0.1",
                },
            )

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def _decoder[T](self, typ: type[T]) -> msgspec.json.Decoder[T]:
        dec = self._decoders.get(typ)
        if dec is None:
            dec = self._decoders[typ] = msgspec.json.Decoder(typ)
        return dec

    async def request[T](
        self,
        method: str,
        path: str,
        typ: type[T],
        *,
        params: Mapping[str, Any] | None = None,
        body: msgspec.Struct | None = None,
        retries: int = 3,
        ok_statuses: Sequence[int] = (200, 201, 207),
        trade: bool = False,
        retry_rate_limited: bool = True,
        max_wait_s: float | None = None,
        timeout_s: float | None = None,
    ) -> T:
        """``max_wait_s``: raise RATE_LIMITED without sending if the budget would make the first attempt
        wait longer (retries of an already-sent request always go through, to learn its outcome).
        ``timeout_s`` overrides the client's total timeout for this call (batches run long on SIG's side)."""
        if self._session is None:
            await self.start()
        assert self._session is not None
        url = f"{self.base_url}{path}"
        payload = _encoder.encode(body) if body is not None else None
        headers = {"Content-Type": "application/json"} if payload is not None else None
        query = _clean(params) if params else None
        timeout = aiohttp.ClientTimeout(total=timeout_s, sock_connect=5) if timeout_s else self._timeout
        attempt = 0
        write = method not in ("GET", "HEAD")  # SIG budgets writes separately (30/min on a standard key)
        while True:
            if attempt == 0 and max_wait_s is not None:
                wait = self.budget.wait_time(trade, write=write)
                if wait > max_wait_s:
                    raise SigApiError(429, "NOT_SENT", f"request budget needs {wait:.1f}s", retry_after=wait)
            await self.budget.acquire(trade, write=write)
            self.requests += 1
            try:
                sent = time.perf_counter()
                async with self._session.request(
                    method,
                    url,
                    params=query,
                    data=payload,
                    headers=headers,
                    timeout=timeout,
                ) as resp:
                    raw = await resp.read()
                    if self._observe is not None:
                        self._observe(
                            "sig",
                            "order" if trade else "rest",
                            (time.perf_counter() - sent) * 1000,
                        )
                    if resp.status in ok_statuses:
                        if self._on_outcome is not None:
                            self._on_outcome(resp.status, "", "")
                        try:
                            return self._decoder(typ).decode(raw)
                        except msgspec.DecodeError as exc:
                            # SIG accepted the request (an order may rest or have filled) but the body does
                            # not match our model: surface it as an unknown outcome, never as a crash
                            raise SigApiError(
                                resp.status,
                                "BAD_RESPONSE",
                                f"{exc}; body {raw[:300]!r}",
                            ) from exc
                    err = self._error(resp.status, raw, resp.headers.get("Retry-After"))
            except (aiohttp.ClientError, TimeoutError) as exc:
                err = SigApiError(0, "TRANSPORT", repr(exc))
            if self._on_outcome is not None:
                self._on_outcome(err.status, err.code, err.message)
            if err.status == 429:
                self.rate_limited += 1
                pause = err.retry_after if err.retry_after is not None else ENGINE_THROTTLE_PAUSE_S
                self.budget.penalize(pause)
                log.error(
                    "SIG rate limit hit on %s %s (%s); all requests paused %.0fs",
                    method,
                    path,
                    "Retry-After given" if err.retry_after is not None else "no Retry-After: engine throttle",
                    pause,
                )
                if not retry_rate_limited:
                    raise err
            if attempt >= retries or not (err.retryable or err.status == 0):
                raise err
            attempt += 1
            # 429s wait on the shared penalty clock inside budget.acquire() and others back off here.
            delay = 0.0 if err.status == 429 else min(4.0, 0.2 * 2**attempt) * (0.5 + random.random())
            log.warning("sig %s %s failed (%s); retry %d in %.2fs", method, path, err, attempt, delay)
            await asyncio.sleep(delay)

    @staticmethod
    def _error(status: int, raw: bytes, retry_after: str | None) -> SigApiError:
        try:
            info = _error_decoder.decode(raw).error
            code, message, details = info.code, info.message, info.details
        except msgspec.DecodeError:
            code, message, details = f"HTTP_{status}", raw[:300].decode(errors="replace"), None
        ra: float | None = None
        if retry_after:
            try:
                ra = float(retry_after)
            except ValueError:
                ra = None
        return SigApiError(status, code, message, details, ra)

    def _tid(self, tournament_id: str | None) -> str | None:
        return tournament_id if tournament_id is not None else self.tournament_id

    async def account(self) -> m.Account:
        return await self.request("GET", "/account", m.Account)

    async def tournaments(self) -> m.TournamentList:
        return await self.request("GET", "/tournaments", m.TournamentList)

    async def tournament(self, slug: str) -> m.Tournament:
        return await self.request("GET", f"/tournaments/{slug}", m.Tournament)

    async def tournament_markets(
        self,
        slug: str,
        *,
        limit: int = 100,
        cursor: str | None = None,
        status: str | None = None,
        search: str | None = None,
    ) -> m.MarketPage:
        params = {"limit": limit, "cursor": cursor, "status": status, "search": search}
        return await self.request("GET", f"/tournaments/{slug}/markets", m.MarketPage, params=params)

    async def tournament_leaderboard(
        self,
        slug: str,
        *,
        period: str = "all",
        limit: int = 100,
        offset: int = 0,
    ) -> m.Leaderboard:
        params = {"period": period, "limit": limit, "offset": offset}
        return await self.request("GET", f"/tournaments/{slug}/leaderboard", m.Leaderboard, params=params)

    async def leaderboards(
        self,
        *,
        tournament_slug: str | None = None,
        period: str = "all",
        limit: int = 100,
        offset: int = 0,
        sort: str | None = None,
    ) -> m.Leaderboard:
        params = {
            "tournamentSlug": tournament_slug,
            "period": period,
            "limit": limit,
            "offset": offset,
            "sort": sort,
        }
        return await self.request("GET", "/leaderboards", m.Leaderboard, params=params)

    async def smart_score(self, slug: str) -> list[m.SmartScore]:
        return await self.request("GET", f"/tournaments/{slug}/me/smart-score", list[m.SmartScore])

    async def positions(self, slug: str) -> m.Positions:
        return await self.request("GET", f"/tournaments/{slug}/portfolio/positions", m.Positions)

    async def pnl(self, slug: str, period: str = "all") -> m.Pnl:
        return await self.request(
            "GET",
            f"/tournaments/{slug}/portfolio/pnl",
            m.Pnl,
            params={"period": period},
        )

    async def fills(self, slug: str, *, limit: int = 200, cursor: str | None = None) -> m.FillPage:
        params = {"limit": limit, "cursor": cursor}
        return await self.request("GET", f"/tournaments/{slug}/portfolio/fills", m.FillPage, params=params)

    async def settlements(
        self,
        slug: str,
        *,
        limit: int = 200,
        cursor: str | None = None,
    ) -> m.SettlementPage:
        params = {"limit": limit, "cursor": cursor}
        return await self.request(
            "GET",
            f"/tournaments/{slug}/portfolio/settlements",
            m.SettlementPage,
            params=params,
        )

    async def portfolio_history(self, slug: str, interval: str = "1d") -> m.PortfolioHistory:
        return await self.request(
            "GET",
            f"/tournaments/{slug}/portfolio/history",
            m.PortfolioHistory,
            params={"interval": interval},
        )

    async def collateral(self, tournament_id: str | None = None) -> m.CollateralList:
        params = {"tournamentId": self._tid(tournament_id)}
        return await self.request("GET", "/portfolio/collateral", m.CollateralList, params=params)

    async def markets(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        status: str | None = None,
        ids: Sequence[str] | None = None,
        tournament_id: str | None = None,
    ) -> m.MarketPage:
        params = {
            "limit": limit,
            "cursor": cursor,
            "status": status,
            "ids": ",".join(ids) if ids else None,
            "tournamentId": self._tid(tournament_id),
        }
        return await self.request("GET", "/markets", m.MarketPage, params=params)

    async def all_markets(self, *, status: str | None = None) -> list[m.Market]:
        out: list[m.Market] = []
        cursor: str | None = None
        while True:
            page = await self.markets(cursor=cursor, status=status)
            out.extend(page.data)
            if not page.pagination.has_more or not page.pagination.next_cursor:
                return out
            cursor = page.pagination.next_cursor

    async def market(self, market_id: str, tournament_id: str | None = None) -> m.Market:
        params = {"tournamentId": self._tid(tournament_id)}
        return await self.request("GET", f"/markets/{market_id}", m.Market, params=params)

    async def market_nodes(self, market_id: str) -> dict[str, Any]:
        return await self.request("GET", f"/markets/{market_id}/nodes", dict[str, Any])

    async def market_orderbook(
        self,
        market_id: str,
        depth: int = 20,
        tournament_id: str | None = None,
    ) -> m.MarketOrderbook:
        params = {"depth": depth, "tournamentId": self._tid(tournament_id)}
        return await self.request("GET", f"/markets/{market_id}/orderbook", m.MarketOrderbook, params=params)

    async def exchange_orderbook(
        self,
        exchange_id: str,
        depth: int = 20,
        tournament_id: str | None = None,
    ) -> m.ExchangeOrderbook:
        params = {"depth": depth, "tournamentId": self._tid(tournament_id)}
        return await self.request(
            "GET",
            f"/exchanges/{exchange_id}/orderbook",
            m.ExchangeOrderbook,
            params=params,
        )

    async def exchange_price(self, exchange_id: str, tournament_id: str | None = None) -> m.ExchangePrice:
        params = {"tournamentId": self._tid(tournament_id)}
        return await self.request("GET", f"/exchanges/{exchange_id}/price", m.ExchangePrice, params=params)

    async def bulk_prices(
        self,
        exchange_ids: Sequence[str],
        tournament_id: str | None = None,
    ) -> m.BulkPrices:
        """Top of book for up to 100 exchanges per call; larger lists are chunked."""
        tid = self._tid(tournament_id)
        chunks = [exchange_ids[i : i + 100] for i in range(0, len(exchange_ids), 100)]
        pages = await asyncio.gather(
            *(
                self.request(
                    "GET",
                    "/exchanges/prices",
                    m.BulkPrices,
                    params={"ids": ",".join(c), "tournamentId": tid},
                )
                for c in chunks
            ),
        )
        if len(pages) == 1:
            return pages[0]
        return m.BulkPrices(
            data=[p for page in pages for p in page.data],
            missing_ids=[i for page in pages for i in page.missing_ids],
        )

    async def exchange_trades(
        self,
        exchange_id: str,
        *,
        limit: int = 50,
        cursor: str | None = None,
        since: str | None = None,
        tournament_id: str | None = None,
    ) -> m.ExchangeTradePage:
        params = {"limit": limit, "cursor": cursor, "from": since, "tournamentId": self._tid(tournament_id)}
        return await self.request(
            "GET",
            f"/exchanges/{exchange_id}/trades",
            m.ExchangeTradePage,
            params=params,
        )

    async def price_history(
        self,
        exchange_id: str,
        *,
        resolution: str = "1h",
        limit: int = 200,
        tournament_id: str | None = None,
    ) -> dict[str, Any]:
        params = {"resolution": resolution, "limit": limit, "tournamentId": self._tid(tournament_id)}
        return await self.request(
            "GET",
            f"/exchanges/{exchange_id}/price-history",
            dict[str, Any],
            params=params,
        )

    def order(
        self,
        exchange_id: str,
        side: m.Side,
        action: m.Action,
        quantity: int,
        price: float | None,
        *,
        expiration_date: str | None = None,
    ) -> m.OrderRequest:
        """Build an order bound to the current tournament (price=None -> market order)."""
        return m.OrderRequest(
            exchange_id=exchange_id,
            side=side,
            action=action,
            quantity=quantity,
            price=price,
            expiration_date=expiration_date,
            tournament_id=self.tournament_id,
        )

    async def place_order(
        self,
        order: m.OrderRequest,
        idempotency_key: str | None = None,
        *,
        retries: int = 2,
    ) -> m.OrderResult:
        body = m.SingleOrderRequest(
            exchange_id=order.exchange_id,
            side=order.side,
            action=order.action,
            quantity=order.quantity,
            price=order.price,
            expiration_date=order.expiration_date,
            tournament_id=order.tournament_id or self.tournament_id,
            idempotency_key=idempotency_key or new_key("o-"),
        )
        # A rate-limited placement was never accepted, and retrying it 60s later is worthless anway.
        return await self.request(
            "POST",
            "/orders",
            m.OrderResult,
            body=body,
            retries=retries,
            trade=True,
            retry_rate_limited=False,
            max_wait_s=ORDER_MAX_WAIT_S,
        )

    async def place_batch(
        self,
        orders: Sequence[m.OrderRequest],
        idempotency_key: str | None = None,
        *,
        retries: int = 2,
        in_flight_wait_s: float = BATCH_IN_FLIGHT_WAIT_S,
        max_wait_s: float | None = None,
    ) -> m.BatchResult:
        """SIG places a batch's orders one by one and the whole call can outlast our usual timeout under
        load, so it gets BATCH_TIMEOUT_S. A retry that finds the first attempt still executing (409
        REQUEST_IN_FLIGHT) keeps asking with the same key, up to ``in_flight_wait_s``, until SIG replays the
        stored result: giving up there would leave orders resting that nobody tracks (2026-10-01)."""
        body = m.BatchOrderRequest(idempotency_key=idempotency_key or new_key("b-"), orders=list(orders))
        deadline = time.monotonic() + in_flight_wait_s
        while True:
            try:
                return await self.request(
                    "POST",
                    "/orders/batch",
                    m.BatchResult,
                    body=body,
                    retries=retries,
                    ok_statuses=(200, 207, 422),
                    trade=True,
                    retry_rate_limited=False,
                    timeout_s=BATCH_TIMEOUT_S,
                    max_wait_s=max_wait_s,
                )
            except SigApiError as exc:
                if exc.code != "REQUEST_IN_FLIGHT" or time.monotonic() >= deadline:
                    raise
                log.warning(
                    "batch %s still executing at SIG; asking again in %.0fs",
                    body.idempotency_key,
                    BATCH_IN_FLIGHT_POLL_S,
                )
                await asyncio.sleep(BATCH_IN_FLIGHT_POLL_S)

    async def place_multi_leg(
        self,
        legs: Sequence[m.OrderRequest],
        idempotency_key: str | None = None,
        *,
        relationship_constraint: str | None = None,
        retries: int = 2,
    ) -> m.MultiLegResult:
        body = m.MultiLegRequest(
            idempotency_key=idempotency_key or new_key("ml-"),
            legs=list(legs),
            relationship_constraint=relationship_constraint,
        )
        return await self.request(
            "POST",
            "/orders/multi-leg",
            m.MultiLegResult,
            body=body,
            retries=retries,
            trade=True,
            retry_rate_limited=False,
            max_wait_s=ORDER_MAX_WAIT_S,
        )

    async def list_orders(
        self,
        *,
        status: m.OrderStatusFilter = m.OrderStatusFilter.OPEN,
        exchange_id: str | None = None,
        market_id: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
        tournament_id: str | None = None,
    ) -> m.OrderPage:
        params = {
            "status": status.value,
            "exchangeId": exchange_id,
            "marketId": market_id,
            "limit": limit,
            "cursor": cursor,
            "tournamentId": self._tid(tournament_id),
        }
        return await self.request("GET", "/orders", m.OrderPage, params=params)

    async def iter_orders(self, **kwargs: Any) -> AsyncIterator[m.Order]:
        cursor: str | None = None
        while True:
            page = await self.list_orders(cursor=cursor, **kwargs)
            for order in page.data:
                yield order
            if not page.pagination.has_more or not page.pagination.next_cursor:
                return
            cursor = page.pagination.next_cursor

    async def get_order(self, order_id: int) -> m.Order:
        return await self.request("GET", f"/orders/{order_id}", m.Order)

    async def order_fills(self, order_id: int) -> m.OrderFills:
        return await self.request("GET", f"/orders/{order_id}/fills", m.OrderFills)

    async def cancel_order(
        self,
        order_id: int,
        *,
        retries: int = 3,
        max_wait_s: float | None = None,
    ) -> m.CancelResult:
        return await self.request(
            "DELETE",
            f"/orders/{order_id}",
            m.CancelResult,
            retries=retries,
            trade=True,
            max_wait_s=max_wait_s,
        )

    async def cancel_all(
        self,
        *,
        exchange_id: str | None = None,
        market_id: str | None = None,
        tournament_id: str | None = None,
        retries: int = 5,
    ) -> m.CancelAllResult:
        body = m.CancelAllBody(
            exchange_id=exchange_id,
            market_id=market_id,
            tournament_id=self._tid(tournament_id),
        )
        return await self.request(
            "POST",
            "/orders/cancel-all",
            m.CancelAllResult,
            body=body,
            retries=retries,
            ok_statuses=(200, 207),
            trade=True,
        )

    async def relationships(
        self,
        *,
        market_id: str | None = None,
        limit: int = 200,
        tournament_id: str | None = None,
    ) -> m.RelationshipPage:
        params = {"marketId": market_id, "limit": limit, "tournamentId": self._tid(tournament_id)}
        return await self.request("GET", "/relationships", m.RelationshipPage, params=params)

    async def constraints(
        self,
        *,
        violations_only: bool = True,
        min_violation: float = 0.0,
        tournament_id: str | None = None,
    ) -> m.ConstraintList:
        params = {
            "violationsOnly": violations_only,
            "minViolation": min_violation,
            "tournamentId": self._tid(tournament_id),
        }
        return await self.request("GET", "/relationships/constraints", m.ConstraintList, params=params)

    async def realtime_token(self) -> m.RealtimeToken:
        return await self.request("POST", "/realtime/token", m.RealtimeToken)
