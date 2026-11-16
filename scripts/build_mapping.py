"""Build sig-api/mapping.json: SIG markets <-> Polymarket YES tokens <-> Kalshi tickers.

Kalshi markets are assigned to races by parsing their TITLE (never the series ticker: e.g. SENATELA-26 is
actually the Kentucky race). Read-only everywhere.

Usage: set -a && . ./deploy/.env && set +a && uv run python scripts/build_mapping.py
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import aiohttp
import msgspec

from sigbot.core.util import TokenBucket
from sigbot.market.mapping import (
    STATES,
    Kind,
    Mapping,
    Outcome,
    Party,
    Race,
    Unlisted,
    parse_sig_title,
    race_key,
)
from sigbot.sigapi import SigClient

GAMMA = "https://gamma-api.polymarket.com"
KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
UA = {"User-Agent": "Mozilla/5.0 (sigbot mapping builder)"}
UNLISTED_EXHAUSTIVE_THRESHOLD = 0.01

# Polymarket AK markets list candidates without party tags.
PM_PARTY_OVERRIDES = {
    "Mary Peltola": Party.D,
    "Sen. Dan Sullivan": Party.R,
    "Daniel J. Sullivan Jr.": Party.R,
    "Jonathan Kreiss-Tomkins": Party.D,
    "Tom Begich": Party.D,
    "Bernadette Wilson": Party.R,
    "David Bronson": Party.R,
    "Shelley Hughes": Party.R,
    "Treg Taylor": Party.R,
    "Edna DeVries": Party.R,
    "Lisa Murkowski": Party.R,
}
PLACEHOLDER = re.compile(r"^(Person|Party) [A-Z]$|^Other$")


@dataclass
class ExtOutcome:
    party: Party | None
    name: str
    ids: list[str] = field(default_factory=list)
    bid: float = 0.0
    ask: float = 1.0

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2


def pm_party(name: str) -> Party | None:
    if name in PM_PARTY_OVERRIDES:
        return PM_PARTY_OVERRIDES[name]
    tag = re.search(r"\((D|R|I)\)\s*$", name)
    if tag:
        return Party(tag.group(1))
    low = name.lower()
    if low.startswith("democrat"):
        return Party.D
    if low.startswith("republican"):
        return Party.R
    if low.startswith("independent"):
        return Party.I
    return None


def pm_race(title: str) -> str | None:
    title = title.strip()
    if title == "Which party will win the House in 2026?":
        return race_key(Kind.CTRL, chamber="HOUSE")
    if title == "Which party will win the Senate in 2026?":
        return race_key(Kind.CTRL, chamber="SENATE")
    if mt := re.match(r"^([A-Z]{2})-(\d{1,2}) House Election Winner$", title):
        return race_key(Kind.HOUSE, mt.group(1), int(mt.group(2)))
    if (mt := re.match(r"^(.+?) Senate Election Winner$", title)) and mt.group(1) in STATES:
        return race_key(Kind.SEN, STATES[mt.group(1)])
    if (mt := re.match(r"^(.+?) Governor (?:Election )?Winner(?: 2026)?$", title)) and mt.group(1) in STATES:
        return race_key(Kind.GOV, STATES[mt.group(1)])
    return None


def kalshi_race(title: str) -> str | None:
    if mt := re.search(r"win the (?:U\.S\. )?(House|Senate) in 2026", title):
        return race_key(Kind.CTRL, chamber=mt.group(1).upper())
    if (mt := re.search(r"Senate race in (.+?)\?", title)) and mt.group(1) in STATES:
        return race_key(Kind.SEN, STATES[mt.group(1)])
    if (mt := re.search(r"governorship in (.+?)\??$", title)) and mt.group(1).strip() in STATES:
        return race_key(Kind.GOV, STATES[mt.group(1).strip()])
    if mt := re.search(r"House race for ([A-Z]{2})-(\d{1,2})", title):
        return race_key(Kind.HOUSE, mt.group(1), int(mt.group(2)))
    return None


def kalshi_party(ticker: str, title: str) -> Party:
    suffix = ticker.rsplit("-", 1)[-1]
    if suffix == "D" or re.search(r"\bDemocrat", title):
        return Party.D
    if suffix == "R" or re.search(r"\bRepublican", title):
        return Party.R
    return Party.I


def kprice(market: dict, name: str) -> float | None:
    value = market.get(f"{name}_dollars")
    if value not in (None, ""):
        return float(value)
    value = market.get(name)
    return None if value is None else float(value) / 100


async def get_json(session: aiohttp.ClientSession, url: str, bucket: TokenBucket, params: dict | None = None):
    for attempt in range(5):
        await bucket.acquire()
        async with session.get(url, params=params, headers=UA) as resp:
            if resp.status == 429:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            if resp.status == 404:
                return None
            resp.raise_for_status()
            return await resp.json(content_type=None)
    raise RuntimeError(f"rate limited: {url}")


async def fetch_polymarket(session: aiohttp.ClientSession) -> dict[str, list[ExtOutcome]]:
    bucket = TokenBucket(3, 3)
    events: list[dict] = []
    offset = 0
    while True:
        page = await get_json(
            session,
            f"{GAMMA}/events",
            bucket,
            {"limit": 100, "offset": offset, "closed": "false", "tag_slug": "midterms"},
        )
        events.extend(page or [])
        if not page or len(page) < 100:
            break
        offset += 100
    best: dict[str, tuple[float, str, list[ExtOutcome]]] = {}
    for ev in events:
        key = pm_race(ev.get("title", ""))
        if key is None:
            continue
        outs: list[ExtOutcome] = []
        for mk in ev.get("markets", []):
            name = mk.get("groupItemTitle") or mk.get("question") or ""
            if (
                mk.get("closed")
                or not mk.get("active")
                or PLACEHOLDER.match(name)
                or not mk.get("clobTokenIds")
            ):
                continue
            tokens = json.loads(mk["clobTokenIds"])
            bid, ask = mk.get("bestBid"), mk.get("bestAsk")
            outs.append(ExtOutcome(pm_party(name), name, [tokens[0]], float(bid or 0), float(ask or 1)))
        liquidity = float(ev.get("liquidity") or 0)
        if outs and (key not in best or liquidity > best[key][0]):
            best[key] = (liquidity, ev["slug"], outs)
    for _, slug, outs in best.values():
        for o in outs:
            o.ids.insert(0, f"event:{slug}")
    return {key: outs for key, (_, _, outs) in best.items()}


async def fetch_kalshi(session: aiohttp.ClientSession, sig_keys: set[str]) -> dict[str, list[ExtOutcome]]:
    bucket = TokenBucket(6, 6)
    series: list[str] = []
    cursor = ""
    while True:
        params = {"category": "Elections"} | ({"cursor": cursor} if cursor else {})
        page = await get_json(session, f"{KALSHI}/series", bucket, params)
        series.extend(s["ticker"] for s in page.get("series", []))
        cursor = page.get("cursor") or ""
        if not cursor:
            break
    house = {k.split("-", 1)[1].replace("-", "") for k in sig_keys if k.startswith("HOUSE-")}
    house_series = {f"{p}HOUSE{h[:2]}{int(h[2:])}" for h in house for p in ("", "KX")}
    wanted = [
        s
        for s in series
        if re.fullmatch(r"(KX)?(SENATE[A-Z]{2}|GOVPARTY[A-Z]{2}|CONTROLS|CONTROLH)", s) or s in house_series
    ]
    results = await asyncio.gather(
        *(
            get_json(
                session,
                f"{KALSHI}/markets",
                bucket,
                {"series_ticker": s, "status": "open", "limit": 100},
            )
            for s in wanted
        ),
    )
    by_race: dict[str, dict[str, list[ExtOutcome]]] = defaultdict(lambda: defaultdict(list))
    for res in results:
        for mk in (res or {}).get("markets", []):
            if not re.search(r"-(26|2026)$", mk.get("event_ticker", "")):
                continue
            key = kalshi_race(mk.get("title", ""))
            if key is None:
                continue
            bid, ask = kprice(mk, "yes_bid"), kprice(mk, "yes_ask")
            party = kalshi_party(mk["ticker"], mk.get("title", ""))
            by_race[key][mk["event_ticker"]].append(
                ExtOutcome(
                    party,
                    mk.get("yes_sub_title") or mk["ticker"],
                    [mk["ticker"]],
                    bid or 0.0,
                    ask if ask not in (None, 0.0) else 1.0,
                ),
            )
    out: dict[str, list[ExtOutcome]] = {}
    for key, events in by_race.items():
        event, outs = max(events.items(), key=lambda kv: len(kv[1]))
        for o in outs:
            o.ids.insert(0, f"event:{event}")
        out[key] = outs
    return out


def race_flags(
    kind: Kind,
    state: str | None,
    parties: set[Party],
    unlisted: list[Unlisted],
) -> tuple[str, ...]:
    flags: list[str] = []
    if kind is Kind.CTRL:
        flags.append("manual_settle")
    if state == "AK" or (state == "ME" and kind in (Kind.SEN, Kind.HOUSE)):
        flags.append("rcv")
    if state == "GA" and kind in (Kind.SEN, Kind.GOV):
        flags.append("runoff")
    if state == "NE" and kind is Kind.SEN and Party.I in parties:
        flags.append("label_risk")
    if unlisted:
        flags.append("unlisted_indep")
    return tuple(flags)


def write_mapping(path: Path, mapping: Mapping) -> None:
    path.parent.mkdir(exist_ok=True)
    path.write_bytes(msgspec.json.format(msgspec.json.encode(mapping), indent=1))


async def main() -> int:
    slug = os.environ.get("TOURNAMENT_SLUG", "midterm-elections")
    async with SigClient(
        os.environ.get("SIG_BASE_URL", "https://sig.thesuper.market/api/v1"),
        os.environ["API_KEY"],
    ) as sig:
        markets = await sig.all_markets()
    sig_races: dict[str, dict] = {}
    for mk in markets:
        parsed = parse_sig_title(mk.title)
        if parsed is None or not mk.exchanges:
            print(f"WARN unparsed SIG market {mk.id}: {mk.title}")
            continue
        race = sig_races.setdefault(parsed.key, {"parsed": parsed, "outcomes": {}})
        race["outcomes"][parsed.party] = (mk.id, mk.exchanges[0].id, mk.title)

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        pm, kalshi = await asyncio.gather(fetch_polymarket(session), fetch_kalshi(session, set(sig_races)))

    races: list[Race] = []
    warnings: list[str] = []
    for key, info in sorted(sig_races.items()):
        parsed = info["parsed"]
        pm_outs = pm.get(key, [])
        k_outs = kalshi.get(key, [])
        pm_event = next((o.ids[0].removeprefix("event:") for o in pm_outs), None)
        k_event = next((o.ids[0].removeprefix("event:") for o in k_outs), None)
        for o in pm_outs:
            if o.party is None and o.ask > 0.02:
                warnings.append(f"{key}: Polymarket outcome without party: {o.name} ({o.bid}/{o.ask})")
        parties = set(info["outcomes"])
        outcomes: list[Outcome] = []
        for party, (market_id, exchange_id, title) in sorted(info["outcomes"].items()):
            pms = [o for o in pm_outs if o.party == party]
            ks = [o for o in k_outs if o.party == party]
            outcomes.append(
                Outcome(
                    party=party,
                    sig_market_id=market_id,
                    sig_exchange_id=exchange_id,
                    sig_title=title,
                    pm_tokens=tuple(o.ids[-1] for o in pms),
                    pm_names=tuple(o.name for o in pms),
                    kalshi_tickers=tuple(o.ids[-1] for o in ks),
                    kalshi_names=tuple(o.name for o in ks),
                ),
            )
            if not pms and not ks:
                warnings.append(f"{key}/{party}: NO external source")
        unlisted: list[Unlisted] = []
        extra_prob = 0.0
        for party in {o.party for o in pm_outs + k_outs if o.party is not None} - parties:
            pms = [o for o in pm_outs if o.party == party]
            ks = [o for o in k_outs if o.party == party]
            mids = [sum(o.mid for o in pms)] if pms else []
            mids += [sum(o.mid for o in ks)] if ks else []
            prob = max(mids) if mids else 0.0
            if prob >= 0.005:
                unlisted.append(
                    Unlisted(
                        label=f"{party.value}:" + "/".join(o.name for o in pms + ks),
                        pm_tokens=tuple(o.ids[-1] for o in pms),
                        kalshi_tickers=tuple(o.ids[-1] for o in ks),
                    ),
                )
                extra_prob += prob
        exhaustive = extra_prob < UNLISTED_EXHAUSTIVE_THRESHOLD
        races.append(
            Race(
                key=key,
                kind=parsed.kind,
                name=parsed.name,
                outcomes=tuple(outcomes),
                exhaustive=exhaustive,
                state=parsed.state,
                district=parsed.district,
                flags=race_flags(parsed.kind, parsed.state, parties, unlisted if not exhaustive else []),
                unlisted=tuple(unlisted),
                pm_event=pm_event,
                kalshi_event=k_event,
            ),
        )

    mapping = Mapping(
        generated_at=datetime.now(UTC).isoformat(timespec="seconds"),
        tournament_slug=slug,
        races=races,
    )
    out = Path("sig-api/mapping.json")
    await asyncio.to_thread(write_mapping, out, mapping)

    n_out = sum(len(r.outcomes) for r in races)
    n_pm = sum(1 for r in races for o in r.outcomes if o.pm_tokens)
    n_k = sum(1 for r in races for o in r.outcomes if o.kalshi_tickers)
    print(f"wrote {out}: {len(races)} races, {n_out} SIG outcomes; with Polymarket {n_pm}, with Kalshi {n_k}")
    print("non-exhaustive:", [r.key for r in races if not r.exhaustive])
    print("flagged:", {r.key: r.flags for r in races if r.flags})
    for w in warnings:
        print("WARN", w)
    return 1 if any("NO external source" in w for w in warnings) else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
