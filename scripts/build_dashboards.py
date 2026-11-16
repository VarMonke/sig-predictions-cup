"""Generate the Grafana dashboards (deploy/grafana/.../json/*.json) from compact panel definitions.

    uv run python scripts/build_dashboards.py                   # write the JSON files
    uv run python scripts/build_dashboards.py --check DSN       # also run every panel query on a DB

--check substitutes Grafana's macros with plain SQL and executes each query, so a broken query
fails here instead of showing up as an empty panel.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from pathlib import Path
from typing import Any

OUT = Path("deploy/grafana/provisioning/dashboards/json")
DS = {"uid": "sigbot"}
RED_GREEN = [{"color": "red", "value": None}, {"color": "green", "value": 0}]
LATEST = "(SELECT max(ts) FROM equity)"

type Panel = dict[str, Any]


def target(sql: str, fmt: str = "table", ref: str = "A") -> dict[str, Any]:
    return {
        "refId": ref,
        "datasource": DS,
        "editorMode": "code",
        "format": fmt,
        "rawQuery": True,
        "rawSql": sql,
    }


def stat(
    title: str,
    sql: str,
    *,
    unit: str = "none",
    decimals: int | None = 0,
    steps: list[dict[str, Any]] | None = None,
    mappings: list[dict[str, Any]] | None = None,
    no_value: str = "-",
) -> Panel:
    return {
        "type": "stat",
        "title": title,
        "targets": [target(sql)],
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "colorMode": "value",
            "graphMode": "none",
            "textMode": "value",
            "justifyMode": "center",
        },
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "decimals": decimals,
                "noValue": no_value,
                "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": steps or [{"color": "text", "value": None}]},
                "mappings": mappings or [],
            },
            "overrides": [],
        },
    }


def timeseries(
    title: str,
    *sqls: str,
    unit: str = "none",
    step: bool = False,
    as_bars: bool = False,
    fill_opacity: int | None = None,
    overrides: list[dict[str, Any]] | None = None,
) -> Panel:
    return {
        "type": "timeseries",
        "title": title,
        "targets": [target(sql, "time_series", chr(65 + i)) for i, sql in enumerate(sqls)],
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi", "sort": "none"},
        },
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {
                    "drawStyle": "bars" if as_bars else "line",
                    "lineWidth": 2,
                    "fillOpacity": fill_opacity if fill_opacity is not None else (70 if as_bars else 6),
                    "showPoints": "never",
                    "spanNulls": not as_bars,
                    "lineInterpolation": "stepAfter" if step else "linear",
                },
            },
            "overrides": overrides or [],
        },
    }


def series(name: str, props: dict[str, Any]) -> dict[str, Any]:
    """Override for one series by name; keys are Grafana property ids ("color", "custom.fillBelowTo", ...)."""
    return {
        "matcher": {"id": "byName", "options": name},
        "properties": [{"id": k, "value": v} for k, v in props.items()],
    }


def color(hex_color: str) -> dict[str, str]:
    return {"mode": "fixed", "fixedColor": hex_color}


def latency_sql(source: str, metric: str, label: str, *, p99: bool = True) -> str:
    cols = f'p50_ms AS "{label} p50"' + (f', p99_ms AS "{label} p99"' if p99 else "")
    return (
        f"SELECT ts AS time, {cols} FROM latency "
        f"WHERE source = '{source}' AND metric = '{metric}' AND $__timeFilter(ts) ORDER BY 1"
    )


def pnl_cells(*fields: str) -> list[dict[str, Any]]:
    return [
        {
            "matcher": {"id": "byName", "options": name},
            "properties": [
                {"id": "custom.cellOptions", "value": {"type": "color-text"}},
                {"id": "color", "value": {"mode": "thresholds"}},
                {"id": "thresholds", "value": {"mode": "absolute", "steps": RED_GREEN}},
            ],
        }
        for name in fields
    ]


LEVEL_COLORS = {"CRITICAL": "red", "ERROR": "red", "WARNING": "orange", "INFO": "text"}


def level_cells(field: str) -> dict[str, Any]:
    mapping = {level: {"color": color, "index": i} for i, (level, color) in enumerate(LEVEL_COLORS.items())}
    return {
        "matcher": {"id": "byName", "options": field},
        "properties": [
            {"id": "custom.cellOptions", "value": {"type": "color-text"}},
            {"id": "mappings", "value": [{"type": "value", "options": mapping}]},
            {"id": "custom.width", "value": 90},
        ],
    }


def table(title: str, sql: str, *, colored: tuple[str, ...] = (), levels: str | None = None) -> Panel:
    overrides = pnl_cells(*colored) + ([level_cells(levels)] if levels else [])
    return {
        "type": "table",
        "title": title,
        "targets": [target(sql)],
        "options": {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}},
        "fieldConfig": {"defaults": {"custom": {"align": "auto"}}, "overrides": overrides},
    }


def bars(title: str, sql: str, x_field: str) -> Panel:
    return {
        "type": "barchart",
        "title": title,
        "targets": [target(sql)],
        "options": {
            "orientation": "horizontal",
            "xField": x_field,
            "showValue": "auto",
            "barWidth": 0.8,
            "groupWidth": 0.7,
            "legend": {"showLegend": False, "displayMode": "list", "placement": "bottom"},
            "tooltip": {"mode": "single", "sort": "none"},
        },
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": RED_GREEN},
            },
            "overrides": [],
        },
    }


def layout(rows: list[tuple[int, list[tuple[int, Panel]]]]) -> list[Panel]:
    """rows = [(height, [(width, panel), ...]), ...] on Grafana's 24-column grid."""
    panels, y, pid = [], 0, 1
    for height, row in rows:
        x = 0
        for width, panel in row:
            panels.append(
                panel | {"id": pid, "datasource": DS, "gridPos": {"x": x, "y": y, "w": width, "h": height}},
            )
            x, pid = x + width, pid + 1
        y += height
    return panels


def dashboard(uid: str, title: str, rows: list[tuple[int, list[tuple[int, Panel]]]], **extra: Any) -> dict:
    return {
        "uid": uid,
        "title": title,
        "tags": ["sigbot"],
        "timezone": "utc",
        "schemaVersion": 39,
        "version": 1,
        "editable": True,
        "refresh": "30s",
        "time": {"from": "now-24h", "to": "now"},
        "links": [{"type": "dashboards", "tags": ["sigbot"], "asDropdown": False, "title": "Sigbot"}],
        "annotations": {"list": []},
        "templating": {"list": []},
        "panels": layout(rows),
    } | extra


def latest(column: str) -> str:
    return f"SELECT {column} FROM equity WHERE {column} IS NOT NULL ORDER BY ts DESC LIMIT 1"


RACE_PNL = f"""SELECT coalesce(race, exchange_id) AS "Race",
  round(sum(realized_pnl + unrealized_pnl)::numeric, 2) AS "P&L"
FROM positions WHERE ts = {LATEST}
GROUP BY 1 ORDER BY 2 {{order}} LIMIT 12"""

PNL = dashboard(
    "sigbot-pnl",
    "Sigbot - P&L",
    [
        (
            4,
            [
                (4, stat("P&L (SUSQ)", latest("pnl"), steps=RED_GREEN)),
                (
                    4,
                    stat(
                        "P&L %",
                        latest("pnl / nullif(initial_balance, 0)"),
                        unit="percentunit",
                        decimals=2,
                        steps=RED_GREEN,
                    ),
                ),
                (4, stat("Realized", latest("realized_pnl"), steps=RED_GREEN)),
                (4, stat("Unrealized", latest("unrealized_pnl"), steps=RED_GREEN)),
                (4, stat("Equity (fair marks)", latest("equity_fair"))),
                (4, stat("Cash", latest("cash"))),
            ],
        ),
        (
            4,
            [
                (3, stat("Deployed", latest("deployed"))),
                (
                    3,
                    stat(
                        "Open positions",
                        f"SELECT count(*) FROM positions WHERE ts = {LATEST} AND quantity <> 0",
                    ),
                ),
                (
                    4,
                    stat(
                        "Leaderboard rank",
                        "SELECT rank FROM standing WHERE rank IS NOT NULL ORDER BY ts DESC LIMIT 1",
                        no_value="unranked",
                    ),
                ),
                (3, stat("Capital tier", latest("tier"))),
                (
                    4,
                    stat(
                        "SIG market",
                        latest(
                            "CASE sig_state WHEN 'OPEN' THEN 1 WHEN 'NOT STARTED' THEN 0 "
                            "WHEN 'ENDED' THEN 2 END",
                        ),
                        mappings=[
                            {
                                "type": "value",
                                "options": {
                                    "0": {"text": "NOT OPEN YET", "color": "blue"},
                                    "1": {"text": "OPEN", "color": "green"},
                                    "2": {"text": "ENDED", "color": "text"},
                                },
                            },
                        ],
                    ),
                ),
                (
                    4,
                    stat(
                        "Trading",
                        latest("CASE WHEN halted THEN 1 ELSE 0 END"),
                        mappings=[
                            {
                                "type": "value",
                                "options": {
                                    "0": {"text": "running", "color": "green"},
                                    "1": {"text": "HALTED", "color": "red"},
                                },
                            },
                        ],
                    ),
                ),
                (
                    3,
                    stat(
                        "Fills today",
                        "SELECT count(*) FROM orders WHERE qty_traded > 0 AND ts >= date_trunc('day', now())",
                    ),
                ),
            ],
        ),
        (
            9,
            [
                (
                    12,
                    timeseries(
                        "Equity",
                        'SELECT ts AS time, equity_fair AS "Equity (fair marks)", '
                        'equity_sig AS "Equity (SIG marks)" FROM equity WHERE $__timeFilter(ts) ORDER BY 1',
                    ),
                ),
                (
                    12,
                    timeseries(
                        "P&L",
                        'SELECT ts AS time, pnl AS "P&L", realized_pnl AS "Realized", '
                        'unrealized_pnl AS "Unrealized" FROM equity '
                        "WHERE $__timeFilter(ts) AND pnl IS NOT NULL ORDER BY 1",
                    ),
                ),
            ],
        ),
        (
            8,
            [
                (
                    8,
                    timeseries(
                        "SIG latency (ms)",
                        latency_sql("sig", "rest", "Data request"),
                        latency_sql("sig", "order", "Order", p99=False),
                        latency_sql("sig", "feed", "Live feed delay"),
                        unit="ms",
                    ),
                ),
                (
                    8,
                    timeseries(
                        "Polymarket feed delay (ms)",
                        latency_sql("polymarket", "feed", "Feed delay"),
                        unit="ms",
                    ),
                ),
                (
                    8,
                    timeseries(
                        "Kalshi poll round trip (ms)",
                        latency_sql("kalshi", "rest", "Poll"),
                        unit="ms",
                    ),
                ),
            ],
        ),
        (
            6,
            [
                (
                    4,
                    stat(
                        "Lost to speed today",
                        "SELECT count(*) FROM lost_races WHERE ts >= date_trunc('day', now())",
                    ),
                ),
                (
                    4,
                    stat(
                        "Snipe shares filled today",
                        "SELECT sum(qty_traded) / nullif(sum(quantity), 0) FROM orders "
                        "WHERE strategy = 'snipe' AND NOT shadow AND ts >= date_trunc('day', now())",
                        unit="percentunit",
                        steps=[
                            {"color": "red", "value": None},
                            {"color": "orange", "value": 0.5},
                            {"color": "green", "value": 0.8},
                        ],
                    ),
                ),
                (
                    16,
                    timeseries(
                        "Orders lost to faster traders (per 15 min)",
                        "SELECT $__timeGroupAlias(ts, '15m'), count(*) AS \"Lost races\" "
                        "FROM lost_races WHERE $__timeFilter(ts) GROUP BY 1 ORDER BY 1",
                        as_bars=True,
                    ),
                ),
            ],
        ),
        (
            10,
            [
                (12, bars("Top paying races", RACE_PNL.format(order="DESC"), "Race")),
                (12, bars("Worst races", RACE_PNL.format(order="ASC"), "Race")),
            ],
        ),
        (
            10,
            [
                (
                    16,
                    table(
                        "Open positions",
                        f"""SELECT race AS "Race", party AS "Party",
  CASE WHEN quantity > 0 THEN 'YES' ELSE 'NO' END AS "Side", abs(quantity) AS "Shares",
  round(avg_cost::numeric, 3) AS "Avg cost", round(fair::numeric, 3) AS "Fair (YES)",
  round(sig_price::numeric, 3) AS "SIG mid (YES)",
  round(unrealized_pnl::numeric, 2) AS "Unrealized", round(realized_pnl::numeric, 2) AS "Realized"
FROM positions WHERE ts = {LATEST} AND quantity <> 0
ORDER BY abs(unrealized_pnl) DESC""",
                        colored=("Unrealized", "Realized"),
                    ),
                ),
                (
                    8,
                    table(
                        "Fills by trade reason",
                        """SELECT trade_reason AS "Reason", count(*) AS "Fills",
  round(sum(total_cost)::numeric, 0) AS "Notional"
FROM orders WHERE qty_traded > 0 AND $__timeFilter(ts)
GROUP BY 1 ORDER BY 3 DESC""",
                    ),
                ),
            ],
        ),
        (
            10,
            [
                (
                    16,
                    table(
                        "Recent fills",
                        """SELECT o.ts AS "Time", o.strategy AS "Strategy", o.trade_reason AS "Reason",
  r.race AS "Race", r.party AS "Party", o.side AS "Side", o.price AS "Limit",
  round(o.fill_price::numeric, 3) AS "Fill", o.qty_traded AS "Shares",
  round(o.total_cost::numeric, 2) AS "Cost", CASE WHEN o.shadow THEN 'shadow' ELSE 'live' END AS "Mode"
FROM orders o LEFT JOIN races r ON r.exchange_id = o.exchange_id
WHERE o.qty_traded > 0 AND $__timeFilter(o.ts)
ORDER BY o.ts DESC LIMIT 100""",
                    ),
                ),
                (
                    8,
                    table(
                        "Leaderboard (top 20)",
                        """SELECT rank AS "Rank", coalesce(username, left(profile_id, 8)) AS "Trader",
  round(pnl::numeric, 0) AS "P&L", round(roi::numeric, 4) AS "ROI", trades AS "Trades"
FROM leaderboard WHERE ts = (SELECT max(ts) FROM leaderboard)
ORDER BY rank LIMIT 20""",
                        colored=("P&L",),
                    ),
                ),
            ],
        ),
        (
            7,
            [
                (
                    12,
                    table(
                        "Smart Score",
                        """SELECT market_type AS "Market", round(score::numeric, 3) AS "Score",
  round(decayed::numeric, 3) AS "Decayed", rank AS "Rank", total AS "Of", elite AS "Elite"
FROM smart_score WHERE ts = (SELECT max(ts) FROM smart_score)
ORDER BY market_type""",
                    ),
                ),
                (
                    12,
                    table(
                        "Capital tier changes",
                        'SELECT ts AS "Time", from_tier AS "From", to_tier AS "To", reason AS "Why" '
                        "FROM ladder ORDER BY ts DESC LIMIT 20",
                    ),
                ),
            ],
        ),
        (
            8,
            [
                (
                    24,
                    table(
                        "Latest warnings and errors (full log: Sigbot - Logs)",
                        """SELECT ts AS "Time", level AS "Level", logger AS "Source", message AS "Message"
FROM logs WHERE level IN ('WARNING', 'ERROR', 'CRITICAL') AND $__timeFilter(ts)
ORDER BY ts DESC LIMIT 20""",
                        levels="Level",
                    ),
                ),
            ],
        ),
    ],
)

LEVEL_VAR = {
    "name": "level",
    "label": "Level",
    "type": "custom",
    "query": "INFO,WARNING,ERROR,CRITICAL",
    "multi": True,
    "includeAll": True,
    "current": {"text": ["All"], "value": ["$__all"]},
}

LOGS = dashboard(
    "sigbot-logs",
    "Sigbot - Logs",
    [
        (
            6,
            [
                (
                    24,
                    timeseries(
                        "Log lines per minute",
                        "SELECT $__timeGroupAlias(ts, '1m'), level AS metric, count(*) AS value "
                        "FROM logs WHERE $__timeFilter(ts) GROUP BY 1, 2 ORDER BY 1",
                    ),
                ),
            ],
        ),
        (
            22,
            [
                (
                    24,
                    table(
                        "Bot log (newest first)",
                        """SELECT ts AS "Time", level AS "Level", logger AS "Source", message AS "Message"
FROM logs WHERE level IN ($level) AND $__timeFilter(ts)
ORDER BY ts DESC LIMIT 500""",
                        levels="Level",
                    ),
                ),
            ],
        ),
    ],
    refresh="10s",
    time={"from": "now-6h", "to": "now"},
    templating={"list": [LEVEL_VAR]},
)

MARKET_LIST = "SELECT exchange_id AS __value, race || ' ' || party AS __text FROM races ORDER BY race, party"
EXCHANGE_VAR = {
    "name": "exchange",
    "label": "Market",
    "type": "query",
    "datasource": DS,
    "query": MARKET_LIST,
    "definition": MARKET_LIST,
    "refresh": 1,
    "sort": 0,
    "includeAll": False,
    "multi": False,
}


def tob_span(columns: str) -> str:
    """SIG top of book is stored only when it changes: carry the last price into the window and up to now."""
    return f"""SELECT time, {columns} FROM (
  (SELECT $__timeFrom()::timestamptz AS time, bid, ask FROM sig_tob
   WHERE exchange_id = '$exchange' AND ts < $__timeFrom()::timestamptz ORDER BY ts DESC LIMIT 1)
  UNION ALL
  SELECT ts, bid, ask FROM sig_tob WHERE exchange_id = '$exchange' AND $__timeFilter(ts)
  UNION ALL
  (SELECT least(now(), $__timeTo()::timestamptz), bid, ask FROM sig_tob
   WHERE exchange_id = '$exchange' AND ts <= $__timeTo()::timestamptz ORDER BY ts DESC LIMIT 1)
) AS span ORDER BY 1"""


OUR_FILLS = "o.exchange_id = '$exchange' AND o.qty_traded > 0 AND NOT o.shadow AND $__timeFilter(o.ts)"
IS_EXIT = "(o.strategy = 'exits' OR o.trade_reason = 'INVENTORY_REBALANCE')"
YES_PRICE = "CASE WHEN o.side = 'yes' THEN o.fill_price ELSE 1 - o.fill_price END"
"""A NO bought at p is a YES sold at 1 - p: fills are drawn on the same YES price axis as everything else."""

FILL_POINTS = f"""SELECT o.ts AS time,
  CASE WHEN NOT {IS_EXIT} THEN {YES_PRICE} END AS "Entry",
  CASE WHEN {IS_EXIT} THEN {YES_PRICE} END AS "Exit"
FROM orders o WHERE {OUR_FILLS} ORDER BY 1"""

FILL_NOTES = f"""SELECT o.ts AS time,
  CASE WHEN {IS_EXIT} THEN 'EXIT' ELSE 'ENTRY' END || ' · ' || round(o.qty_traded)::int || ' '
  || upper(o.side) || ' @ ' || round((o.fill_price * 100)::numeric, 1) || 'c'
  || coalesce(' · edge ' || CASE WHEN e.edge_c >= 0 THEN '+' ELSE '' END || e.edge_c || 'c vs fair '
     || round((f.fair * 100)::numeric, 1) || 'c', '')
  || ' · cost ' || round(o.total_cost::numeric) || ' · ' || o.strategy AS text,
  o.strategy AS tags
FROM orders o
LEFT JOIN LATERAL (SELECT fair FROM fair_values fv WHERE fv.exchange_id = o.exchange_id AND fv.ts <= o.ts
                   ORDER BY fv.ts DESC LIMIT 1) f ON true
CROSS JOIN LATERAL (SELECT round(((CASE WHEN o.side = 'yes' THEN f.fair - o.fill_price
                                        ELSE 1 - o.fill_price - f.fair END) * 100)::numeric, 1) AS edge_c) e
WHERE {OUR_FILLS} ORDER BY 1"""
"""Edge vs the fair value at the time: positive means we bought below (YES) or sold above (NO) fair."""

FILL_ANNOTATIONS = {
    "name": "Our fills",
    "datasource": DS,
    "enable": True,
    "iconColor": "#ff7eb6",
    "target": target(FILL_NOTES, "table", "Anno"),
}

PRICE_OVERRIDES = [
    series("Fair", {"color": color("#73bf69"), "custom.lineWidth": 2}),
    series(
        "Kalshi",
        {"color": color("#33b5e5"), "custom.fillBelowTo": "Polymarket", "custom.fillOpacity": 22},
    ),
    series("Polymarket", {"color": color("#fade2a"), "custom.lineStyle": {"fill": "dash", "dash": [8, 6]}}),
    series("SIG ask", {"color": color("#f2495c"), "custom.fillBelowTo": "SIG bid", "custom.fillOpacity": 8}),
    series("SIG bid", {"color": color("#ff9830")}),
    *(
        series(
            name,
            {
                "color": color(hex_color),
                "custom.drawStyle": "points",
                "custom.pointSize": 14,
                "custom.showPoints": "always",
            },
        )
        for name, hex_color in (("Entry", "#56d364"), ("Exit", "#ff5f7e"))
    ),
]

MARKETS = dashboard(
    "sigbot-markets",
    "Sigbot - Markets & signals",
    [
        (
            12,
            [
                (
                    24,
                    timeseries(
                        "$exchange: Kalshi × Polymarket × SIG book, with our entries and exits",
                        'SELECT ts AS time, fair AS "Fair", pm_mid AS "Polymarket", kalshi_mid AS "Kalshi" '
                        "FROM fair_values WHERE exchange_id = '$exchange' AND $__timeFilter(ts) ORDER BY 1",
                        tob_span('bid AS "SIG bid", ask AS "SIG ask"'),
                        FILL_POINTS,
                        unit="percentunit",
                        step=True,
                        fill_opacity=0,
                        overrides=PRICE_OVERRIDES,
                    ),
                ),
            ],
        ),
        (
            7,
            [
                (
                    12,
                    timeseries(
                        "Gaps: Kalshi − Polymarket and SIG ask − bid",
                        'SELECT ts AS time, (kalshi_mid - pm_mid) * 100 AS "Kalshi − Polymarket" '
                        "FROM fair_values WHERE exchange_id = '$exchange' AND kalshi_mid IS NOT NULL "
                        "AND pm_mid IS NOT NULL AND $__timeFilter(ts) ORDER BY 1",
                        tob_span('(ask - bid) * 100 AS "SIG ask − bid"'),
                        unit="suffix:c",
                        step=True,
                        overrides=[
                            series("Kalshi − Polymarket", {"color": color("#33b5e5")}),
                            series("SIG ask − bid", {"color": color("#f2495c")}),
                        ],
                    ),
                ),
                (
                    12,
                    timeseries(
                        "Volume (shares per 5 min): our fills (+YES / −NO) and all SIG trades",
                        "SELECT $__timeGroupAlias(o.ts, '5m'), sum(CASE WHEN o.side = 'yes' "
                        'THEN o.qty_traded ELSE -o.qty_traded END) AS "Our fills" '
                        f"FROM orders o WHERE {OUR_FILLS} GROUP BY 1 ORDER BY 1",
                        "SELECT $__timeGroupAlias(ts, '5m'), sum(quantity) AS \"All SIG trades\" "
                        "FROM sig_trades WHERE exchange_id = '$exchange' AND $__timeFilter(ts) "
                        "GROUP BY 1 ORDER BY 1",
                        as_bars=True,
                        overrides=[
                            series("Our fills", {"color": color("#56d364")}),
                            series("All SIG trades", {"color": color("#8e8e8e"), "custom.fillOpacity": 40}),
                        ],
                    ),
                ),
            ],
        ),
        (
            10,
            [
                (
                    12,
                    table(
                        "Signals by race",
                        """SELECT race AS "Race",
  count(*) FILTER (WHERE decision = 'submitted') AS "Submitted",
  count(*) FILTER (WHERE decision LIKE 'rejected%') AS "Rejected",
  round((sum(edge * quantity) FILTER (WHERE decision = 'submitted') / 1000.0)::numeric, 2) AS "Expected edge"
FROM intents WHERE $__timeFilter(ts)
GROUP BY 1 ORDER BY 2 DESC, 3 DESC LIMIT 40""",
                    ),
                ),
                (
                    12,
                    bars(
                        "Why signals were rejected",
                        """SELECT decision AS "Reason", count(*) AS "Count"
FROM intents WHERE decision LIKE 'rejected%' AND $__timeFilter(ts)
GROUP BY 1 ORDER BY 2 DESC LIMIT 15""",
                        "Reason",
                    ),
                ),
            ],
        ),
        (
            7,
            [
                (
                    12,
                    table(
                        "Sharp moves: confirmed by Kalshi or by waiting",
                        """SELECT count(*) FILTER (WHERE kind = 'sharp_move') AS "Sharp moves",
  count(*) FILTER (WHERE kind = 'sharp_confirmed' AND data->>'by' = 'kalshi') AS "By Kalshi",
  count(*) FILTER (WHERE kind = 'sharp_confirmed' AND data->>'by' = 'timeout') AS "By timeout",
  round((percentile_cont(0.5) WITHIN GROUP (ORDER BY (data->>'after_s')::float)
    FILTER (WHERE kind = 'sharp_confirmed' AND data->>'by' = 'kalshi'))::numeric, 2)
    AS "Median Kalshi confirm (s)"
FROM events WHERE kind IN ('sharp_move', 'sharp_confirmed') AND $__timeFilter(ts)""",
                    ),
                ),
                (
                    12,
                    table(
                        "Recent sharp moves",
                        """SELECT e.ts AS "Time", r.race AS "Race", r.party AS "Party",
  e.kind AS "Event", e.message AS "Detail"
FROM events e LEFT JOIN races r ON r.exchange_id = e.data->>'exchange_id'
WHERE e.kind IN ('sharp_move', 'sharp_confirmed') AND $__timeFilter(e.ts)
ORDER BY e.ts DESC LIMIT 50""",
                    ),
                ),
            ],
        ),
        (
            10,
            [
                (
                    24,
                    table(
                        "Recent signals",
                        """SELECT i.ts AS "Time", i.strategy AS "Strategy", i.trade_reason AS "Reason",
  i.race AS "Race", r.party AS "Party", i.side AS "Side", i.price AS "Limit", i.quantity AS "Qty",
  round((i.edge / 10.0)::numeric, 1) AS "Edge (c)", i.decision AS "Decision", i.detail AS "Detail"
FROM intents i LEFT JOIN races r ON r.exchange_id = i.exchange_id
WHERE $__timeFilter(i.ts) ORDER BY i.ts DESC LIMIT 200""",
                    ),
                ),
            ],
        ),
        (
            8,
            [
                (
                    12,
                    timeseries(
                        "SIG activity (book changes, trades)",
                        "SELECT $__timeGroupAlias(ts, '5m'), count(*) AS \"Book changes\" "
                        "FROM book_events WHERE kind = 'dirty' AND $__timeFilter(ts) GROUP BY 1 ORDER BY 1",
                        "SELECT $__timeGroupAlias(ts, '5m'), count(*) AS \"Books pushed by SIG\" "
                        "FROM book_events WHERE kind = 'book' AND $__timeFilter(ts) GROUP BY 1 ORDER BY 1",
                        "SELECT $__timeGroupAlias(ts, '5m'), count(*) AS \"Trades\" "
                        "FROM sig_trades WHERE $__timeFilter(ts) GROUP BY 1 ORDER BY 1",
                    ),
                ),
                (
                    12,
                    table(
                        "Lost races",
                        """SELECT l.ts AS "Time", l.strategy AS "Strategy",
  r.race AS "Race", r.party AS "Party",
  l.side AS "Side", l.price AS "Price", l.requested AS "Wanted", l.filled AS "Got",
  round(l.send_to_resp_ms::numeric) AS "Response ms"
FROM lost_races l LEFT JOIN races r ON r.exchange_id = l.exchange_id
WHERE $__timeFilter(l.ts) ORDER BY l.ts DESC LIMIT 50""",
                    ),
                ),
            ],
        ),
        (
            8,
            [
                (
                    12,
                    timeseries(
                        "Data flow per minute",
                        "SELECT $__timeGroupAlias(ts, '1m'), count(*) AS \"Fair value updates\" "
                        "FROM fair_values WHERE $__timeFilter(ts) GROUP BY 1 ORDER BY 1",
                        "SELECT $__timeGroupAlias(ts, '1m'), count(*) AS \"SIG top-of-book changes\" "
                        "FROM sig_tob WHERE $__timeFilter(ts) GROUP BY 1 ORDER BY 1",
                        "SELECT $__timeGroupAlias(ts, '1m'), count(*) AS \"Books pushed by SIG\" "
                        "FROM book_events WHERE kind = 'book' AND $__timeFilter(ts) GROUP BY 1 ORDER BY 1",
                    ),
                ),
                (
                    12,
                    table(
                        "Bot events",
                        'SELECT ts AS "Time", level AS "Level", kind AS "Kind", message AS "Message" '
                        "FROM events WHERE $__timeFilter(ts) ORDER BY ts DESC LIMIT 100",
                    ),
                ),
            ],
        ),
    ],
    templating={"list": [EXCHANGE_VAR]},
    annotations={"list": [FILL_ANNOTATIONS]},
)

DASHBOARDS = {"pnl.json": PNL, "markets.json": MARKETS, "logs.json": LOGS}


def plain_sql(sql: str) -> str:
    """Replace Grafana macros/variables so the query runs directly on Postgres."""
    sql = re.sub(r"\$__timeFilter\(([\w.]+)\)", r"\1 > now() - interval '24 hours'", sql)
    sql = sql.replace("$__timeFrom()", "(now() - interval '24 hours')").replace("$__timeTo()", "now()")

    def bucket(mt: re.Match[str]) -> str:
        seconds = int(mt[2]) * 60
        return f"to_timestamp(floor(extract(epoch from {mt[1]}) / {seconds}) * {seconds}) AS time"

    sql = re.sub(r"\$__timeGroupAlias\(([\w.]+), '(\d+)m'\)", bucket, sql)
    return sql.replace("$exchange", "0").replace("$level", "'INFO','WARNING','ERROR','CRITICAL'")


def all_queries() -> list[tuple[str, str, str]]:
    out = []
    for name, dash in DASHBOARDS.items():
        for panel in dash["panels"]:
            out.extend((name, panel["title"], t["rawSql"]) for t in panel["targets"])
        out.extend(
            (name, f"variable {v['name']}", v["query"])
            for v in dash["templating"]["list"]
            if v["type"] == "query"
        )
        out.extend(
            (name, f"annotation {a['name']}", a["target"]["rawSql"]) for a in dash["annotations"]["list"]
        )
    return out


async def check(dsn: str) -> int:
    import asyncpg

    conn = await asyncpg.connect(dsn)
    failures = 0
    try:
        for name, title, sql in all_queries():
            try:
                rows = await conn.fetch(plain_sql(sql))
                print(f"ok    {name:13} {title:40} {len(rows)} rows")
            except asyncpg.PostgresError as exc:
                failures += 1
                print(f"FAIL  {name:13} {title:40} {exc}")
    finally:
        await conn.close()
    return failures


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", metavar="DSN", help="run every panel query against this database")
    args = parser.parse_args()
    for old in OUT.glob("*.json"):
        if old.name not in DASHBOARDS:
            old.unlink()
    for name, dash in DASHBOARDS.items():
        (OUT / name).write_text(json.dumps(dash, indent=2) + "\n")
        print(f"wrote {OUT / name} ({len(dash['panels'])} panels)")
    if args.check and asyncio.run(check(args.check)):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
