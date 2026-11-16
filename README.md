# sigbot: SIG Predictions Cup on the Midterm Elections

Automated trading bot built for SIG's **Predictions Cup**, the Midterm Elections tournament on
`sig.thesuper.market`, a play-money prediction market ("SUSQies", 100k start). It ran live through the
tournament (2026-10-01 → 2026-11-04) against ~1,600 other accounts.

The post-mortem is in [WRITEUP.md](WRITEUP.md).
Development happened in a private repo during the comp, this repo is the finished tree.

![the private repo at 183 commits](.github/private-history.png)

## What's inside

- `src/sigbot/` — the bot: SIG REST client + Supabase/Phoenix realtime, Polymarket websocket and Kalshi
  polling feeds, a fair-value engine, strategies (stale-book snipes, flow quoting anchored on SIG's own mid, a sweep ladder for whole-bankroll dumps, exits/recycling), a layered risk gate, Discord control and alerts, Postgres recording with Grafana dashboards.
- `deploy/` — one-step deployment (below).
- `sig-api/` — SIG platform reference: the API spec (`api-spec.json`, captured from their docs), the generated market mapping (`mapping.json`), and pre-open data snapshots
  (`preopen-data/`).
- `scripts/` — probes and one-offs (read-only smoke tests, feed probes, mapping builder, dashboard builder, the post-halt settlement lift).

## Constraints from SIG

SIG allows **100 reads and 30 writes/min per key, and a 429 locks the key for 60 s — orders included**.
Every SIG call goes through `SigClient`'s `RequestBudget`; an order the budget cannot send in time is a
`NOT_SENT`. Realtime is Supabase channels (`tournament:{id}:market:{marketId}`, 100 channels
per socket); books are still polled so quiet markets stay provably fresh for the stale-book risk rule.

## Run it locally

Python 3.13, deps via uv (pinned by `exclude-newer`):

```
uv sync --all-extras
uv run ruff format src scripts && uv run ruff check src scripts
uv run pyright
```

The bot defaults to **shadow mode** (`TRADING_ENABLED` unset): it runs the whole pipeline and records what
it *would* have done, placing no orders. `API_KEY` goes in `deploy/.env` for live SIG reads.

## Deploy

```
cd deploy
cp .env.example .env   # fill in API_KEY, POSTGRES_PASSWORD, GRAFANA_ADMIN_PASSWORD, DOMAIN
docker compose up -d --build
```

One command brings up Postgres, the bot, the Discord control bot, Grafana with provisioned dashboards, and
Caddy terminating HTTPS for `DOMAIN`. Every variable for risk limits, strategy toggles and Discord IDs is
documented in `deploy/.env.example`.

## License

MIT — see [LICENSE](LICENSE).
