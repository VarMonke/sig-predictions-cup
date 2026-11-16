CREATE TABLE IF NOT EXISTS events (
    ts timestamptz NOT NULL,
    kind text NOT NULL,
    level text NOT NULL,
    message text NOT NULL,
    data jsonb
);
CREATE INDEX IF NOT EXISTS events_ts ON events (ts DESC);

CREATE TABLE IF NOT EXISTS fair_values (
    ts timestamptz NOT NULL,
    exchange_id text NOT NULL,
    fair double precision NOT NULL,
    unc double precision,
    pm_mid double precision,
    kalshi_mid double precision,
    sources smallint,
    confirmed boolean
);
CREATE INDEX IF NOT EXISTS fair_values_ex_ts ON fair_values (exchange_id, ts DESC);

CREATE TABLE IF NOT EXISTS sig_tob (
    ts timestamptz NOT NULL,
    exchange_id text NOT NULL,
    bid double precision,
    bid_qty double precision,
    ask double precision,
    ask_qty double precision
);
CREATE INDEX IF NOT EXISTS sig_tob_ex_ts ON sig_tob (exchange_id, ts DESC);

CREATE TABLE IF NOT EXISTS sig_trades (
    ts timestamptz NOT NULL,
    exchange_id text NOT NULL,
    market_id text,
    price double precision,
    quantity double precision,
    server_ts timestamptz
);
CREATE INDEX IF NOT EXISTS sig_trades_ex_ts ON sig_trades (exchange_id, ts DESC);

CREATE TABLE IF NOT EXISTS book_events (
    ts timestamptz NOT NULL,
    server_ts timestamptz,
    kind text NOT NULL,
    exchange_id text,
    market_id text
);
CREATE INDEX IF NOT EXISTS book_events_ex_ts ON book_events (exchange_id, ts DESC);

CREATE TABLE IF NOT EXISTS intents (
    ts timestamptz NOT NULL,
    strategy text NOT NULL,
    race text,
    exchange_id text NOT NULL,
    side text,
    price double precision,
    quantity integer,
    edge double precision,
    group_id text,
    decision text,
    trade_reason text,
    detail text
);
CREATE INDEX IF NOT EXISTS intents_ts ON intents (ts DESC);

CREATE TABLE IF NOT EXISTS orders (
    ts timestamptz NOT NULL,
    idem_key text,
    order_id bigint,
    strategy text,
    group_id text,
    exchange_id text,
    side text,
    price double precision,
    quantity integer,
    shadow boolean,
    status text,
    trade_reason text,
    qty_traded double precision,
    fill_price double precision,
    total_cost double precision,
    signal_to_send_ms double precision,
    send_to_resp_ms double precision,
    error text
);
CREATE INDEX IF NOT EXISTS orders_ts ON orders (ts DESC);

CREATE TABLE IF NOT EXISTS fills (
    ts timestamptz NOT NULL,
    order_id bigint,
    exchange_id text,
    price double precision,
    quantity double precision,
    source text
);
CREATE INDEX IF NOT EXISTS fills_ts ON fills (ts DESC);

CREATE TABLE IF NOT EXISTS lost_races (
    ts timestamptz NOT NULL,
    strategy text,
    exchange_id text,
    side text,
    price double precision,
    requested integer,
    filled double precision,
    signal_to_send_ms double precision,
    send_to_resp_ms double precision,
    book_age_ms double precision
);
CREATE INDEX IF NOT EXISTS lost_races_ts ON lost_races (ts DESC);

CREATE TABLE IF NOT EXISTS equity (
    ts timestamptz NOT NULL,
    cash double precision,
    positions_fair double precision,
    positions_sig double precision,
    equity_fair double precision,
    equity_sig double precision,
    deployed double precision,
    national_delta double precision,
    tier smallint,
    halted boolean
);
CREATE INDEX IF NOT EXISTS equity_ts ON equity (ts DESC);

CREATE TABLE IF NOT EXISTS positions (
    ts timestamptz NOT NULL,
    exchange_id text,
    quantity double precision,
    avg_cost double precision,
    cost_basis double precision,
    fair double precision,
    sig_price double precision
);
CREATE INDEX IF NOT EXISTS positions_ts ON positions (ts DESC);

CREATE TABLE IF NOT EXISTS smart_score (
    ts timestamptz NOT NULL,
    market_type text,
    score double precision,
    decayed double precision,
    rank integer,
    total integer,
    elite boolean,
    min_elite double precision
);

CREATE TABLE IF NOT EXISTS leaderboard (
    ts timestamptz NOT NULL,
    rank integer,
    profile_id text,
    username text,
    pnl double precision,
    trades integer,
    volume double precision,
    roi double precision,
    win_rate double precision
);
CREATE INDEX IF NOT EXISTS leaderboard_ts ON leaderboard (ts DESC);

CREATE TABLE IF NOT EXISTS ladder (
    ts timestamptz NOT NULL,
    from_tier smallint,
    to_tier smallint,
    reason text
);
