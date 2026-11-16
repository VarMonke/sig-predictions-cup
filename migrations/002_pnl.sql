ALTER TABLE equity ADD COLUMN IF NOT EXISTS realized_pnl double precision;
ALTER TABLE equity ADD COLUMN IF NOT EXISTS unrealized_pnl double precision;
ALTER TABLE equity ADD COLUMN IF NOT EXISTS pnl double precision;
ALTER TABLE equity ADD COLUMN IF NOT EXISTS initial_balance double precision;

ALTER TABLE positions ADD COLUMN IF NOT EXISTS race text;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS party text;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS realized_pnl double precision;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS unrealized_pnl double precision;
CREATE INDEX IF NOT EXISTS positions_race_ts ON positions (race, ts DESC);

CREATE TABLE IF NOT EXISTS races (
    exchange_id text PRIMARY KEY,
    market_id text,
    race text NOT NULL,
    party text,
    kind text,
    name text
);

CREATE TABLE IF NOT EXISTS standing (
    ts timestamptz NOT NULL,
    rank integer,
    total integer
);
CREATE INDEX IF NOT EXISTS standing_ts ON standing (ts DESC);
