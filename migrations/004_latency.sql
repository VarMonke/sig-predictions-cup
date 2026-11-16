CREATE TABLE IF NOT EXISTS latency (
    ts timestamptz NOT NULL,
    source text NOT NULL,
    metric text NOT NULL,
    samples integer NOT NULL,
    p50_ms double precision,
    p95_ms double precision,
    max_ms double precision
);
CREATE INDEX IF NOT EXISTS latency_ts ON latency (ts DESC);
