CREATE TABLE IF NOT EXISTS logs (
    ts timestamptz NOT NULL,
    level text NOT NULL,
    logger text,
    message text NOT NULL
);
CREATE INDEX IF NOT EXISTS logs_ts ON logs (ts DESC);
