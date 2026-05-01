-- TimescaleDB schema for real-time market data pipeline
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS trades (
    time            TIMESTAMPTZ     NOT NULL,
    received_at     TIMESTAMPTZ     NOT NULL,
    symbol          VARCHAR(20)     NOT NULL,
    trade_id        BIGINT          NOT NULL,
    price           NUMERIC(20, 8)  NOT NULL,
    quantity        NUMERIC(20, 8)  NOT NULL,
    is_buyer_maker  BOOLEAN         NOT NULL,
    vwap_1m         NUMERIC(20, 8),
    is_anomaly      BOOLEAN         NOT NULL DEFAULT FALSE
);

SELECT create_hypertable('trades', 'time', if_not_exists => TRUE);

-- Deduplication guard at the storage layer
CREATE UNIQUE INDEX IF NOT EXISTS trades_symbol_trade_id_idx
    ON trades (symbol, trade_id);

CREATE INDEX IF NOT EXISTS trades_symbol_time_idx
    ON trades (symbol, time DESC);

CREATE INDEX IF NOT EXISTS trades_price_idx
    ON trades (price);

-- Partial index — anomaly lookups stay fast even at scale
CREATE INDEX IF NOT EXISTS trades_anomaly_time_idx
    ON trades (time DESC)
    WHERE is_anomaly = TRUE;

CREATE TABLE IF NOT EXISTS dead_letter_trades (
    id          BIGSERIAL       PRIMARY KEY,
    received_at TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    raw_data    JSONB           NOT NULL,
    reason      VARCHAR(500)    NOT NULL
);

CREATE INDEX IF NOT EXISTS dlq_received_at_idx
    ON dead_letter_trades (received_at DESC);

CREATE INDEX IF NOT EXISTS dlq_reason_idx
    ON dead_letter_trades (reason);
