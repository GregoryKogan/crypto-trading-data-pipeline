CREATE DATABASE airflow_db;


\c crypto_data;

CREATE TABLE trades_1min_agg (
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    symbol          VARCHAR(16) NOT NULL,
    open_price      NUMERIC(20, 8),
    high_price      NUMERIC(20, 8),
    low_price       NUMERIC(20, 8),
    close_price     NUMERIC(20, 8),
    total_volume    NUMERIC(20, 8),
    vwap            NUMERIC(20, 8),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, window_start)
);

CREATE INDEX idx_trades_1min_agg_window_start ON trades_1min_agg (window_start DESC);
