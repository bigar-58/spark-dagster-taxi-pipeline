CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS audit;

-- DAILY ZONE METRICS

CREATE TABLE IF NOT EXISTS mart.daily_zone_metrics (
    pickup_date DATE NOT NULL,
    pickup_location_id INTEGER NOT NULL,
    pickup_borough TEXT NOT NULL,
    pickup_zone TEXT NOT NULL,
    pickup_service_zone TEXT NOT NULL,
    trip_count BIGINT NOT NULL CHECK (trip_count >= 0),
    gross_revenue_amount NUMERIC(20, 2) NOT NULL,
    fare_revenue_amount NUMERIC(20, 2) NOT NULL,
    total_tip_amount NUMERIC(20, 2),
    avg_trip_distance DOUBLE PRECISION,
    avg_trip_duration_minutes DOUBLE PRECISION,
    avg_fare_per_mile DOUBLE PRECISION,
    card_payment_share DOUBLE PRECISION NOT NULL
        CHECK (card_payment_share BETWEEN 0 AND 1),
    cash_payment_share DOUBLE PRECISION NOT NULL
        CHECK (cash_payment_share BETWEEN 0 AND 1),
    source_batch_count BIGINT NOT NULL,
    latest_source_ingested_at TIMESTAMPTZ NOT NULL,
    publish_run_id TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pickup_date, pickup_location_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_zone_metrics_location_date ON mart.daily_zone_metrics (pickup_location_id, pickup_date);


-- HOURLY DEMAND METRICS

CREATE TABLE IF NOT EXISTS mart.hourly_demand_metrics (
    pickup_date DATE NOT NULL,
    pickup_hour INTEGER NOT NULL
        CHECK (pickup_hour BETWEEN 0 AND 23),
    trip_count BIGINT NOT NULL CHECK (trip_count >= 0),
    distinct_pickup_zones BIGINT NOT NULL
        CHECK (distinct_pickup_zones >= 0),
    gross_revenue_amount NUMERIC(20, 2) NOT NULL,
    fare_revenue_amount NUMERIC(20, 2) NOT NULL,
    total_tip_amount NUMERIC(20, 2),
    avg_trip_distance DOUBLE PRECISION,
    avg_trip_duration_minutes DOUBLE PRECISION,
    card_payment_share DOUBLE PRECISION NOT NULL
        CHECK (card_payment_share BETWEEN 0 AND 1),
    cash_payment_share DOUBLE PRECISION NOT NULL
        CHECK (cash_payment_share BETWEEN 0 AND 1),
    source_batch_count BIGINT NOT NULL,
    latest_source_ingested_at TIMESTAMPTZ NOT NULL,
    publish_run_id TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pickup_date, pickup_hour)
);

-- STAGING TABLES
CREATE TABLE IF NOT EXISTS staging.daily_zone_metrics (
    LIKE mart.daily_zone_metrics INCLUDING DEFAULTS
);
CREATE TABLE IF NOT EXISTS staging.hourly_demand_metrics (
    LIKE mart.hourly_demand_metrics INCLUDING DEFAULTS
);


-- AUDIT TABLE
CREATE TABLE IF NOT EXISTS audit.gold_publish_runs (
    run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    date_from DATE,
    date_to DATE,
    daily_zone_row_count BIGINT,
    hourly_demand_row_count BIGINT,
    error_message TEXT
);