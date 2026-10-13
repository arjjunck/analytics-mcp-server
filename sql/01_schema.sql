-- ============================================================
-- Analytics MCP Server - PostgreSQL Schema
-- Mirrors a real SaaS analytics setup (Stripe/HubSpot style)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ── Raw layer (operational data) ──────────────────────────────

CREATE TABLE IF NOT EXISTS raw.users (
    user_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email         TEXT NOT NULL UNIQUE,
    plan          TEXT NOT NULL CHECK (plan IN ('free', 'starter', 'pro', 'enterprise')),
    country       TEXT NOT NULL DEFAULT 'IE',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    churned_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       UUID NOT NULL REFERENCES raw.users(user_id),
    amount_eur    NUMERIC(10, 2) NOT NULL,
    currency      TEXT NOT NULL DEFAULT 'EUR',
    status        TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'refunded', 'failed')),
    product_sku   TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.events (
    event_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       UUID REFERENCES raw.users(user_id),
    session_id    TEXT NOT NULL,
    event_name    TEXT NOT NULL,  -- e.g. 'page_view', 'feature_used', 'api_call'
    properties    JSONB DEFAULT '{}',
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.feature_usage (
    usage_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       UUID NOT NULL REFERENCES raw.users(user_id),
    feature_name  TEXT NOT NULL,
    usage_count   INT NOT NULL DEFAULT 1,
    recorded_date DATE NOT NULL DEFAULT CURRENT_DATE
);

-- ── Analytics layer (dbt materialises these) ──────────────────

CREATE TABLE IF NOT EXISTS analytics.daily_revenue (
    report_date   DATE PRIMARY KEY,
    total_eur     NUMERIC(14, 2),
    order_count   INT,
    avg_order_eur NUMERIC(10, 2),
    refund_rate   NUMERIC(5, 4)
);

CREATE TABLE IF NOT EXISTS analytics.user_metrics (
    report_date   DATE PRIMARY KEY,
    new_users     INT,
    active_users  INT,
    churned_users INT,
    mrr_eur       NUMERIC(14, 2)
);

CREATE TABLE IF NOT EXISTS analytics.product_metrics (
    report_date    DATE,
    feature_name   TEXT,
    unique_users   INT,
    total_uses     INT,
    adoption_rate  NUMERIC(5, 4),
    PRIMARY KEY (report_date, feature_name)
);

-- ── Indexes ───────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_orders_user_id    ON raw.orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON raw.orders(created_at);
CREATE INDEX IF NOT EXISTS idx_events_user_id    ON raw.events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_occurred   ON raw.events(occurred_at);
CREATE INDEX IF NOT EXISTS idx_events_name       ON raw.events(event_name);
CREATE INDEX IF NOT EXISTS idx_feature_user      ON raw.feature_usage(user_id);
CREATE INDEX IF NOT EXISTS idx_feature_date      ON raw.feature_usage(recorded_date);
