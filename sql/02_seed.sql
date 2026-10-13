-- ============================================================
-- Seed data — 90 days of realistic SaaS analytics
-- ============================================================

-- Users (100 across plans, spread over 90 days)
INSERT INTO raw.users (user_id, email, plan, country, created_at, churned_at)
SELECT
    gen_random_uuid(),
    'user_' || i || '@example.com',
    (ARRAY['free','free','free','starter','starter','pro','pro','enterprise'])[1 + (i % 8)],
    (ARRAY['IE','IE','IE','GB','DE','FR','US','NL'])[1 + (i % 8)],
    NOW() - ((90 - (i % 90)) || ' days')::INTERVAL,
    CASE WHEN i % 12 = 0
         THEN NOW() - ((i % 30) || ' days')::INTERVAL
         ELSE NULL END
FROM generate_series(1, 100) AS i;

-- Orders (500 orders from paying users over 90 days)
INSERT INTO raw.orders (order_id, user_id, amount_eur, status, product_sku, created_at)
SELECT
    gen_random_uuid(),
    u.user_id,
    ROUND((CASE u.plan
        WHEN 'starter'    THEN 29 + (RANDOM() * 10)
        WHEN 'pro'        THEN 99 + (RANDOM() * 20)
        WHEN 'enterprise' THEN 499 + (RANDOM() * 100)
        ELSE 0
    END)::NUMERIC, 2),
    (ARRAY['completed','completed','completed','completed','refunded','failed'])[1 + (RANDOM()*5)::INT],
    'PLAN-' || UPPER(u.plan) || '-MONTHLY',
    NOW() - ((RANDOM() * 90)::INT || ' days')::INTERVAL
FROM raw.users u
CROSS JOIN generate_series(1, 5)
WHERE u.plan != 'free';

-- Events (page views, feature usage, API calls)
INSERT INTO raw.events (user_id, session_id, event_name, properties, occurred_at)
SELECT
    u.user_id,
    'sess_' || MD5(u.user_id::TEXT || i::TEXT),
    (ARRAY[
        'page_view','page_view','page_view',
        'feature_used','feature_used',
        'api_call','api_call',
        'dashboard_viewed','report_exported','user_invited'
    ])[1 + (RANDOM()*9)::INT],
    jsonb_build_object(
        'page',    (ARRAY['/dashboard','/reports','/settings','/api-docs'])[1+(RANDOM()*3)::INT],
        'source',  (ARRAY['direct','email','organic','referral'])[1+(RANDOM()*3)::INT]
    ),
    NOW() - ((RANDOM() * 90)::INT || ' days')::INTERVAL - ((RANDOM() * 24)::INT || ' hours')::INTERVAL
FROM raw.users u
CROSS JOIN generate_series(1, 30) AS i;

-- Feature usage (daily rollups)
INSERT INTO raw.feature_usage (user_id, feature_name, usage_count, recorded_date)
SELECT
    u.user_id,
    feat,
    (1 + (RANDOM() * 20))::INT,
    CURRENT_DATE - (RANDOM() * 89)::INT
FROM raw.users u
CROSS JOIN unnest(ARRAY[
    'data_export','api_integration','custom_dashboard',
    'automated_reports','team_sharing','webhook_sink','sql_editor'
]) AS feat
WHERE RANDOM() > 0.4;

-- Materialise analytics layer (dbt would do this; seeding manually here)
INSERT INTO analytics.daily_revenue (report_date, total_eur, order_count, avg_order_eur, refund_rate)
SELECT
    created_at::DATE                                                  AS report_date,
    SUM(amount_eur) FILTER (WHERE status = 'completed')              AS total_eur,
    COUNT(*) FILTER (WHERE status = 'completed')                     AS order_count,
    AVG(amount_eur) FILTER (WHERE status = 'completed')              AS avg_order_eur,
    ROUND(COUNT(*) FILTER (WHERE status = 'refunded')::NUMERIC /
          NULLIF(COUNT(*), 0), 4)                                    AS refund_rate
FROM raw.orders
GROUP BY created_at::DATE
ON CONFLICT (report_date) DO NOTHING;

INSERT INTO analytics.user_metrics (report_date, new_users, active_users, churned_users, mrr_eur)
SELECT
    d::DATE                                                              AS report_date,
    COUNT(u.user_id) FILTER (WHERE u.created_at::DATE = d::DATE)        AS new_users,
    COUNT(DISTINCT e.user_id)                                           AS active_users,
    COUNT(u.user_id) FILTER (WHERE u.churned_at::DATE = d::DATE)        AS churned_users,
    SUM(CASE u.plan
        WHEN 'starter'    THEN 29
        WHEN 'pro'        THEN 99
        WHEN 'enterprise' THEN 499
        ELSE 0 END) FILTER (WHERE u.churned_at IS NULL)                 AS mrr_eur
FROM generate_series(CURRENT_DATE - 89, CURRENT_DATE, '1 day') AS d
LEFT JOIN raw.users  u ON TRUE
LEFT JOIN raw.events e ON e.occurred_at::DATE = d::DATE
GROUP BY d
ON CONFLICT (report_date) DO NOTHING;

INSERT INTO analytics.product_metrics (report_date, feature_name, unique_users, total_uses, adoption_rate)
SELECT
    recorded_date                                               AS report_date,
    feature_name,
    COUNT(DISTINCT user_id)                                     AS unique_users,
    SUM(usage_count)                                            AS total_uses,
    ROUND(COUNT(DISTINCT user_id)::NUMERIC /
          NULLIF((SELECT COUNT(*) FROM raw.users), 0), 4)       AS adoption_rate
FROM raw.feature_usage
GROUP BY recorded_date, feature_name
ON CONFLICT (report_date, feature_name) DO NOTHING;
