-- mart_users: daily user KPIs (new signups, actives, churn, MRR)

{{
    config(
        materialized='incremental',
        unique_key='report_date',
        incremental_strategy='merge'
    )
}}

WITH date_spine AS (
    SELECT generate_series(
        CURRENT_DATE - INTERVAL '89 days',
        CURRENT_DATE,
        INTERVAL '1 day'
    )::DATE AS report_date

    {% if is_incremental() %}
        -- Only reprocess last 7 days on incremental runs
        WHERE generate_series > (SELECT MAX(report_date) - 7 FROM {{ this }})
    {% endif %}
),

new_users AS (
    SELECT
        signup_date                    AS report_date,
        COUNT(*)                       AS new_users,
        SUM(plan_mrr_eur)              AS new_mrr_eur
    FROM {{ ref('stg_users') }}
    GROUP BY signup_date
),

active_users AS (
    SELECT
        occurred_at::DATE              AS report_date,
        COUNT(DISTINCT user_id)        AS active_users
    FROM {{ source('raw', 'events') }}
    GROUP BY occurred_at::DATE
),

churned_users AS (
    SELECT
        churned_at::DATE               AS report_date,
        COUNT(*)                       AS churned_users,
        SUM(plan_mrr_eur)              AS churned_mrr_eur
    FROM {{ ref('stg_users') }}
    WHERE is_churned
    GROUP BY churned_at::DATE
),

total_mrr AS (
    SELECT
        d.report_date,
        SUM(u.plan_mrr_eur)            AS mrr_eur
    FROM date_spine d
    JOIN {{ ref('stg_users') }} u
        ON u.signup_date <= d.report_date
        AND (u.churned_at IS NULL OR u.churned_at::DATE > d.report_date)
    GROUP BY d.report_date
)

SELECT
    d.report_date,
    COALESCE(n.new_users,     0)       AS new_users,
    COALESCE(a.active_users,  0)       AS active_users,
    COALESCE(c.churned_users, 0)       AS churned_users,
    COALESCE(m.mrr_eur,       0)       AS mrr_eur
FROM date_spine d
LEFT JOIN new_users     n ON n.report_date = d.report_date
LEFT JOIN active_users  a ON a.report_date = d.report_date
LEFT JOIN churned_users c ON c.report_date = d.report_date
LEFT JOIN total_mrr     m ON m.report_date = d.report_date
