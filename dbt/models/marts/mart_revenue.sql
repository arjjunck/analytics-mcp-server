-- mart_revenue: daily revenue rollup
-- Incremental model — only processes new orders since last run

{{
    config(
        materialized='incremental',
        unique_key='report_date',
        incremental_strategy='merge'
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}

    {% if is_incremental() %}
        WHERE order_date > (SELECT MAX(report_date) FROM {{ this }})
    {% endif %}
),

daily_agg AS (
    SELECT
        order_date                                                       AS report_date,
        SUM(amount_eur) FILTER (WHERE is_completed)                     AS total_eur,
        COUNT(*) FILTER (WHERE is_completed)                            AS order_count,
        AVG(amount_eur) FILTER (WHERE is_completed)                     AS avg_order_eur,
        ROUND(
            COUNT(*) FILTER (WHERE is_refunded)::NUMERIC /
            NULLIF(COUNT(*), 0), 4
        )                                                               AS refund_rate
    FROM orders
    GROUP BY order_date
)

SELECT * FROM daily_agg
