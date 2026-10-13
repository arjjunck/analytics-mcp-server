-- stg_orders: clean and type-cast raw orders
-- Materialized as a view on top of raw.orders

WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),

cleaned AS (
    SELECT
        order_id,
        user_id,
        amount_eur,
        currency,
        status,
        product_sku,
        created_at,

        -- Derived fields
        (status = 'completed')                     AS is_completed,
        (status = 'refunded')                      AS is_refunded,
        (status = 'failed')                        AS is_failed,
        created_at::DATE                           AS order_date,
        DATE_TRUNC('month', created_at)::DATE      AS order_month

    FROM source
    WHERE amount_eur > 0  -- exclude zero-value test orders
)

SELECT * FROM cleaned
