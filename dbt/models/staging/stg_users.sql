-- stg_users: clean user data with churn flags

WITH source AS (
    SELECT * FROM {{ source('raw', 'users') }}
),

cleaned AS (
    SELECT
        user_id,
        email,
        plan,
        country,
        created_at,
        churned_at,

        -- Derived fields
        (churned_at IS NOT NULL)                   AS is_churned,
        created_at::DATE                           AS signup_date,
        DATE_TRUNC('month', created_at)::DATE      AS signup_month,
        CASE plan
            WHEN 'starter'    THEN 29
            WHEN 'pro'        THEN 99
            WHEN 'enterprise' THEN 499
            ELSE 0
        END                                        AS plan_mrr_eur,
        EXTRACT(DAYS FROM NOW() - created_at)::INT AS days_since_signup

    FROM source
)

SELECT * FROM cleaned
