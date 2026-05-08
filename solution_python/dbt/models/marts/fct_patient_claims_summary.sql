-- models/marts/fct_patient_claims_summary.sql
-- Task 5: fct_patient_claims_summary mart model.
-- One row per patient with aggregated lapse/reinstatement metrics.

{{
  config(
    materialized = 'table',
    unique_key   = 'client_key',
    tags         = ['fact', 'claims', 'summary']
  )
}}

WITH lapses AS (
    SELECT * FROM {{ ref('stg_lapses') }}
),

patients AS (
    SELECT * FROM {{ ref('dim_patients') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

lapse_metrics AS (
    SELECT
        l.client_key,

        -- Volume
        COUNT(*)                                            AS total_lapses,
        COUNT(*) FILTER (WHERE l.lapse_status = 'LAPSED')
                                                            AS active_lapses,
        COUNT(*) FILTER (WHERE l.lapse_status = 'REINSTATED')
                                                            AS reinstated_lapses,
        COUNT(*) FILTER (WHERE l.lapse_status = 'CANCELLED')
                                                            AS cancelled_lapses,

        -- Financials
        SUM(l.premium_at_lapse)                             AS total_premium_at_lapse,
        AVG(l.premium_at_lapse)                             AS avg_premium_at_lapse,
        SUM(l.outstanding_balance)                          AS total_outstanding_balance,

        -- Reinstatement speed
        AVG(l.days_to_reinstatement)
            FILTER (WHERE l.days_to_reinstatement IS NOT NULL)
                                                            AS avg_days_to_reinstatement,

        -- Lapse dates
        MIN(l.lapse_date_key)                               AS first_lapse_date_key,
        MAX(l.lapse_date_key)                               AS last_lapse_date_key,

        -- Product diversity
        COUNT(DISTINCT l.product_key)                       AS distinct_products_lapsed,

        -- Most frequent lapse reason
        MODE() WITHIN GROUP (ORDER BY l.lapse_reason)       AS most_common_lapse_reason

    FROM lapses l
    GROUP BY l.client_key
),

final AS (
    SELECT
        -- Patient dimension attributes
        p.client_key,
        p.client_id,
        p.full_name,
        p.age,
        p.age_band,
        p.gender,
        p.province,
        p.income,
        p.income_band,
        p.income_imputed,

        -- Lapse summary metrics (NULL when patient has no lapses)
        COALESCE(m.total_lapses,             0)             AS total_lapses,
        COALESCE(m.active_lapses,            0)             AS active_lapses,
        COALESCE(m.reinstated_lapses,        0)             AS reinstated_lapses,
        COALESCE(m.cancelled_lapses,         0)             AS cancelled_lapses,
        m.total_premium_at_lapse,
        m.avg_premium_at_lapse,
        m.total_outstanding_balance,
        m.avg_days_to_reinstatement,
        m.distinct_products_lapsed,
        m.most_common_lapse_reason,

        -- Derived KPIs
        CASE
            WHEN COALESCE(m.total_lapses, 0) = 0 THEN 'No Lapse'
            WHEN COALESCE(m.reinstated_lapses, 0) > 0
              AND COALESCE(m.active_lapses, 0) = 0         THEN 'Fully Reinstated'
            WHEN COALESCE(m.active_lapses, 0) > 0          THEN 'Active Lapse'
            ELSE 'Mixed'
        END                                                 AS lapse_segment,

        ROUND(
            100.0 * COALESCE(m.reinstated_lapses, 0)
            / NULLIF(m.total_lapses, 0),
            2
        )                                                   AS reinstatement_rate_pct,

        -- Metadata
        NOW()                                               AS model_updated_at

    FROM patients p
    LEFT JOIN lapse_metrics m ON p.client_key = m.client_key
)

SELECT * FROM final
