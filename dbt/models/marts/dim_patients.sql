-- models/marts/dim_patients.sql
-- Task 5: dim_patients mart model.
-- Enriches the client staging model with derived attributes for analytics.
-- Materialized as TABLE so BI tools can query without repeated joins.

{{
  config(
    materialized = 'table',
    unique_key   = 'client_key',
    tags         = ['dimension', 'patients']
  )
}}

WITH clients AS (
    SELECT * FROM {{ ref('stg_clients') }}
),

enriched AS (
    SELECT
        -- Surrogate key
        client_key,

        -- Business key
        client_id,

        -- Name
        first_name,
        last_name,
        first_name || ' ' || last_name          AS full_name,

        -- Demographics
        date_of_birth,
        gender,
        DATE_PART('year', AGE(date_of_birth))::INT  AS age,
        CASE
            WHEN DATE_PART('year', AGE(date_of_birth)) < 18  THEN 'Minor'
            WHEN DATE_PART('year', AGE(date_of_birth)) < 35  THEN '18-34'
            WHEN DATE_PART('year', AGE(date_of_birth)) < 50  THEN '35-49'
            WHEN DATE_PART('year', AGE(date_of_birth)) < 65  THEN '50-64'
            ELSE '65+'
        END                                         AS age_band,

        -- Contact
        email,
        phone,

        -- Location
        city,
        province,
        postal_code,

        -- Income (post-imputation)
        income,
        income_imputed,
        CASE
            WHEN income < 100000                THEN 'Low'
            WHEN income < 350000                THEN 'Middle'
            WHEN income < 750000                THEN 'Upper-Middle'
            ELSE                                     'High'
        END                                         AS income_band,

        -- Metadata
        created_at,
        updated_at

    FROM clients
)

SELECT * FROM enriched
