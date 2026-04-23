-- models/staging/stg_lapses.sql

WITH source AS (
    SELECT * FROM {{ source('health', 'fct_health_lapses') }}
),

cleaned AS (
    SELECT
        lapse_id,
        client_key,
        product_key,
        lapse_date_key,
        reinstatement_date_key,
        client_id,
        product_code,
        TRIM(lapse_reason)                          AS lapse_reason,
        UPPER(TRIM(lapse_status))                   AS lapse_status,
        days_to_reinstatement,
        premium_at_lapse,
        outstanding_balance,
        source_file,
        loaded_at

    FROM source
)

SELECT * FROM cleaned
