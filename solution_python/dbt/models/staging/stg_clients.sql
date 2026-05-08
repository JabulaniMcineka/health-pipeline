-- models/staging/stg_clients.sql
-- Staging layer: light cleaning + rename from raw health schema.
-- Materialized as view so it always reflects the latest source data.

WITH source AS (
    SELECT * FROM {{ source('health', 'dim_clients') }}
),

renamed AS (
    SELECT
        client_key,
        client_id,
        TRIM(first_name)                        AS first_name,
        TRIM(last_name)                         AS last_name,
        date_of_birth,
        UPPER(TRIM(gender))                     AS gender,
        LOWER(TRIM(email))                      AS email,
        TRIM(phone)                             AS phone,
        TRIM(city)                              AS city,
        TRIM(province)                          AS province,
        TRIM(postal_code)                       AS postal_code,
        income,
        income_imputed,
        created_at,
        updated_at

    FROM source
)

SELECT * FROM renamed
