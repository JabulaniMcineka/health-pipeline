-- models/staging/stg_products.sql

WITH source AS (
    SELECT * FROM {{ source('health', 'dim_products') }}
),

cleaned AS (
    SELECT
        product_key,
        product_code,
        TRIM(product_name)                          AS product_name,
        TRIM(product_category)                      AS product_category,
        TRIM(product_type)                          AS product_type,
        TRIM(insurer_name)                          AS insurer_name,
        premium_amount,
        cover_amount,
        effective_date,
        expiry_date,
        is_active,
        created_at,
        updated_at

    FROM source
)

SELECT * FROM cleaned
