-- ============================================================
-- 03_populate_dims.sql
-- Populate dimension tables from staging
-- ============================================================

-- Step 1: dim_date
INSERT INTO scratch.dim_date (date_key, full_date, year, month_number, month_name, is_weekend)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT,
    d,
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT,
    TO_CHAR(d, 'Month'),
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM GENERATE_SERIES('2024-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) d
ON CONFLICT (date_key) DO NOTHING;

-- Step 2: dim_clients (deduplicate + type convert)
INSERT INTO scratch.dim_clients (client_id, name, income, province)
SELECT DISTINCT ON (client_id)
    client_id,
    name,
    CASE WHEN income = '' THEN NULL 
         ELSE income::NUMERIC(14,2) END AS income,
    province
FROM scratch.stg_clients
ORDER BY client_id
ON CONFLICT (client_id) DO UPDATE
SET
    name = EXCLUDED.name,
    income = EXCLUDED.income,
    province = EXCLUDED.province;

-- Step 3: dim_products (clean product_code + map is_active)
INSERT INTO scratch.dim_products (product_code, product_name, tier, is_active)
SELECT DISTINCT ON (product_code)
    UPPER(product_code),
    product_name,
    tier,
    CASE WHEN LOWER(status) = 'active' THEN TRUE ELSE FALSE END
FROM scratch.stg_products
ORDER BY product_code
ON CONFLICT (product_code) DO UPDATE
SET
    product_name = EXCLUDED.product_name,
    tier = EXCLUDED.tier,
    is_active = EXCLUDED.is_active;