-- ============================================================
-- 04_transformations_quality.sql
-- Data quality, deduplication, imputation, and analytical views
-- Performs all cleaning: product normalization, client dedup, income imputation
-- ============================================================

-- Step 0: Product normalization - clean raw product data
-- Uppercase product codes, normalize status values
UPDATE scratch.stg_products
SET
    product_code = UPPER(TRIM(product_code)),
    product_name = TRIM(product_name),
    tier = TRIM(tier),
    status = UPPER(TRIM(status));

-- Step 1: Deduplication - Archive duplicates before deleting
-- Keeps most recent record per client, archives others for compliance

CREATE TABLE IF NOT EXISTS scratch.dim_clients_duplicates_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(50),
    name VARCHAR(100),
    income NUMERIC(14,2),
    province VARCHAR(100),
    action VARCHAR(50),
    reason VARCHAR(200),
    archived_at TIMESTAMPTZ DEFAULT NOW()
);

-- Step 2: Deduplicate clients - keep most recent, archive rest
WITH ranked_clients AS (
    SELECT
        client_id,
        name,
        income,
        province,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY client_id DESC) AS rn
    FROM scratch.dim_clients
)
INSERT INTO scratch.dim_clients_duplicates_audit 
    (client_id, name, income, province, action, reason)
SELECT
    client_id,
    name,
    income,
    province,
    'ARCHIVED',
    'Duplicate - kept more recent record'
FROM ranked_clients
WHERE rn > 1;

-- Delete duplicate records (keep rank 1)
DELETE FROM scratch.dim_clients
WHERE client_id IN (
    SELECT client_id
    FROM scratch.dim_clients
    GROUP BY client_id
    HAVING COUNT(*) > 1
)
AND client_id NOT IN (
    SELECT DISTINCT ON (client_id) client_id
    FROM scratch.dim_clients
    ORDER BY client_id, client_key DESC
);

-- Step 3: Impute missing income using province-level median
-- Falls back to national median for provinces with <30 known-income records

-- Calculate province-level median incomes (only non-NULL)
WITH province_medians AS (
    SELECT
        province,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY income) AS median_income,
        COUNT(*) AS record_count
    FROM scratch.dim_clients
    WHERE income IS NOT NULL
    GROUP BY province
),
-- National median as fallback
national_median AS (
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY income) AS median_income
    FROM scratch.dim_clients
    WHERE income IS NOT NULL
)
-- Apply imputation
UPDATE scratch.dim_clients dc
SET
    income = COALESCE(
        (SELECT median_income FROM province_medians WHERE province = dc.province AND record_count >= 30),
        (SELECT median_income FROM national_median)
    ),
    income_imputed = TRUE
WHERE income IS NULL;

-- Step 4: Create analytical view - joins all dimensions with facts
CREATE OR REPLACE VIEW scratch.vw_analytical_lapses AS
SELECT
    fl.lapse_id,
    fl.policy_id,
    fl.premium_amount,
    fl.lapse_status,
    fl.loaded_at,
    -- Date dimension
    dd.full_date,
    dd.year,
    dd.month_number,
    dd.month_name,
    dd.is_weekend,
    -- Client dimension
    dc.client_id,
    dc.name AS client_name,
    dc.income,
    dc.province,
    dc.income_imputed,
    -- Product dimension
    dp.product_code,
    dp.product_name,
    dp.tier,
    dp.is_active
FROM scratch.fct_health_lapses fl
INNER JOIN scratch.dim_date dd ON fl.lapse_date_key = dd.date_key
INNER JOIN scratch.dim_clients dc ON fl.client_key = dc.client_key
INNER JOIN scratch.dim_products dp ON fl.product_key = dp.product_key;

-- Step 5: Data quality checks - flag issues for monitoring
CREATE TABLE IF NOT EXISTS scratch.data_quality_checks (
    check_id BIGSERIAL PRIMARY KEY,
    check_name VARCHAR(100),
    check_timestamp TIMESTAMPTZ DEFAULT NOW(),
    status VARCHAR(20),
    rows_affected BIGINT,
    description TEXT
);

-- Check 1: Null incomes after imputation
INSERT INTO scratch.data_quality_checks (check_name, status, rows_affected, description)
SELECT
    'Null incomes post-imputation',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*),
    CASE WHEN COUNT(*) = 0 
        THEN 'All client incomes populated' 
        ELSE 'Found ' || COUNT(*) || ' clients with NULL income after imputation'
    END
FROM scratch.dim_clients
WHERE income IS NULL;

-- Check 2: Duplicate clients
INSERT INTO scratch.data_quality_checks (check_name, status, rows_affected, description)
SELECT
    'Duplicate clients',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*),
    CASE WHEN COUNT(*) = 0 
        THEN 'No duplicate client_ids found' 
        ELSE 'Found ' || COUNT(*) || ' duplicate client records'
    END
FROM (
    SELECT client_id, COUNT(*) as cnt
    FROM scratch.dim_clients
    GROUP BY client_id
    HAVING COUNT(*) > 1
) dups;

-- Check 3: Orphaned facts (FK violations)
INSERT INTO scratch.data_quality_checks (check_name, status, rows_affected, description)
SELECT
    'Orphaned facts - missing clients',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*),
    CASE WHEN COUNT(*) = 0 
        THEN 'All facts reference valid clients' 
        ELSE 'Found ' || COUNT(*) || ' facts with invalid client_key'
    END
FROM scratch.fct_health_lapses fl
LEFT JOIN scratch.dim_clients dc ON fl.client_key = dc.client_key
WHERE dc.client_key IS NULL;

-- Check 4: Orphaned facts - missing products
INSERT INTO scratch.data_quality_checks (check_name, status, rows_affected, description)
SELECT
    'Orphaned facts - missing products',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*),
    CASE WHEN COUNT(*) = 0 
        THEN 'All facts reference valid products' 
        ELSE 'Found ' || COUNT(*) || ' facts with invalid product_key'
    END
FROM scratch.fct_health_lapses fl
LEFT JOIN scratch.dim_products dp ON fl.product_key = dp.product_key
WHERE dp.product_key IS NULL;

-- Check 5: Invalid dates
INSERT INTO scratch.data_quality_checks (check_name, status, rows_affected, description)
SELECT
    'Invalid dates in facts',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    COUNT(*),
    CASE WHEN COUNT(*) = 0 
        THEN 'All fact dates are valid' 
        ELSE 'Found ' || COUNT(*) || ' facts with invalid/missing dates'
    END
FROM scratch.fct_health_lapses fl
LEFT JOIN scratch.dim_date dd ON fl.lapse_date_key = dd.date_key
WHERE dd.date_key IS NULL;

-- Step 6: Summary statistics view
CREATE OR REPLACE VIEW scratch.vw_quality_summary AS
SELECT
    check_name,
    status,
    rows_affected,
    description,
    check_timestamp
FROM scratch.data_quality_checks
ORDER BY check_timestamp DESC
LIMIT 5;
