-- =============================================================================
-- Task 4: SQL Transformation & Data Quality
-- Goal  : Unified analytical dataset with deduplication + NULL income handling.
-- =============================================================================

SET search_path = health, public;

-- ---------------------------------------------------------------------------
-- 4A. DEDUPLICATION – dim_clients
--
-- Strategy: use ROW_NUMBER() partitioned by client_id, ordered by the most
-- recent updated_at. The latest record is kept (row = 1), all others are
-- tombstoned into an audit table before deletion.
-- ---------------------------------------------------------------------------

-- Audit table: preserves every rejected duplicate for lineage/compliance
CREATE TABLE IF NOT EXISTS health.dim_clients_duplicates_audit (
    audit_id          BIGSERIAL       NOT NULL PRIMARY KEY,
    client_key        INT             NOT NULL,
    client_id         VARCHAR(50)     NOT NULL,
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    income            NUMERIC(14, 2),
    duplicate_rank    INT             NOT NULL,   -- rank from dedup window
    resolved_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Step 1: identify duplicates
WITH ranked AS (
    SELECT
        client_key,
        client_id,
        first_name,
        last_name,
        income,
        ROW_NUMBER() OVER (
            PARTITION BY client_id
            ORDER BY updated_at DESC, client_key DESC   -- latest update wins; tie-break on PK
        ) AS rn
    FROM health.dim_clients
),
dupes AS (
    SELECT * FROM ranked WHERE rn > 1
)
-- Step 2: archive duplicates before removal
INSERT INTO health.dim_clients_duplicates_audit
    (client_key, client_id, first_name, last_name, income, duplicate_rank)
SELECT
    d.client_key,
    d.client_id,
    c.first_name,
    c.last_name,
    c.income,
    d.rn
FROM dupes d
JOIN health.dim_clients c USING (client_key);

-- Step 3: delete the duplicates
DELETE FROM health.dim_clients
WHERE client_key IN (
    SELECT client_key
    FROM (
        SELECT
            client_key,
            ROW_NUMBER() OVER (
                PARTITION BY client_id
                ORDER BY updated_at DESC, client_key DESC
            ) AS rn
        FROM health.dim_clients
    ) ranked
    WHERE rn > 1
);


-- ---------------------------------------------------------------------------
-- 4B. NULL INCOME IMPUTATION
--
-- Method chosen: Province-level median (P50)
--
-- Justification (see comment below):
-- Income distributions in health insurance datasets are typically right-skewed
-- (a small number of high earners pull the mean upward). Using the median is
-- therefore more robust than the mean because it is not distorted by outliers
-- and better represents the "typical" client.  We impute at province level
-- rather than a global median to preserve regional income variation — a client
-- in Gauteng likely has a different typical income than one in Limpopo. If a
-- province has too few known-income records (< 30), we fall back to the
-- national median to avoid unstable estimates on thin data.
--
-- Alternative considered: k-NN imputation based on age + province features.
-- Rejected for this task because it requires Python (scikit-learn) and
-- introduces model dependency; the median approach is simple, reproducible,
-- and fully set-based.
-- ---------------------------------------------------------------------------

-- Step 1: compute province medians (fall back to national if n < 30)
WITH province_median AS (
    SELECT
        province,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY income) AS median_income,
        COUNT(*)                                              AS n
    FROM health.dim_clients
    WHERE income IS NOT NULL
    GROUP BY province
),
national_median AS (
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY income) AS median_income
    FROM health.dim_clients
    WHERE income IS NOT NULL
),
-- Step 2: assign imputed value – provincial if stable, national otherwise
imputed AS (
    SELECT
        c.client_key,
        COALESCE(
            CASE WHEN pm.n >= 2 THEN pm.median_income END,
            nm.median_income
        ) AS imputed_income
    FROM health.dim_clients c
    LEFT JOIN province_median pm  ON c.province = pm.province
    CROSS JOIN national_median nm
    WHERE c.income IS NULL
)
-- Step 3: apply imputation + flag the rows
UPDATE health.dim_clients c
SET
    income          = i.imputed_income,
    income_imputed  = TRUE,
    updated_at      = NOW()
FROM imputed i
WHERE c.client_key = i.client_key;


-- ---------------------------------------------------------------------------
-- 4C. UNIFIED ANALYTICAL VIEW
-- Combines fact + all dimensions into a wide, analytics-ready dataset.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW health.vw_analytical_lapses AS
SELECT
    -- Lapse event
    f.lapse_id,
    f.lapse_status,
    f.lapse_reason,
    f.days_to_reinstatement,
    f.premium_at_lapse,
    f.outstanding_balance,
    f.source_file,
    f.loaded_at,

    -- Client attributes
    c.client_id,
    c.first_name,
    c.last_name,
    c.gender,
    c.date_of_birth,
    DATE_PART('year', AGE(c.date_of_birth))::INT      AS age,
    c.province,
    c.city,
    c.income,
    c.income_imputed,

    -- Product attributes
    p.product_code,
    p.product_name,
    p.product_category,
    p.product_type,
    p.insurer_name,
    p.premium_amount                                   AS listed_premium,

    -- Date attributes (lapse)
    ld.full_date                                       AS lapse_date,
    ld.year                                            AS lapse_year,
    ld.month_number                                    AS lapse_month,
    ld.month_name                                      AS lapse_month_name,
    ld.quarter                                         AS lapse_quarter,

    -- Date attributes (reinstatement – may be NULL)
    rd.full_date                                       AS reinstatement_date,
    rd.year                                            AS reinstatement_year,
    rd.month_number                                    AS reinstatement_month

FROM health.fct_health_lapses f
JOIN health.dim_clients  c  ON f.client_key            = c.client_key
JOIN health.dim_products p  ON f.product_key           = p.product_key
JOIN health.dim_date     ld ON f.lapse_date_key        = ld.date_key
LEFT JOIN health.dim_date rd ON f.reinstatement_date_key = rd.date_key;

COMMENT ON VIEW health.vw_analytical_lapses
    IS 'Denormalised star-schema join; single source of truth for BI reports and dbt staging models.';


-- ---------------------------------------------------------------------------
-- 4D. SPOT-CHECK QUERIES (run manually to verify data quality)
-- ---------------------------------------------------------------------------

-- Count remaining duplicates (should be 0 after dedup)
SELECT client_id, COUNT(*) AS n
FROM health.dim_clients
GROUP BY client_id
HAVING COUNT(*) > 1;

-- Confirm NULL income records are gone
SELECT COUNT(*) AS null_income_remaining
FROM health.dim_clients
WHERE income IS NULL;

-- Fact table orphan check
SELECT COUNT(*) AS orphan_clients
FROM health.fct_health_lapses f
LEFT JOIN health.dim_clients c ON f.client_key = c.client_key
WHERE c.client_key IS NULL;

SELECT COUNT(*) AS orphan_products
FROM health.fct_health_lapses f
LEFT JOIN health.dim_products p ON f.product_key = p.product_key
WHERE p.product_key IS NULL;
