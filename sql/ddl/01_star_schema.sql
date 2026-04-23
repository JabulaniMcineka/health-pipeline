-- =============================================================================
-- Task 3: Star Schema DDL – Health Insurance Pipeline
-- Database: PostgreSQL 15+
--
-- Schema design:
--   Fact table  : fct_health_lapses   (one row per lapse event)
--   Dimensions  : dim_clients         (client demographics)
--                 dim_products        (health product catalogue)
--                 dim_date            (calendar dimension)
--
-- Indexing strategy (documented inline):
--   • Primary keys are clustered B-tree indexes (automatic).
--   • Every FK column on the fact table gets a dedicated B-tree index so
--     hash-joins can avoid full scans when filtering by dimension attributes.
--   • dim_date is indexed on year+month for common range partitioning.
--   • Partial indexes on frequently-filtered predicates (e.g. active lapses).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 0. Schema & Extensions
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS health;

SET search_path = health, public;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";     -- uuid_generate_v4()
CREATE EXTENSION IF NOT EXISTS pg_trgm;         -- future fuzzy-name searches

-- ---------------------------------------------------------------------------
-- 1. dim_date  (role-playing: used for lapse_date, reinstatement_date)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key          INT             NOT NULL,   -- YYYYMMDD surrogate
    full_date         DATE            NOT NULL,
    day_of_week       SMALLINT        NOT NULL,   -- 1=Mon … 7=Sun (ISO)
    day_name          VARCHAR(10)     NOT NULL,
    day_of_month      SMALLINT        NOT NULL,
    day_of_year       SMALLINT        NOT NULL,
    week_of_year      SMALLINT        NOT NULL,
    month_number      SMALLINT        NOT NULL,
    month_name        VARCHAR(10)     NOT NULL,
    quarter           SMALLINT        NOT NULL,
    year              SMALLINT        NOT NULL,
    is_weekend        BOOLEAN         NOT NULL,
    is_public_holiday BOOLEAN         NOT NULL DEFAULT FALSE,

    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);

-- Range scans by year/month (common in insurance reporting)
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month
    ON dim_date (year, month_number);

-- ---------------------------------------------------------------------------
-- 2. dim_clients  (SCD Type 1 – last-write-wins for slowly-changing attrs)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_clients (
    client_key        SERIAL          NOT NULL,
    client_id         VARCHAR(50)     NOT NULL,   -- business key from clients.csv
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    date_of_birth     DATE,
    gender            CHAR(1),                    -- M / F / O
    email             VARCHAR(254),
    phone             VARCHAR(30),
    address_line1     VARCHAR(200),
    address_line2     VARCHAR(200),
    city              VARCHAR(100),
    province          VARCHAR(100),
    postal_code       VARCHAR(20),
    income            NUMERIC(14, 2),             -- annual income; NULLs handled in Task 4
    income_imputed    BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_dim_clients         PRIMARY KEY (client_key),
    CONSTRAINT uq_dim_clients_id      UNIQUE      (client_id)
);

-- FK lookups from fact table use client_id (business key) → fast equality probe
CREATE INDEX IF NOT EXISTS idx_dim_clients_client_id
    ON dim_clients (client_id);

-- Analytics often filter by province or income band
CREATE INDEX IF NOT EXISTS idx_dim_clients_province
    ON dim_clients (province);

CREATE INDEX IF NOT EXISTS idx_dim_clients_income
    ON dim_clients (income)
    WHERE income IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 3. dim_products  (health product catalogue from health_products.txt)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_products (
    product_key       SERIAL          NOT NULL,
    product_code      VARCHAR(50)     NOT NULL,   -- business key
    product_name      VARCHAR(200)    NOT NULL,
    product_category  VARCHAR(100),
    product_type      VARCHAR(100),
    insurer_name      VARCHAR(200),
    premium_amount    NUMERIC(10, 2),
    cover_amount      NUMERIC(14, 2),
    effective_date    DATE,
    expiry_date       DATE,
    is_active         BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_dim_products        PRIMARY KEY (product_key),
    CONSTRAINT uq_dim_products_code   UNIQUE      (product_code)
);

-- Frequently joined on product_code from the fact table
CREATE INDEX IF NOT EXISTS idx_dim_products_code
    ON dim_products (product_code);

-- Analysts often filter by category + active flag
CREATE INDEX IF NOT EXISTS idx_dim_products_category_active
    ON dim_products (product_category, is_active);

-- ---------------------------------------------------------------------------
-- 4. fct_health_lapses  (grain: one row per lapse event per client-product)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fct_health_lapses (
    lapse_id              BIGSERIAL       NOT NULL,
    -- Surrogate FKs (fast joins via integer lookups)
    client_key            INT             NOT NULL,
    product_key           INT             NOT NULL,
    lapse_date_key        INT             NOT NULL,
    reinstatement_date_key INT,                    -- NULL if not reinstated
    -- Business keys (kept for lineage / debugging without joins)
    client_id             VARCHAR(50)     NOT NULL,
    product_code          VARCHAR(50)     NOT NULL,
    -- Measures
    lapse_reason          VARCHAR(200),
    lapse_status          VARCHAR(50)     NOT NULL DEFAULT 'LAPSED',
    days_to_reinstatement INT,                     -- derived; NULL if not reinstated
    premium_at_lapse      NUMERIC(10, 2),
    outstanding_balance   NUMERIC(14, 2),
    -- Audit
    source_file           VARCHAR(500),            -- lineage: which parquet file
    loaded_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_fct_health_lapses    PRIMARY KEY (lapse_id),

    CONSTRAINT fk_fct_client
        FOREIGN KEY (client_key)
        REFERENCES dim_clients (client_key)
        ON DELETE RESTRICT,

    CONSTRAINT fk_fct_product
        FOREIGN KEY (product_key)
        REFERENCES dim_products (product_key)
        ON DELETE RESTRICT,

    CONSTRAINT fk_fct_lapse_date
        FOREIGN KEY (lapse_date_key)
        REFERENCES dim_date (date_key)
        ON DELETE RESTRICT,

    CONSTRAINT fk_fct_reinstatement_date
        FOREIGN KEY (reinstatement_date_key)
        REFERENCES dim_date (date_key)
        ON DELETE RESTRICT,

    CONSTRAINT chk_lapse_status
        CHECK (lapse_status IN ('LAPSED', 'REINSTATED', 'CANCELLED', 'PENDING'))
);

-- ---------------------------------------------------------------------------
-- Indexes on fact table FK columns
-- (Rationale: star-schema queries filter on dimension attributes, then join
--  back to fact; these indexes allow index-nested-loop joins at scale.)
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_fct_lapses_client_key
    ON fct_health_lapses (client_key);

CREATE INDEX IF NOT EXISTS idx_fct_lapses_product_key
    ON fct_health_lapses (product_key);

CREATE INDEX IF NOT EXISTS idx_fct_lapses_lapse_date_key
    ON fct_health_lapses (lapse_date_key);

-- Composite: most reports group by (product, date) for trend analysis
CREATE INDEX IF NOT EXISTS idx_fct_lapses_product_date
    ON fct_health_lapses (product_key, lapse_date_key);

-- Partial index: active (not-yet-reinstated) lapses are queried very frequently
CREATE INDEX IF NOT EXISTS idx_fct_lapses_active
    ON fct_health_lapses (client_key, lapse_date_key)
    WHERE lapse_status = 'LAPSED';

-- ---------------------------------------------------------------------------
-- 5. Utility: updated_at auto-refresh trigger
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION health.set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER trg_dim_clients_updated_at
    BEFORE UPDATE ON dim_clients
    FOR EACH ROW EXECUTE FUNCTION health.set_updated_at();

CREATE OR REPLACE TRIGGER trg_dim_products_updated_at
    BEFORE UPDATE ON dim_products
    FOR EACH ROW EXECUTE FUNCTION health.set_updated_at();

-- ---------------------------------------------------------------------------
-- 6. Comments (documentation-as-code)
-- ---------------------------------------------------------------------------
COMMENT ON TABLE fct_health_lapses   IS 'Fact table: one row per health policy lapse event. Grain = client + product + lapse date.';
COMMENT ON TABLE dim_clients         IS 'Client dimension (SCD1). Source: clients.csv. Income NULLs are median-imputed (see Task 4).';
COMMENT ON TABLE dim_products        IS 'Product catalogue dimension. Source: health_products.txt (cleaned by scripts/clean_health_products.py).';
COMMENT ON TABLE dim_date            IS 'Calendar dimension. Populate via scripts/populate_dim_date.sql for 2000-2040.';
COMMENT ON COLUMN dim_clients.income_imputed IS 'TRUE when income was NULL in source and has been filled with the province-level median.';
