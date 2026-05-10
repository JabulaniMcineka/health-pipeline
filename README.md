# Health Pipeline

This repository contains a working SQL-based health insurance data pipeline built for local execution. The current implementation covers raw staging ingestion, dimensional table population, deduplication, income imputation, and data quality validation.

## What is implemented

### 1. Local staging ingestion
- `solution_in_sql/scripts/load_data.py`
  - Loads `data_files/clients.csv` into `scratch.stg_clients`
  - Loads `data_files/health_products.txt` into `scratch.stg_products`
  - Loads `data_files/health_lapses.parquet` into `scratch.stg_health_lapses`

### 2. Schema and dimension building
- `solution_in_sql/sql/ddl/01_schema.sql`
  - Creates the core star schema with `dim_date`, `dim_clients`, `dim_products`, and `fct_health_lapses`
- `solution_in_sql/sql/ddl/02_staging_schema.sql`
  - Creates the raw staging tables used by the pipeline
- `solution_in_sql/sql/ddl/03_populate_dims.sql`
  - Populates `dim_date` using `GENERATE_SERIES`
  - Loads `dim_clients` from staging with deduplication and numeric type conversion
  - Loads `dim_products` from staging with normalized product codes and active-state mapping
  - Uses PostgreSQL `ON CONFLICT` to make dimension loads repeatable

### 3. Transformation and data quality
- `solution_in_sql/sql/ddl/04_transformations_quality.sql`
  - Normalizes raw product data in staging
  - Archives duplicate client rows into `scratch.dim_clients_duplicates_audit`
  - Deletes duplicate clients and keeps a single canonical row
  - Imputes missing client income using province median, with fallback to national median
  - Builds analytical view `scratch.vw_analytical_lapses`
  - Runs data quality checks and stores results in `scratch.data_quality_checks`
  - Exposes a summary view `scratch.vw_quality_summary`

### 4. Automation helpers
- `solution_in_sql/scripts/run_sql_pipeline.py`
  - Runs the SQL pipeline in order:
    1. `01_schema.sql`
    2. `02_staging_schema.sql`
    3. `03_populate_dims.sql`
    4. `04_transformations_quality.sql`
- `solution_in_sql/dags/health_pipeline_dag.py`
  - Airflow DAG skeleton for the local SQL pipeline
  - Runs schema creation, staging load, dimension build, and quality checks

## Current pipeline status

### Completed
- Local read and ingestion of raw source files
- Raw staging load into PostgreSQL
- Dimension population with idempotent inserts
- Product normalization and client deduplication
- Income imputation and quality audit logging
- End-to-end SQL pipeline execution via `run_sql_pipeline.py`

### Remaining work
- Loading `scratch.stg_health_lapses` into `scratch.fct_health_lapses`
- Completing the Airflow DAG to include lapse ingestion and full orchestration
- AWS Lambda ingestion path for `health_lapses.parquet`
- dbt project implementation and tests for production-grade analytics
- Automated unit tests for Python scripts and integration tests for the full pipeline

## How to run the pipeline locally

1. Ensure PostgreSQL is running locally with a database named `health_db`.
2. Ensure Python dependencies are installed (pandas, psycopg2).
3. Run the staging load:

```bash
cd solution_in_sql
python scripts/load_data.py
```

4. Run the SQL transformation pipeline:

```bash
cd solution_in_sql
python scripts/run_sql_pipeline.py
```

## Notes for job applications

This repository now includes a documented, working SQL pipeline with:
- raw ingestion from CSV and Parquet
- repeatable staging loads
- dimension table build with deduplication
- data quality validation and auditing
- both script-based and DAG-based orchestration artifacts

The code is suitable for a data engineering portfolio because it demonstrates:
- ETL design principles
- SQL idempotency patterns
- data quality checks
- pipeline documentation and reproducibility

## Important files

- `solution_in_sql/scripts/load_data.py`
- `solution_in_sql/scripts/run_sql_pipeline.py`
- `solution_in_sql/sql/ddl/01_schema.sql`
- `solution_in_sql/sql/ddl/02_staging_schema.sql`
- `solution_in_sql/sql/ddl/03_populate_dims.sql`
- `solution_in_sql/sql/ddl/04_transformations_quality.sql`
- `solution_in_sql/dags/health_pipeline_dag.py`

---

If you want, I can also add a second documentation file that explains the technical details of each SQL step and the exact data model used for the star schema.