# Health Insurance Data Pipeline

A resilient, cloud-native ETL pipeline built for the Alignd Data Engineering Assessment. It covers the full data lifecycle: cloud-based file conversion on AWS, production Python cleaning, PostgreSQL star-schema design, SQL data quality enforcement, and a dbt-orchestrated transformation layer running inside Docker.

---

## Architecture Overview

```
data_files/
 ├── clients.csv              ──► PostgreSQL dim_clients
 ├── health_products.txt      ──► [Python cleaning] ──► dim_products
 └── health_lapses.parquet    ──► [S3 + Lambda]      ──► fct_health_lapses
                                                              │
                                                  ┌──────────▼──────────┐
                                                  │  PostgreSQL          │
                                                  │  (Star Schema)       │
                                                  │                      │
                                                  │  dim_date            │
                                                  │  dim_clients ◄───┐   │
                                                  │  dim_products     │  │
                                                  │  fct_health_lapses┘  │
                                                  └──────────┬──────────┘
                                                             │
                                                  ┌──────────▼──────────┐
                                                  │   dbt (Docker)       │
                                                  │                      │
                                                  │  staging/            │
                                                  │   stg_clients        │
                                                  │   stg_lapses         │
                                                  │   stg_products       │
                                                  │                      │
                                                  │  marts/              │
                                                  │   dim_patients       │
                                                  │   fct_patient_       │
                                                  │     claims_summary   │
                                                  └─────────────────────┘
```

---

## Project Structure

```
health-pipeline/
├── Dockerfile                      # dbt + Python runner image
├── docker-compose.yml              # Postgres + dbt services
├── pyproject.toml                  # Poetry dependency manifest
├── .gitignore
│
├── lambda/
│   ├── handler.py                  # Task 1: S3-triggered parquet→CSV Lambda
│   └── requirements.txt
│
├── scripts/
│   ├── clean_health_products.py    # Task 2: Idempotent pipe-delimited cleaner
│   └── load_data.py                # Upsert CSVs into PostgreSQL
│
├── sql/ddl/
│   ├── 01_star_schema.sql          # Task 3: DDL + index strategy
│   └── 02_transformations_quality.sql  # Task 4: Dedup + imputation + view
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── schema.yml              # Task 5: Sources + all dbt tests
│       ├── staging/
│       │   ├── stg_clients.sql
│       │   ├── stg_lapses.sql
│       │   └── stg_products.sql
│       └── marts/
│           ├── dim_patients.sql    # Task 5: dim_patients model
│           └── fct_patient_claims_summary.sql  # Task 5: fact summary model
│
├── tests/
│   ├── test_clean_health_products.py
│   └── test_lambda_handler.py
│
└── data_files/
    └── .gitkeep                    # Raw files excluded from git
```

---

## Task Summaries

### Task 1 – Resilient Cloud ETL (AWS Lambda)

**File:** `lambda/handler.py`

The Lambda function is triggered by an S3 `PutObject` event on the source bucket. It:

1. Downloads the `.parquet` object into memory.
2. Parses it with `pandas` + `pyarrow` (raises on corrupt data).
3. Converts to CSV and writes to the processed bucket under a `processed/` prefix.
4. On any exception: copies the source file to `error/<original-key>`, deletes the original, and emits a structured JSON log to CloudWatch.

**AWS setup:**
- Source bucket: `[initials]-source-bucket-analytics`
- Processed bucket: `[initials]-processed-bucket-analytics`
- Lambda: `[initials]-etl-function`, execution role `arn:aws:iam::970243984210:role/s3LambdaRole`
- Environment variable: `PROCESSED_BUCKET=[initials]-processed-bucket-analytics`
- S3 trigger: `All object create events` on the source bucket.

---

### Task 2 – Production-Grade Python (Data Cleaning)

**File:** `scripts/clean_health_products.py`

```bash
python scripts/clean_health_products.py \
  --input  data_files/health_products.txt \
  --output data_files/health_products_clean.csv
```

Key behaviours:
- **Programmatic structure discovery**: scans lines for the first pipe-delimited row to locate the data header — no hardcoded line numbers.
- **Idempotency**: computes a SHA-256 checksum of the input; if the output already exists and the checksum matches, the run is a no-op. Use `--force` to override.
- **Atomic writes**: output is written to a temp file then renamed, so a partial run never leaves a corrupt CSV.
- **Type inference**: numeric columns stored as strings are automatically promoted to `int`/`float`.
- **Null sentinel handling**: `N/A`, `NULL`, `-`, `?`, empty strings → `pd.NA`.

---

### Task 3 – Schema Design (PostgreSQL)

**File:** `sql/ddl/01_star_schema.sql`

Star schema in the `health` schema:

| Table | Type | Grain |
|---|---|---|
| `fct_health_lapses` | Fact | One row per lapse event |
| `dim_clients` | Dimension | One row per client (SCD Type 1) |
| `dim_products` | Dimension | One row per product |
| `dim_date` | Dimension | One row per calendar day |

**Indexing strategy:**
- All FK columns on the fact table get dedicated B-tree indexes to support index-nested-loop joins.
- A composite `(product_key, lapse_date_key)` index covers the most common trend-analysis query pattern.
- A partial index on `lapse_status = 'LAPSED'` accelerates active-lapse dashboards without indexing historical data.
- `dim_date` is indexed on `(year, month_number)` for range queries.

---

### Task 4 – SQL Transformation & Data Quality

**File:** `sql/ddl/02_transformations_quality.sql`

**Deduplication:** `ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY updated_at DESC)` — keeps the most recent record. Rejected duplicates are archived to `dim_clients_duplicates_audit` before deletion for compliance traceability.

**NULL income imputation — Province-level Median:**
Income distributions in health insurance are right-skewed (high earners pull the mean up). The median is therefore more robust and better represents the typical client. Imputation is performed at province level to preserve regional income variation; provinces with fewer than 30 known-income records fall back to the national median to avoid unstable estimates on thin data.

The view `vw_analytical_lapses` joins all four tables into a single wide dataset ready for BI tools or dbt staging.

---

### Task 5 – Automation & Environment (dbt)

**Files:** `dbt/models/`

**Staging layer** (views — always reflect latest source):
- `stg_clients` — normalised names, lowercased email
- `stg_lapses` — uppercased status
- `stg_products` — trimmed strings

**Mart layer** (tables — pre-built for BI performance):
- `dim_patients` — enriches clients with `age`, `age_band`, `income_band`, `full_name`
- `fct_patient_claims_summary` — one row per patient with aggregated lapse volume, financials, reinstatement rate, and `lapse_segment` classification

**dbt tests** (`schema.yml`):
- `unique` + `not_null` on every PK and business key
- `accepted_values` on `lapse_status`, `gender`, `age_band`, `income_band`, `lapse_segment`
- `dbt_utils.accepted_range` on `age` (0–120) and `reinstatement_rate_pct` (0–100)

---

## Quickstart

### Prerequisites
- Docker + Docker Compose
- AWS CLI configured (for Lambda deployment)

### 1. Place source files

```bash
cp /path/to/clients.csv             data_files/
cp /path/to/health_lapses.parquet   data_files/
cp /path/to/health_products.txt     data_files/
```

### 2. Run the full pipeline locally

```bash
docker compose up --build
```

This will:
1. Start PostgreSQL and apply DDL scripts automatically.
2. Run `clean_health_products.py` to produce the clean CSV.
3. Run `load_data.py` to upsert all data into the star schema.
4. Execute `dbt run` to build all staging and mart models.
5. Execute `dbt test` to validate data quality.

### 3. Run tests

```bash
# Install dev dependencies
poetry install

# All tests
poetry run pytest tests/ -v --cov=lambda --cov=scripts --cov-report=term-missing
```

### 4. Deploy Lambda

```bash
# Package dependencies
cd lambda
pip install -r requirements.txt -t package/
cp handler.py package/
cd package && zip -r ../function.zip . && cd ..

# Deploy
aws lambda update-function-code \
  --function-name [initials]-etl-function \
  --zip-file fileb://function.zip
```

---

## Scaling to 100× Volume

The current pipeline handles hundreds of thousands of rows. At 100× volume (tens of millions of rows, multi-GB parquet files), the following changes would be made:

### Ingestion (Task 1)
- Replace Lambda (15-min timeout, 10 GB RAM limit) with **AWS Glue** or **ECS Fargate** tasks triggered by S3 Event Notifications via SQS. SQS decouples arrival rate from processing rate, providing back-pressure handling automatically.
- Use **S3 multipart upload** for CSV outputs > 100 MB.
- Process parquet files **partition-by-partition** using `pyarrow.dataset` to avoid loading the full file into memory.

### Storage (Task 3)
- **Partition** `fct_health_lapses` by `lapse_date_key` (range partitioning by year-month) so queries on recent data never touch historical partitions.
- Consider **columnar storage** in PostgreSQL via `pg_partman` + `TimescaleDB` for time-series queries, or migrate the fact table to **Amazon Redshift** / **BigQuery** for analytic workloads.
- Add **read replicas** for BI tools to avoid contention with ETL writes.

### Transformation (Task 4 & 5)
- Replace the single-node dbt run with **dbt Cloud** or **Astronomer (Airflow)** for parallelised model execution and dependency-aware scheduling.
- Use **incremental dbt models** (`materialized='incremental'`, `unique_key='lapse_id'`) so only new/changed rows are processed on each run rather than full rebuilds.
- Move income imputation to a **Python dbt model** or a pre-processing step using `scikit-learn` k-NN imputer trained on the full dataset for better accuracy at scale.

### Orchestration & Observability
- Add **Apache Airflow** (or AWS Step Functions) DAG to enforce task ordering: clean → load → dbt run → dbt test → notify.
- Emit **structured JSON logs** from every step to CloudWatch / Datadog with `pipeline_run_id` for end-to-end tracing.
- Set up **dbt source freshness** checks and alerting so the team is notified if source data stops arriving.
- Implement **data contracts** (Great Expectations or dbt's `constraints:`) to catch schema drift from upstream teams early.
