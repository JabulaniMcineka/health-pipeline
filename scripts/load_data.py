"""
scripts/load_data.py
Loads all cleaned data files into PostgreSQL.
Idempotent: uses UPSERT (INSERT ... ON CONFLICT DO UPDATE).
"""

import logging
import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "localhost"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
    "user":     os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "postgres"),
    "dbname":   os.environ.get("DB_NAME", "health_db"),
}

DATA_DIR = Path("data_files")


def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def load_clients(conn, path: Path) -> int:
    """Upsert clients.csv into health.dim_clients."""
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    log.info("Loading %d client rows from %s", len(df), path)

    sql = """
        INSERT INTO health.dim_clients
            (client_id, first_name, last_name, date_of_birth, gender,
             email, phone, address_line1, city, province, postal_code, income)
        VALUES %s
        ON CONFLICT (client_id) DO UPDATE SET
            first_name    = EXCLUDED.first_name,
            last_name     = EXCLUDED.last_name,
            date_of_birth = EXCLUDED.date_of_birth,
            gender        = EXCLUDED.gender,
            email         = EXCLUDED.email,
            phone         = EXCLUDED.phone,
            city          = EXCLUDED.city,
            province      = EXCLUDED.province,
            postal_code   = EXCLUDED.postal_code,
            income        = EXCLUDED.income,
            updated_at    = NOW()
    """

    def _row(r):
        return (
            r.get("client_id"),   r.get("first_name"),   r.get("last_name"),
            r.get("date_of_birth") or None,
            r.get("gender"),      r.get("email"),        r.get("phone"),
            r.get("address_line1"), r.get("city"),       r.get("province"),
            r.get("postal_code"),
            float(r["income"]) if pd.notna(r.get("income")) else None,
        )

    with conn.cursor() as cur:
        execute_values(cur, sql, [_row(r) for _, r in df.iterrows()])
    conn.commit()
    log.info("Upserted %d client rows", len(df))
    return len(df)


def load_products(conn, path: Path) -> int:
    """Upsert health_products_clean.csv into health.dim_products."""
    if not path.exists():
        log.warning("Clean products file not found: %s – run clean_health_products.py first", path)
        return 0

    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    log.info("Loading %d product rows from %s", len(df), path)

    sql = """
        INSERT INTO health.dim_products
            (product_code, product_name, product_category, product_type,
             insurer_name, premium_amount, cover_amount, effective_date, expiry_date, is_active)
        VALUES %s
        ON CONFLICT (product_code) DO UPDATE SET
            product_name     = EXCLUDED.product_name,
            product_category = EXCLUDED.product_category,
            product_type     = EXCLUDED.product_type,
            insurer_name     = EXCLUDED.insurer_name,
            premium_amount   = EXCLUDED.premium_amount,
            cover_amount     = EXCLUDED.cover_amount,
            effective_date   = EXCLUDED.effective_date,
            expiry_date      = EXCLUDED.expiry_date,
            is_active        = EXCLUDED.is_active,
            updated_at       = NOW()
    """

    def _row(r):
        return (
            r.get("product_code"), r.get("product_name"),
            r.get("product_category"), r.get("product_type"),
            r.get("insurer_name"),
            float(r["premium_amount"]) if pd.notna(r.get("premium_amount")) else None,
            float(r["cover_amount"])   if pd.notna(r.get("cover_amount"))   else None,
            r.get("effective_date") or None,
            r.get("expiry_date")    or None,
            (str(r.get("is_active", "true")).lower() not in ("false", "0", "no")),
        )

    with conn.cursor() as cur:
        execute_values(cur, sql, [_row(r) for _, r in df.iterrows()])
    conn.commit()
    log.info("Upserted %d product rows", len(df))
    return len(df)


if __name__ == "__main__":
    conn = get_connection()
    try:
        load_clients(conn,  DATA_DIR / "clients.csv")
        load_products(conn, DATA_DIR / "health_products_clean.csv")
        log.info("All data loaded successfully.")
    finally:
        conn.close()
