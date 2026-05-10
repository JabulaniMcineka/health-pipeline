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
    "host": "127.0.0.1",   #  forces trust auth
    "port": 5432,
    "user": "postgres",
    "password": "",        #  no password
    "dbname": "health_db",
}


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_files"


def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def load_clients(conn, path: Path) -> int:
    """Load clients.csv into scratch.stg_clients."""

    df = pd.read_csv(path, dtype=str)

    df.columns = [
        c.strip().lower().replace(" ", "_")
        for c in df.columns
    ]

    log.info("Loading %d client rows from %s", len(df), path)

    insert_sql = """
        INSERT INTO scratch.stg_clients (
            client_id,
            name,
            income,
            province
        )
        VALUES %s
    """

    def _row(r):
        return (
            r.get("client_id"),
            r.get("name"),
            r.get("income"),
            r.get("province"),
        )

    rows = [_row(r) for _, r in df.iterrows()]

    with conn.cursor() as cur:

        # Optional for repeatable loads
        cur.execute("TRUNCATE TABLE scratch.stg_clients;")

        execute_values(cur, insert_sql, rows)

    conn.commit()

    log.info("Inserted %d staging rows", len(rows))

    return len(rows)



def load_products(conn, path: Path) -> int:
    """Load clients.csv into scratch.stg_products."""

    df = pd.read_csv(
        path,
        sep="|",
        dtype=str,
        skiprows=1,
        header=None
    )

    df.columns = [
        "product_code",
        "product_name",
        "tier",
        "status"
    ]

    log.info("Loading %d product rows from %s", len(df), path)

    insert_sql = """
        -- Insert from staging into scratch.stg_products
    INSERT INTO scratch.stg_products (
        product_code,
        product_name,
        tier,
        status
    )
        VALUES %s
    """

    def _row(r):
        return (
            r.get("product_code"),
            r.get("product_name"),
            r.get("tier"),
            r.get("status"),
        )

    rows = [_row(r) for _, r in df.iterrows()]

    with conn.cursor() as cur:

        # Optional for repeatable loads
        cur.execute("TRUNCATE TABLE scratch.stg_products;")

        execute_values(cur, insert_sql, rows)

    conn.commit()

    log.info("Inserted %d staging rows", len(rows))

    return len(rows)


def load_lapses(conn, path: Path) -> int:
    """Load health_lapses.parquet into scratch.stg_health_lapses."""

    df = pd.read_parquet(path)

    df.columns = [
        c.strip().lower().replace(" ", "_")
        for c in df.columns
    ]

    log.info("Loading %d lapse rows from %s", len(df), path)

    insert_sql = """
        INSERT INTO scratch.stg_health_lapses (
            client_id,
            product_code,
            lapse_date,
            policy_id,
            premium_amount,
            lapse_status
        )
        VALUES %s
    """

    def _row(r):
        return (
            r.get("client_id"),
            r.get("product_code"),
            r.get("lapse_date"),
            r.get("policy_id"),
            r.get("premium_amount"),
            r.get("lapse_status"),
        )

    rows = [_row(r) for _, r in df.iterrows()]

    with conn.cursor() as cur:

        # Optional for repeatable loads
        cur.execute("TRUNCATE TABLE scratch.stg_health_lapses;")

        execute_values(cur, insert_sql, rows)

    conn.commit()

    log.info("Inserted %d staging rows", len(rows))

    return len(rows)


if __name__ == "__main__":
    conn = get_connection()
    try:
        load_clients(conn,  DATA_DIR / "clients.csv")
        load_products(conn, DATA_DIR / "health_products.txt")
        load_lapses(conn, DATA_DIR / "health_lapses.parquet")
        log.info("All data loaded successfully.")
    finally:
        conn.close()
