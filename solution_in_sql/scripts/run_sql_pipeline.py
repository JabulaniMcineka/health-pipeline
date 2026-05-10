import psycopg2
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "database": "health_db",
    "user": "postgres"
}

def run_schema(conn, file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        sql_script = f.read()

    cursor = conn.cursor()

    try:
        cursor.execute(sql_script)

        conn.commit()
        print(f"SUCCESS: {file_path}")

    except Exception as e:
        conn.rollback()
        print(f"ERROR in {file_path}:")
        print(e)

    finally:
        cursor.close()


if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    
    with conn.cursor() as cur:
        cur.execute("SELECT current_database(), current_schema();")
        print(cur.fetchone())

    print("CONNECTED")




    pipeline_files = [
        
        BASE_DIR / "sql/ddl/01_schema.sql",
        BASE_DIR / "sql/ddl/02_staging_schema.sql",
        BASE_DIR / "sql/ddl/03_populate_dims.sql",
        BASE_DIR / "sql/ddl/04_transformations_quality.sql",
    ]

    for file_path in pipeline_files:
        print(f"RUNNING: {file_path}")
        run_schema(conn, file_path)

        
    conn.close()