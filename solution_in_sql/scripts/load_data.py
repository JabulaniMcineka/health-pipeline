import psycopg2

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "database": "health_db",
    "user": "postgres"
}

def run_schema(conn, file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        sql = f.read()

    cursor = conn.cursor()

    try:
        cursor.execute(sql)   # IMPORTANT: NO SPLITTING
        conn.commit()
        print("SCHEMA CREATED SUCCESSFULLY")

    except Exception as e:
        conn.rollback()
        print(" ERROR:")
        print(e)

    cursor.close()

try:
    conn = psycopg2.connect(**DB_CONFIG)
    print("CONNECTED")

    run_schema(conn, "solution_in_sql/sql/ddl/load_data.sql")

    conn.close()

except Exception as e:
    print("CONNECTION ERROR:", e)