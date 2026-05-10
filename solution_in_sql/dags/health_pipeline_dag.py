from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

def load_data():
    """Load raw CSV files into PostgreSQL staging tables."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    import scripts.load_data as ld
    conn = ld.get_connection()
    try:
        ld.load_clients(conn, "data_files/clients.csv")
        ld.load_products(conn, "data_files/health_products.txt")
    finally:
        conn.close()

with DAG(
    dag_id="health_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    create_schema = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="health_db",
        sql="sql/ddl/02_staging_schema.sql"
    )

    load_staging = PythonOperator(
        task_id="load_staging",
        python_callable=load_data
    )

    build_dims = PostgresOperator(
        task_id="build_dimensions",
        postgres_conn_id="health_db",
        sql="sql/ddl/03_populate_dims.sql"
    )

    quality_checks = PostgresOperator(
        task_id="data_quality_checks",
        postgres_conn_id="health_db",
        sql="sql/ddl/04_transformations_quality.sql"
    )

    create_schema >> load_staging >> build_dims >> quality_checks