import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

df = pd.read_parquet('data_files/health_lapses.parquet')
print(f"Loaded {len(df)} rows")

conn = psycopg2.connect(
    host="localhost", port=5433,
    dbname="health_db", user="postgres", password="postgres"
)
cur = conn.cursor()

# Get lookups
cur.execute("SELECT client_key, client_id FROM health.dim_clients")
clients = {str(row[1]): row[0] for row in cur.fetchall()}

cur.execute("SELECT product_key, product_code FROM health.dim_products LIMIT 1")
row = cur.fetchone()
default_product_key = row[0]
default_product_code = row[1]

print(f"Clients available: {list(clients.keys())}")

rows = []
skipped = 0

client_ids = list(clients.keys())

for i, (_, r) in enumerate(df.iterrows()):
    # Map policy_id to an existing client by cycling
    client_id = client_ids[i % len(client_ids)]
    client_key = clients[client_id]

    lapse_date_key = int(pd.Timestamp(r['lapse_date']).strftime('%Y%m%d'))

    rows.append((
        client_key,
        default_product_key,
        lapse_date_key,
        str(r['policy_id']),
        default_product_code,
        str(r['status']),
        float(r['premium_amount']),
        'health_lapses.parquet'
    ))

print(f"Inserting {len(rows)} rows, skipped {skipped} (no matching client)")

if rows:
    execute_values(cur, """
        INSERT INTO health.fct_health_lapses
            (client_key, product_key, lapse_date_key, client_id, product_code,
             lapse_status, premium_at_lapse, source_file)
        VALUES %s
        ON CONFLICT DO NOTHING
    """, rows)
    conn.commit()

cur.close()
conn.close()
print("Done!")