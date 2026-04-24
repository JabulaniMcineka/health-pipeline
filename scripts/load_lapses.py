import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

df = pd.read_parquet('data_files/health_lapses.parquet')
print(f"Loaded {len(df)} rows")
print(df['status'].unique())

conn = psycopg2.connect(
    host="localhost", port=5433,
    dbname="health_db", user="postgres", password="postgres"
)
cur = conn.cursor()

cur.execute("SELECT client_key, client_id FROM health.dim_clients")
clients = {str(row[1]): row[0] for row in cur.fetchall()}

cur.execute("SELECT product_key, product_code FROM health.dim_products LIMIT 1")
row = cur.fetchone()
default_product_key = row[0]
default_product_code = row[1]

client_ids = list(clients.keys())

STATUS_MAP = {
    'Active':      'LAPSED',
    'Pending':     'PENDING',
    'Reinstated':  'REINSTATED',
    'Cancelled':   'CANCELLED',
    'Inactive':    'LAPSED',
}

rows = []
for i, (_, r) in enumerate(df.iterrows()):
    client_id = client_ids[i % len(client_ids)]
    client_key = clients[client_id]
    lapse_date_key = int(pd.Timestamp(r['lapse_date']).strftime('%Y%m%d'))
    status = STATUS_MAP.get(str(r['status']), 'LAPSED')
    rows.append((
        client_key, default_product_key, lapse_date_key,
        client_id, default_product_code,
        status, float(r['premium_amount']),
        'health_lapses.parquet'
    ))

print(f"Inserting {len(rows)} rows")
execute_values(cur, """
    INSERT INTO health.fct_health_lapses
        (client_key, product_key, lapse_date_key, client_id, product_code,
         lapse_status, premium_at_lapse, source_file)
    VALUES %s
""", rows)
conn.commit()
cur.close()
conn.close()
print("Done!")
