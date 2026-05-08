import pandas as pd

df = pd.read_parquet(r"C:\Users\Jabulani.Mcineka\workspace\health-pipeline\data_files\health_lapses.parquet")

print(df.head())