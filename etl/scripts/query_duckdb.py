import os 
import duckdb

MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minio12345")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lake")

con = duckdb.connect()

con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

con.execute(f"""
SET s3_region='us-east-1';
SET s3_url_style='path';
SET s3_endpoint='minio:9000';
SET s3_access_key_id='{MINIO_ROOT_USER}';
SET s3_secret_access_key='{MINIO_ROOT_PASSWORD}';
SET s3_use_ssl=false;
""")

q = f"""
SELECT sale_date, COUNT(*) cnt, ROUND(SUM(amount),2) revenue
FROM read_parquet('s3://{MINIO_BUCKET}/processed/sales.parquet')
GROUP BY 1
ORDER BY 1;
"""
print(con.execute(q).fetchdf())