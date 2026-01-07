import os
import psycopg2
import pandas as pd
from io import BytesIO
from minio import Minio

conn = psycopg2.connect(
    host="postgres",
    dbname=os.getenv("POSTGRES_DB", "source"),
    user=os.getenv("POSTGRES_USER", "source"),
    password=os.getenv("POSTGRES_PASSWORD", "source"),
)

df = pd.read_sql("SELECT * FROM sales;", conn)
conn.close()

buf = BytesIO()
df.to_csv(buf, index=False)
buf.seek(0)

client = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_ROOT_USER", "minio"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minio12345"),
    secure=False,
)

bucket = os.getenv("MINIO_BUCKET", "lake")
client.put_object(bucket, "raw/sales.csv", buf, length=buf.getbuffer().nbytes, content_type="text/csv")
print("Uploaded raw/sales.csv", flush=True)
