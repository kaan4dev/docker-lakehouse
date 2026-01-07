import os
import pandas as pd
from io import BytesIO
from minio import Minio

client = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_ROOT_USER", "minio"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minio12345"),
    secure=False,
)

bucket = os.getenv("MINIO_BUCKET", "lake")

obj = client.get_object(bucket, "raw/sales.csv")
csv_bytes = obj.read()
obj.close()
obj.release_conn()

df = pd.read_csv(BytesIO(csv_bytes))
df["sale_date"] = pd.to_datetime(df["sale_ts"]).dt.date

out = BytesIO()
df.to_parquet(out, index=False)
out.seek(0)

client.put_object(bucket, "processed/sales.parquet", out, length=out.getbuffer().nbytes, content_type="application/octet-stream")
print("Uploaded processed/sales.parquet", flush=True)
