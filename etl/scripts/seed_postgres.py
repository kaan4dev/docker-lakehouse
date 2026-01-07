import os, random
import psycopg2
from datetime import datetime, timedelta

conn = psycopg2.connect(
    host="postgres",
    dbname=os.getenv("POSTGRES_DB", "source"),
    user=os.getenv("POSTGRES_USER", "source"),
    password=os.getenv("POSTGRES_PASSWORD", "source"),
)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS sales (
  id SERIAL PRIMARY KEY,
  sale_ts TIMESTAMP NOT NULL,
  customer_id INT NOT NULL,
  amount NUMERIC(10,2) NOT NULL
);
""")
cur.execute("SELECT COUNT(*) FROM sales;")
count = cur.fetchone()[0]

if count < 200:
    base = datetime(2026, 1, 1)
    for _ in range(200 - count):
        ts = base + timedelta(hours=random.randint(0, 240))
        cid = random.randint(1, 25)
        amount = round(random.uniform(5, 250), 2)
        cur.execute(
            "INSERT INTO sales(sale_ts, customer_id, amount) VALUES (%s,%s,%s)",
            (ts, cid, amount),
        )
    conn.commit()

cur.close()
conn.close()
print("Seeded sales table.", flush=True)
