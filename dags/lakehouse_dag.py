from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="lakehouse_postgres_minio_duckdb",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    seed = BashOperator(
        task_id="seed_postgres",
        bash_command="python /opt/airflow/etl/scripts/seed_postgres.py",
    )

    extract = BashOperator(
        task_id="extract_to_minio",
        bash_command="python /opt/airflow/etl/scripts/extract_to_minio.py",
    )

    transform = BashOperator(
        task_id="transform_to_parquet",
        bash_command="python /opt/airflow/etl/scripts/transform_to_parquet.py",
    )

    query = BashOperator(
        task_id="duckdb_query_from_minio",
        bash_command=r"""
docker run --rm --network ${DOCKER_NETWORK} duckdb/duckdb:1.1.3 /duckdb -c "
INSTALL httpfs;
LOAD httpfs;
SET s3_region='us-east-1';
SET s3_url_style='path';
SET s3_endpoint='minio:9000';
SET s3_access_key_id='minio';
SET s3_secret_access_key='minio12345';
SET s3_use_ssl=false;
SELECT sale_date, COUNT(*) cnt, ROUND(SUM(amount),2) revenue
FROM read_parquet('s3://lake/processed/sales.parquet')
GROUP BY 1
ORDER BY 1;
"
""",
    )

    seed >> extract >> transform >> query
