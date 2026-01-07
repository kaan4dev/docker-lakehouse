# Docker Lakehouse (Airflow + Postgres + MinIO + DuckDB)

A minimal Docker-based lakehouse project that demonstrates a typical Data Engineering pipeline:

**Postgres (source) → Airflow (orchestration) → MinIO (S3-compatible data lake) → DuckDB (analytics)**

## Architecture

- **Postgres**: Source database
- **Airflow**: Orchestrates the pipeline
- **MinIO**: S3-compatible object storage
  - `raw/` → CSV
  - `processed/` → Parquet
- **DuckDB**: Runs analytical queries on Parquet via S3 endpoint (job container, not a service)

## Pipeline

1. Seed sample sales data into Postgres  
2. Extract data to MinIO as CSV (`raw/`)  
3. Transform CSV → Parquet (`processed/`)  
4. Query Parquet with DuckDB (daily aggregates)
