"""
backend.db
──────────
Storage backends: PostgreSQL, S3 + Parquet, DuckDB.

  postgres_store  — PostgreSQL connection + OHLCV helpers (legacy / fallback)
  s3_store        — boto3 S3 wrapper: read/write Parquet files
  duckdb_store    — per-thread DuckDB connections with S3/httpfs configured
"""
