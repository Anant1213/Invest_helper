"""
backend.data_workbench
──────────────────────
Data Workbench feature backend.

  config    — DataHubConfig dataclass + cfg() singleton
  store     — SQLite metadata store (projects, datasets, columns, profiles…)
  ingest    — File parsing + column classification + S3 upload
  profile   — Statistical profiling: distributions, correlations, null rates
  quality   — Data quality scoring: 7 rule checks, severity-graded issues
  llm       — GPT-4o-mini dataset narrative + KPI/view suggestions
  views     — Rule-based auto chart spec generation
  queries   — Safe DuckDB SQL execution over uploaded parquet files
"""
