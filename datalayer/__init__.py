"""
datalayer
─────────
AssetEra S3-first data layer.

This package provides:
  - Canonical schema contracts and asset universe lists
  - Zone-aware S3 client (raw / curated / features / control)
  - Ingest workers for equities, ETFs, fixed income proxies, and FRED macro
  - Technical and macro feature computation
  - SQS queue helpers (with local in-process fallback for dev)
  - Run manifest and checkpoint tracking
  - Dataset catalog management

Quick start
───────────
  from datalayer import enqueue_all, run_worker_loop, is_configured

  if is_configured():
      summary = enqueue_all(start_date="2016-01-01")      # publish all messages
      run_worker_loop("equities")                          # process one queue

  # Read curated data for a symbol:
  from datalayer.s3 import read_parquet, curated_key
  df = read_parquet(curated_key("equities", "yfinance", "AAPL"))

  # Read features:
  from datalayer.s3 import read_parquet, features_key
  df = read_parquet(features_key("equities", "technical_v1", "AAPL"))

Env vars
────────
  DATA_BUCKET                  — S3 bucket name
  AWS_REGION                   — e.g. ap-south-1
  AWS_ACCESS_KEY_ID            — static key (omit when using IAM role)
  AWS_SECRET_ACCESS_KEY        — static secret
  FRED_API_KEY                 — required for macro/FRED ingestion
  QUEUE_EQUITIES               — SQS queue name (default: assetera-ingest-equities)
  QUEUE_ETF                    — SQS queue name (default: assetera-ingest-etf)
  QUEUE_FIXED_INCOME           — SQS queue name
  QUEUE_FRED                   — SQS queue name
"""
from __future__ import annotations

# Re-export the most commonly used entry points so callers can do:
#   from datalayer import enqueue_all, run_worker_loop, is_configured

from datalayer.s3 import is_configured
from datalayer.enqueuer import enqueue_all, enqueue_asset_class
from datalayer.worker import run_worker_loop, process_message

__all__ = [
    "is_configured",
    "enqueue_all",
    "enqueue_asset_class",
    "run_worker_loop",
    "process_message",
]
