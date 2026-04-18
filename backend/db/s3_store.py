"""
backend.db.s3_store
────────────────
Thin boto3 wrapper for all S3 operations.
Keys are relative to DATA_BUCKET; s3_uri() adds the s3:// prefix.

Required env vars (when using S3 backend):
  DATA_BUCKET              — target S3 bucket name
  AWS_REGION               — e.g. ap-south-1  (default: us-east-1)
  AWS_ACCESS_KEY_ID        — static key (not needed when using IAM role)
  AWS_SECRET_ACCESS_KEY    — static secret
  AWS_SESSION_TOKEN        — optional session token (for temporary creds)

Market data key layout:
  market/us_prices.parquet
  market/us_equity_prices.parquet
  analytics/{module}_{market}.parquet   e.g. analytics/returns_US.parquet

Data Workbench key layout:
  datahub/{dataset_id}/raw/{filename}
  datahub/{dataset_id}/curated/data.parquet
  datahub/{dataset_id}/curated/sample.parquet
  datahub/{dataset_id}/artifacts/profile.json
  datahub/{dataset_id}/artifacts/quality.json
  datahub/{dataset_id}/artifacts/llm_summary.json
  datahub/{dataset_id}/artifacts/view_specs.json
"""

from __future__ import annotations

import io
import json
import logging
import os
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

BUCKET = os.getenv("DATA_BUCKET", "")
REGION = os.getenv("AWS_REGION", "us-east-1")

_client = None


# ── Connectivity ──────────────────────────────────────────────────────

def is_enabled() -> bool:
    """True if S3 bucket + credentials are configured."""
    return bool(
        BUCKET and (
            os.getenv("AWS_ACCESS_KEY_ID")
            or os.getenv("AWS_ROLE_ARN")
            or _has_instance_credentials()
        )
    )


def _has_instance_credentials() -> bool:
    """Probe for EC2/ECS instance role credentials (no-cost check)."""
    try:
        import boto3
        session = boto3.Session()
        creds = session.get_credentials()
        return creds is not None
    except Exception:
        return False


def get_client():
    global _client
    if _client is None:
        import boto3
        _client = boto3.client(
            "s3",
            region_name=REGION,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID") or None,
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY") or None,
            aws_session_token=os.getenv("AWS_SESSION_TOKEN") or None,
        )
    return _client


def s3_uri(key: str) -> str:
    return f"s3://{BUCKET}/{key}"


# ── Raw bytes ─────────────────────────────────────────────────────────

def put_bytes(key: str, content: bytes, content_type: str = "application/octet-stream") -> str:
    """Upload raw bytes. Returns s3://bucket/key URI."""
    get_client().put_object(
        Bucket=BUCKET, Key=key, Body=content, ContentType=content_type
    )
    logger.debug("put_bytes → %s (%d bytes)", key, len(content))
    return s3_uri(key)


def get_bytes(key: str) -> bytes:
    resp = get_client().get_object(Bucket=BUCKET, Key=key)
    return resp["Body"].read()


# ── Parquet ───────────────────────────────────────────────────────────

def put_parquet(key: str, df: pd.DataFrame) -> str:
    """Write DataFrame as snappy-compressed Parquet. Returns s3_uri."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy", engine="pyarrow")
    return put_bytes(key, buf.getvalue(), "application/octet-stream")


def read_parquet(key: str) -> pd.DataFrame:
    """Download and deserialise a Parquet file from S3."""
    data = get_bytes(key)
    return pd.read_parquet(io.BytesIO(data), engine="pyarrow")


# ── JSON artifacts ────────────────────────────────────────────────────

def put_json(key: str, payload: Any) -> str:
    content = json.dumps(payload, indent=2, default=str).encode("utf-8")
    return put_bytes(key, content, "application/json")


def read_json(key: str) -> Any:
    return json.loads(get_bytes(key).decode("utf-8"))


# ── Existence / listing ────────────────────────────────────────────────

def key_exists(key: str) -> bool:
    try:
        get_client().head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False


def list_keys(prefix: str) -> list[str]:
    paginator = get_client().get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def delete_key(key: str) -> None:
    get_client().delete_object(Bucket=BUCKET, Key=key)


# ── Convenience: market data keys ─────────────────────────────────────

MARKET_KEY_MAP = {
    "US":    "market/us_prices.parquet",
    "US_EQ": "market/us_equity_prices.parquet",
}

ANALYTICS_MODULES = ["returns", "risk", "momentum", "zscore", "technical"]


def analytics_key(module: str, market: str) -> str:
    """e.g. analytics/returns_US.parquet"""
    return f"analytics/{module}_{market}.parquet"


def market_key(market: str) -> str:
    return MARKET_KEY_MAP.get(market, f"market/{market.lower()}.parquet")
