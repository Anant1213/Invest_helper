"""
datalayer.s3
────────────
Zone-aware S3 client.

All writes go through this module so key patterns stay consistent.
Keys are relative to DATA_BUCKET; full URIs are returned on writes.

Zone layout:
  raw/        source-faithful JSON.gz payloads
  curated/    canonical-schema typed Parquet (snappy)
  features/   derived indicator / feature Parquet (snappy)
  control/    catalog, run manifests, quality reports
"""
from __future__ import annotations

import gzip
import io
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from datalayer.schemas import DATA_BUCKET, AWS_REGION

logger = logging.getLogger(__name__)

_client = None


# ── Connectivity ──────────────────────────────────────────────────────

def is_configured() -> bool:
    """True when bucket + credentials are present."""
    return bool(
        DATA_BUCKET and (
            os.getenv("AWS_ACCESS_KEY_ID")
            or os.getenv("AWS_ROLE_ARN")
            or _has_role_credentials()
        )
    )


def _has_role_credentials() -> bool:
    try:
        import boto3
        creds = boto3.Session().get_credentials()
        return creds is not None
    except Exception:
        return False


def client():
    global _client
    if _client is None:
        import boto3
        _client = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id    = os.getenv("AWS_ACCESS_KEY_ID")     or None,
            aws_secret_access_key= os.getenv("AWS_SECRET_ACCESS_KEY") or None,
            aws_session_token    = os.getenv("AWS_SESSION_TOKEN")     or None,
        )
    return _client


def uri(key: str) -> str:
    return f"s3://{DATA_BUCKET}/{key}"


# ── Key builders ─────────────────────────────────────────────────────
#
# raw/   {asset_class}/source={src}/symbol_or_series={id}/dt={date}/run_id={run}/payload.json.gz
# curated/{asset_class}/source={src}/symbol={id}/data.parquet
#         (symbol-first; enables efficient single-symbol reads)
# features/{asset_class}/feature_set={name}/symbol={id}/data.parquet
# control/catalog/datasets.json
# control/runs/date={date}/run_id={run}/status.json
# control/manifests/date={date}/run_id={run}/written_objects.json
# control/quality/date={date}/run_id={run}/quality_report.json
# control/checkpoints/{pipeline}/{asset_class}.json

def raw_key(asset_class: str, source: str, symbol_or_series: str,
            dt: str, run_id: str) -> str:
    return (
        f"raw/{asset_class}/source={source}"
        f"/symbol_or_series={symbol_or_series}"
        f"/dt={dt}/run_id={run_id}/payload.json.gz"
    )


def curated_key(asset_class: str, source: str, symbol_or_series: str) -> str:
    """Single parquet file per symbol — efficient full-history reads."""
    return f"curated/{asset_class}/source={source}/symbol={symbol_or_series}/data.parquet"


def features_key(asset_class: str, feature_set: str, symbol_or_series: str) -> str:
    return f"features/{asset_class}/feature_set={feature_set}/symbol={symbol_or_series}/data.parquet"


def catalog_key() -> str:
    return "control/catalog/datasets.json"


def run_status_key(run_date: str, run_id: str) -> str:
    return f"control/runs/date={run_date}/run_id={run_id}/status.json"


def run_manifest_key(run_date: str, run_id: str) -> str:
    return f"control/manifests/date={run_date}/run_id={run_id}/written_objects.json"


def quality_key(run_date: str, run_id: str) -> str:
    return f"control/quality/date={run_date}/run_id={run_id}/quality_report.json"


def checkpoint_key(pipeline: str, asset_class: str) -> str:
    return f"control/checkpoints/{pipeline}/{asset_class}.json"


# ── Raw bytes ─────────────────────────────────────────────────────────

def put_bytes(key: str, content: bytes,
              content_type: str = "application/octet-stream") -> str:
    client().put_object(
        Bucket=DATA_BUCKET, Key=key,
        Body=content, ContentType=content_type,
    )
    logger.debug("put_bytes → %s (%d bytes)", key, len(content))
    return uri(key)


def get_bytes(key: str) -> bytes:
    return client().get_object(Bucket=DATA_BUCKET, Key=key)["Body"].read()


# ── JSON (plain + gzip) ───────────────────────────────────────────────

def put_json(key: str, payload: Any) -> str:
    content = json.dumps(payload, indent=2, default=str).encode("utf-8")
    return put_bytes(key, content, "application/json")


def read_json(key: str) -> Any:
    return json.loads(get_bytes(key).decode("utf-8"))


def put_json_gz(key: str, payload: Any) -> str:
    raw = json.dumps(payload, default=str).encode("utf-8")
    compressed = gzip.compress(raw)
    return put_bytes(key, compressed, "application/gzip")


def read_json_gz(key: str) -> Any:
    return json.loads(gzip.decompress(get_bytes(key)).decode("utf-8"))


# ── Parquet ───────────────────────────────────────────────────────────

def put_parquet(key: str, df: pd.DataFrame) -> str:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy", engine="pyarrow")
    return put_bytes(key, buf.getvalue())


def read_parquet(key: str) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(get_bytes(key)), engine="pyarrow")


# ── Existence / listing ───────────────────────────────────────────────

def key_exists(key: str) -> bool:
    try:
        client().head_object(Bucket=DATA_BUCKET, Key=key)
        return True
    except Exception:
        return False


def list_keys(prefix: str) -> list[str]:
    paginator = client().get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=DATA_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def delete_key(key: str) -> None:
    client().delete_object(Bucket=DATA_BUCKET, Key=key)


# ── Utility ───────────────────────────────────────────────────────────

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()
