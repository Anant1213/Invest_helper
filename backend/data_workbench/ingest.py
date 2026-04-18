"""
backend.data_workbench.ingest
──────────────────────
File classification, parsing, normalization, and curated-parquet writing.

Supports: CSV, XLSX/XLS, JSON (records or tabular), Parquet.
Writes curated parquet to:
  • S3  (when DATA_BUCKET is set)
  • local temp file  (fallback, path stored in dataset.curated_key)
"""

from __future__ import annotations

import hashlib
import io
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import BinaryIO

import numpy as np
import pandas as pd

from backend.data_workbench.config import cfg
from backend.data_workbench.store import update_dataset

logger = logging.getLogger(__name__)

# ── Supported extensions ──────────────────────────────────────────────

STRUCTURED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".json", ".parquet"}


# ── Classification ────────────────────────────────────────────────────

def classify_file(filename: str) -> str:
    """Return 'structured' or 'unstructured'."""
    ext = Path(filename).suffix.lower()
    return "structured" if ext in STRUCTURED_EXTENSIONS else "unstructured"


# ── SHA-256 ───────────────────────────────────────────────────────────

def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


# ── Parsing ───────────────────────────────────────────────────────────

def parse_file(filename: str, content: bytes) -> pd.DataFrame | None:
    """
    Parse structured file bytes into a DataFrame.
    Returns None if parsing fails.
    """
    ext = Path(filename).suffix.lower()
    buf = io.BytesIO(content)
    try:
        if ext == ".csv":
            return pd.read_csv(buf)
        elif ext in (".xlsx", ".xls"):
            return pd.read_excel(buf)
        elif ext == ".json":
            buf.seek(0)
            raw = buf.read().decode("utf-8", errors="replace")
            # Try orient=records first, then default
            try:
                return pd.read_json(io.StringIO(raw), orient="records")
            except Exception:
                return pd.read_json(io.StringIO(raw))
        elif ext == ".parquet":
            return pd.read_parquet(buf)
        else:
            return None
    except Exception as e:
        logger.warning("parse_file failed for %s: %s", filename, e)
        return None


# ── Column normalization ──────────────────────────────────────────────

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def _normalize_col(name: str) -> str:
    """lowercase, replace non-alphanumeric runs with _, strip leading/trailing _"""
    s = str(name).strip().lower()
    s = _NON_ALNUM.sub("_", s).strip("_")
    return s or "col"


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names; deduplicate by appending _2, _3 …"""
    seen: dict[str, int] = {}
    new_cols = []
    for col in df.columns:
        norm = _normalize_col(str(col))
        if norm in seen:
            seen[norm] += 1
            norm = f"{norm}_{seen[norm]}"
        else:
            seen[norm] = 1
        new_cols.append(norm)
    df = df.copy()
    df.columns = new_cols
    return df


# ── Type inference ────────────────────────────────────────────────────

def infer_column_type(series: pd.Series) -> str:
    """
    Returns one of: numeric | categorical | datetime | boolean | text | mixed
    """
    s = series.dropna()
    if s.empty:
        return "mixed"
    if pd.api.types.is_bool_dtype(s):
        return "boolean"
    if pd.api.types.is_numeric_dtype(s):
        return "numeric"
    if pd.api.types.is_datetime64_any_dtype(s):
        return "datetime"
    # Try parsing as datetime
    if s.dtype == object:
        try:
            pd.to_datetime(s.head(50), infer_datetime_format=True)
            return "datetime"
        except Exception:
            pass
    # Cardinality heuristic
    unique_ratio = s.nunique() / len(s)
    avg_len = s.astype(str).str.len().mean()
    if unique_ratio < 0.1 and avg_len < 50:
        return "categorical"
    if avg_len > 80:
        return "text"
    return "categorical"


# ── Semantic labelling ────────────────────────────────────────────────

_SEMANTIC_PATTERNS = {
    "id":        re.compile(r"\b(id|_id|uuid|key)\b", re.I),
    "email":     re.compile(r"\bemail\b", re.I),
    "phone":     re.compile(r"\b(phone|mobile|tel)\b", re.I),
    "date":      re.compile(r"\b(date|time|timestamp|at|_on|day|month|year)\b", re.I),
    "amount":    re.compile(r"\b(amount|revenue|sales|price|cost|value|total)\b", re.I),
    "country":   re.compile(r"\b(country|nation|region|state|city|location)\b", re.I),
    "name":      re.compile(r"\b(name|title|label|description)\b", re.I),
    "category":  re.compile(r"\b(category|type|class|segment|group|status)\b", re.I),
    "quantity":  re.compile(r"\b(count|quantity|qty|num|number)\b", re.I),
}


def infer_semantic_label(col_name: str, series: pd.Series) -> str:
    for label, pattern in _SEMANTIC_PATTERNS.items():
        if pattern.search(col_name):
            return label
    # High-cardinality text columns → likely free text / id
    if series.dtype == object and series.nunique() / max(len(series), 1) > 0.9:
        return "id"
    return ""


# ── PII detection ─────────────────────────────────────────────────────

_PII_LABELS = {"email", "phone", "id"}


def is_pii(semantic_label: str, col_name: str) -> bool:
    if semantic_label in _PII_LABELS:
        return True
    col_lower = col_name.lower()
    return any(kw in col_lower for kw in ("ssn", "passport", "nric", "dob", "birth", "address", "zip", "postal"))


# ── Main ingest function ──────────────────────────────────────────────

def ingest(
    dataset_id: str,
    filename: str,
    content: bytes,
    sample_rows: int | None = None,
) -> dict:
    """
    Parse, normalize, and write curated parquet for a dataset.
    Updates the dataset record with row/column counts and curated key.
    Returns a summary dict.
    """
    if sample_rows is None:
        sample_rows = cfg().profile_sample_rows

    df = parse_file(filename, content)
    if df is None:
        return {"ok": False, "error": f"Could not parse {filename}"}

    # Normalize columns
    df = normalize_columns(df)

    total_rows = len(df)
    total_cols = len(df.columns)

    # Write curated parquet
    curated_key = _write_curated(dataset_id, df)

    # Write sample parquet (for fast profiling)
    if len(df) > sample_rows:
        sample_df = df.sample(n=sample_rows, random_state=42)
    else:
        sample_df = df

    sample_key = curated_key.replace("/data.parquet", "/sample.parquet")
    _write_curated_raw(dataset_id, sample_df, sample_key)

    # Update dataset metadata
    update_dataset(
        dataset_id,
        row_count=total_rows,
        column_count=total_cols,
        curated_key=curated_key,
        status="profiled",
    )

    return {
        "ok": True,
        "rows": total_rows,
        "columns": total_cols,
        "curated_key": curated_key,
        "sample_key": sample_key,
        "df": df,
        "sample_df": sample_df,
    }


def _write_curated(dataset_id: str, df: pd.DataFrame) -> str:
    key = f"datahub/{dataset_id}/curated/data.parquet"
    return _write_curated_raw(dataset_id, df, key)


def _write_curated_raw(dataset_id: str, df: pd.DataFrame, key: str) -> str:
    """Write parquet to S3 if enabled, else save locally and return path."""
    try:
        from backend.db.s3_store import is_enabled as s3_on, put_parquet
        if s3_on():
            put_parquet(key, df)
            return key
    except Exception:
        pass

    # Local fallback
    local_dir = Path(cfg().sqlite_path).parent / "curated" / dataset_id
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = str(local_dir / Path(key).name)
    df.to_parquet(local_path, index=False, compression="snappy", engine="pyarrow")
    return local_path


def read_curated(dataset_id: str, key_or_path: str, sample: bool = False) -> pd.DataFrame:
    """Load the curated (or sample) parquet for a dataset."""
    if sample:
        key_or_path = key_or_path.replace("/data.parquet", "/sample.parquet")
    try:
        from backend.db.s3_store import is_enabled as s3_on, read_parquet
        if s3_on() and key_or_path.startswith("datahub/"):
            return read_parquet(key_or_path)
    except Exception:
        pass
    # Local fallback
    if Path(key_or_path).exists():
        return pd.read_parquet(key_or_path)
    return pd.DataFrame()
