"""
backend.data_workbench.profile
───────────────────────
Deterministic dataset profiling.
Produces a rich profile_json dict and saves per-column stats.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from backend.data_workbench.ingest import infer_column_type, infer_semantic_label, is_pii
from backend.data_workbench.store import save_columns, save_profile

logger = logging.getLogger(__name__)


def build_profile(df: pd.DataFrame, dataset_id: str, context_hint: str = "") -> dict:
    """
    Build a comprehensive profile dict from a (possibly sampled) DataFrame.
    Side-effects: writes column stats + profile to SQLite.
    Returns the profile_json dict.
    """
    n_rows, n_cols = df.shape

    col_profiles: list[dict] = []
    date_cols: list[str] = []
    numeric_cols: list[str] = []
    categorical_cols: list[str] = []
    pii_cols: list[str] = []

    for col in df.columns:
        series = df[col]
        dtype  = infer_column_type(series)
        sem    = infer_semantic_label(col, series)
        pii    = is_pii(sem, col)

        null_count   = int(series.isna().sum())
        null_pct     = round(null_count / n_rows, 4) if n_rows > 0 else 0.0
        distinct     = int(series.nunique(dropna=True))
        unique_pct   = round(distinct / n_rows, 4) if n_rows > 0 else 0.0

        cp: dict[str, Any] = {
            "column_name":    col,
            "inferred_type":  dtype,
            "semantic_label": sem,
            "is_pii":         pii,
            "null_count":     null_count,
            "null_pct":       null_pct,
            "distinct_count": distinct,
            "unique_pct":     unique_pct,
        }

        if dtype == "numeric":
            numeric_cols.append(col)
            s_clean = pd.to_numeric(series, errors="coerce").dropna()
            if not s_clean.empty:
                cp.update({
                    "min_value":  float(s_clean.min()),
                    "max_value":  float(s_clean.max()),
                    "mean_value": float(s_clean.mean()),
                    "std_value":  float(s_clean.std()),
                    "median":     float(s_clean.median()),
                    "p25":        float(s_clean.quantile(0.25)),
                    "p75":        float(s_clean.quantile(0.75)),
                    "p95":        float(s_clean.quantile(0.95)),
                    "skewness":   float(s_clean.skew()),
                    "kurtosis":   float(s_clean.kurtosis()),
                })

        elif dtype == "categorical":
            categorical_cols.append(col)
            top = series.value_counts(dropna=True).head(10)
            cp["top_values"] = {str(k): int(v) for k, v in top.items()}
            if not top.empty:
                cp["min_value"] = str(series.dropna().min())
                cp["max_value"] = str(series.dropna().max())

        elif dtype == "datetime":
            date_cols.append(col)
            try:
                parsed = pd.to_datetime(series, errors="coerce").dropna()
                if not parsed.empty:
                    cp["min_value"] = str(parsed.min().date())
                    cp["max_value"] = str(parsed.max().date())
                    delta_days = (parsed.max() - parsed.min()).days
                    cp["date_range_days"] = delta_days
            except Exception:
                pass

        col_profiles.append(cp)
        if dtype in ("numeric", "categorical"):
            pass  # tracked above

    # Duplicate rows
    dup_count = int(df.duplicated().sum())
    dup_pct   = round(dup_count / n_rows, 4) if n_rows > 0 else 0.0

    # Numeric correlations (top 20 pairs)
    correlations: list[dict] = []
    if len(numeric_cols) >= 2:
        try:
            num_df = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
            corr = num_df.corr()
            pairs = (
                corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
                .stack()
                .abs()
                .sort_values(ascending=False)
                .head(20)
            )
            for (c1, c2), v in pairs.items():
                correlations.append({"col1": c1, "col2": c2, "abs_corr": round(float(v), 3)})
        except Exception:
            pass

    # Memory estimate
    try:
        mem_mb = round(df.memory_usage(deep=True).sum() / 1e6, 2)
    except Exception:
        mem_mb = 0.0

    profile = {
        "row_count":        n_rows,
        "column_count":     n_cols,
        "memory_mb":        mem_mb,
        "duplicate_rows":   dup_count,
        "duplicate_pct":    dup_pct,
        "date_columns":     date_cols,
        "numeric_columns":  numeric_cols,
        "categorical_columns": categorical_cols,
        "pii_columns":      pii_cols,
        "correlations":     correlations,
        "columns":          col_profiles,
        "context_hint":     context_hint,
    }

    # Persist column stats
    save_columns(dataset_id, col_profiles)

    return profile
