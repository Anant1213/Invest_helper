"""
datalayer.features.macro
─────────────────────────
Compute macro / fixed-income features from curated FRED data.

Feature set name: macro_v1

Features computed (cross-series, written per series):
  Per individual series:
    mom_1m   — 1-month momentum (change in value)
    mom_3m   — 3-month momentum
    mom_12m  — 12-month momentum
    zscore_1y — z-score vs trailing 252 business days
    zscore_3y — z-score vs trailing 756 business days

  Cross-series features (written to a special "COMPOSITE" symbol):
    t10y2y_spread   — DGS10 - DGS2  (yield curve spread)
    t10y3m_spread   — DGS10 - DGS3MO
    t30y10y_spread  — DGS30 - DGS10
    hy_ig_spread    — (BAMLH0A0HYM2 proxy)
    rate_regime     — Fed Funds Rate percentile (0=low, 1=high)
    inflation_regime — CPI YoY percentile
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd

import datalayer.s3 as s3
from datalayer.schemas import ASSET_MACRO, FEATURES_COLUMNS

logger = logging.getLogger(__name__)

FEATURE_SET = "macro_v1"


# ── Per-series features ───────────────────────────────────────────────

def compute_series_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a curated FRED DataFrame (observation_date, value),
    compute per-series features and return wide DataFrame.
    """
    df = df.copy().sort_values("observation_date").reset_index(drop=True)
    v  = pd.to_numeric(df["value"], errors="coerce")

    df["mom_1m"]     = v.diff(1)
    df["mom_3m"]     = v.diff(3)
    df["mom_12m"]    = v.diff(12)
    df["zscore_1y"]  = (v - v.rolling(252, min_periods=30).mean()) / v.rolling(252, min_periods=30).std()
    df["zscore_3y"]  = (v - v.rolling(756, min_periods=63).mean()) / v.rolling(756, min_periods=63).std()

    return df


_SERIES_WINDOW = {
    "mom_1m":    "1m",
    "mom_3m":    "3m",
    "mom_12m":   "12m",
    "zscore_1y": "252d",
    "zscore_3y": "756d",
}


def _wide_to_long_fred(
    wide: pd.DataFrame,
    series_id: str,
    run_id: str,
    source_ref: str = "",
) -> pd.DataFrame:
    now  = datetime.now(timezone.utc).isoformat()
    rows = []
    for _, row in wide.iterrows():
        date_str = str(row["observation_date"])
        for feat, window in _SERIES_WINDOW.items():
            val = row.get(feat, np.nan)
            if pd.isna(val):
                continue
            rows.append({
                "date":             date_str,
                "asset_class":      ASSET_MACRO,
                "symbol_or_series": series_id,
                "feature_set":      FEATURE_SET,
                "feature_name":     feat,
                "feature_value":    float(val),
                "window":           window,
                "source_ref":       source_ref,
                "generated_at_utc": now,
                "run_id":           run_id,
            })
    return pd.DataFrame(rows, columns=FEATURES_COLUMNS)


# ── Cross-series (composite) features ────────────────────────────────

def compute_composite_features(
    series_map: dict[str, pd.DataFrame],
    run_id: str,
) -> pd.DataFrame:
    """
    Build composite macro features from multiple series.
    `series_map` is {series_id: curated_df}.
    Returns a long-format DataFrame for the COMPOSITE symbol.
    """
    now  = datetime.now(timezone.utc).isoformat()

    def _align(sid: str) -> pd.Series | None:
        df = series_map.get(sid)
        if df is None or df.empty:
            return None
        s = df.set_index("observation_date")["value"]
        return pd.to_numeric(s, errors="coerce")

    dgs10  = _align("DGS10")
    dgs2   = _align("DGS2")
    dgs3mo = _align("DGS3MO")
    dgs30  = _align("DGS30")
    dff    = _align("DFF")
    cpi    = _align("CPIAUCSL")

    if dgs10 is None:
        return pd.DataFrame(columns=FEATURES_COLUMNS)

    # Align all to dgs10 index
    idx = dgs10.index
    rows = []

    def _add_spread(name: str, a: pd.Series, b: pd.Series, window: str) -> None:
        spread = (a - b).reindex(idx)
        for date_str, val in spread.items():
            if pd.notna(val):
                rows.append({
                    "date":             date_str,
                    "asset_class":      ASSET_MACRO,
                    "symbol_or_series": "COMPOSITE",
                    "feature_set":      FEATURE_SET,
                    "feature_name":     name,
                    "feature_value":    float(val),
                    "window":           window,
                    "source_ref":       "",
                    "generated_at_utc": now,
                    "run_id":           run_id,
                })

    if dgs2 is not None:
        _add_spread("t10y2y_spread", dgs10, dgs2, "10y-2y")
    if dgs3mo is not None:
        _add_spread("t10y3m_spread", dgs10, dgs3mo, "10y-3m")
    if dgs30 is not None and dgs10 is not None:
        _add_spread("t30y10y_spread", dgs30, dgs10, "30y-10y")

    # Rate regime: rolling percentile of Fed Funds Rate
    if dff is not None:
        dff_r = dff.reindex(idx)
        def _pct_rank(s: pd.Series, window: int) -> pd.Series:
            return s.rolling(window, min_periods=int(window * 0.5)).apply(
                lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False
            )
        regime = _pct_rank(dff_r, 252)
        for date_str, val in regime.items():
            if pd.notna(val):
                rows.append({
                    "date":             date_str,
                    "asset_class":      ASSET_MACRO,
                    "symbol_or_series": "COMPOSITE",
                    "feature_set":      FEATURE_SET,
                    "feature_name":     "rate_regime",
                    "feature_value":    float(val),
                    "window":           "252d",
                    "source_ref":       "",
                    "generated_at_utc": now,
                    "run_id":           run_id,
                })

    # Inflation regime: CPI YoY change percentile
    if cpi is not None:
        cpi_r = cpi.reindex(idx)
        cpi_yoy = cpi_r.pct_change(12)
        regime_inf = _pct_rank(cpi_yoy, 252)
        for date_str, val in regime_inf.items():
            if pd.notna(val):
                rows.append({
                    "date":             date_str,
                    "asset_class":      ASSET_MACRO,
                    "symbol_or_series": "COMPOSITE",
                    "feature_set":      FEATURE_SET,
                    "feature_name":     "inflation_regime",
                    "feature_value":    float(val),
                    "window":           "252d",
                    "source_ref":       "",
                    "generated_at_utc": now,
                    "run_id":           run_id,
                })

    return pd.DataFrame(rows, columns=FEATURES_COLUMNS)


# ── Main entry points ─────────────────────────────────────────────────

def compute_and_write_series(
    series_id: str,
    run_id: str,
    manifest=None,
) -> bool:
    """Read curated FRED data, compute per-series features, write to features zone."""
    curated_key = s3.curated_key(ASSET_MACRO, "fred", series_id)
    try:
        df = s3.read_parquet(curated_key)
    except Exception as e:
        logger.error("[features/macro] %s — could not read curated: %s", series_id, e)
        return False

    if df.empty:
        logger.warning("[features/macro] %s — curated data is empty", series_id)
        return False

    try:
        wide    = compute_series_features(df)
        long_df = _wide_to_long_fred(wide, series_id, run_id, source_ref=curated_key)
    except Exception as e:
        logger.error("[features/macro] %s — compute failed: %s", series_id, e)
        return False

    feat_key = s3.features_key(ASSET_MACRO, FEATURE_SET, series_id)
    try:
        s3.put_parquet(feat_key, long_df)
        if manifest:
            manifest.record_write(feat_key)
        logger.info("[features/macro] %s — OK  rows=%d", series_id, len(long_df))
        return True
    except Exception as e:
        logger.error("[features/macro] %s — write failed: %s", series_id, e)
        return False


def compute_and_write_composite(run_id: str, manifest=None) -> bool:
    """Read all FRED curated data and compute cross-series composite features."""
    from datalayer.schemas import FRED_SERIES_IDS

    series_map: dict[str, pd.DataFrame] = {}
    for sid in FRED_SERIES_IDS:
        try:
            series_map[sid] = s3.read_parquet(s3.curated_key(ASSET_MACRO, "fred", sid))
        except Exception:
            pass

    if not series_map:
        logger.warning("[features/macro/composite] no curated FRED data found")
        return False

    try:
        long_df = compute_composite_features(series_map, run_id)
    except Exception as e:
        logger.error("[features/macro/composite] compute failed: %s", e)
        return False

    feat_key = s3.features_key(ASSET_MACRO, FEATURE_SET, "COMPOSITE")
    try:
        s3.put_parquet(feat_key, long_df)
        if manifest:
            manifest.record_write(feat_key)
        logger.info("[features/macro/composite] OK  rows=%d", len(long_df))
        return True
    except Exception as e:
        logger.error("[features/macro/composite] write failed: %s", e)
        return False
