"""
datalayer.ingest.fred
─────────────────────
Ingest one FRED macro series → raw JSON.gz + curated Parquet on S3.

FRED API endpoints used:
  /fred/series              — series metadata
  /fred/series/observations — time-series observations

Required env var: FRED_API_KEY
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import pandas as pd
import requests

import datalayer.s3 as s3
from datalayer.schemas import ASSET_MACRO, FRED_API_KEY, FRED_SERIES

logger = logging.getLogger(__name__)

_FRED_BASE = "https://api.stlouisfed.org/fred"
_TIMEOUT   = 30  # seconds

# ── FRED fetch helpers ────────────────────────────────────────────────

def _fred_get(endpoint: str, params: dict) -> dict:
    params = {**params, "api_key": FRED_API_KEY, "file_type": "json"}
    resp = requests.get(f"{_FRED_BASE}/{endpoint}", params=params, timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_series_meta(series_id: str) -> dict:
    """Fetch FRED series metadata."""
    data = _fred_get("series", {"series_id": series_id})
    slist = data.get("seriess", [])
    return slist[0] if slist else {}


def fetch_observations(
    series_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict]:
    """Fetch FRED observations as a list of dicts."""
    params: dict = {"series_id": series_id}
    if start_date:
        params["observation_start"] = start_date
    if end_date:
        params["observation_end"] = end_date
    data = _fred_get("series/observations", params)
    return data.get("observations", [])


# ── Normalization ─────────────────────────────────────────────────────

def normalize_fred(
    observations: list[dict],
    meta: dict,
    series_id: str,
    run_id: str,
) -> pd.DataFrame:
    """Convert FRED observations + metadata into canonical FRED schema."""
    if not observations:
        return pd.DataFrame()

    now = datetime.now(timezone.utc).isoformat()

    records = []
    for obs in observations:
        val_str = obs.get("value", ".")
        if val_str == ".":       # FRED uses "." for missing values
            continue
        try:
            value = float(val_str)
        except (ValueError, TypeError):
            continue

        records.append({
            "series_id":        series_id,
            "observation_date": obs.get("date", ""),
            "value":            value,
            "realtime_start":   obs.get("realtime_start", ""),
            "realtime_end":     obs.get("realtime_end", ""),
            "frequency":        meta.get("frequency_short", meta.get("frequency", "")),
            "units":            meta.get("units", ""),
            "seasonal_adjust":  meta.get("seasonal_adjustment_short", ""),
            "title":            meta.get("title", series_id),
            "source":           "fred",
            "ingested_at_utc":  now,
            "run_id":           run_id,
        })

    df = pd.DataFrame(records)
    if df.empty:
        return df

    df["observation_date"] = pd.to_datetime(df["observation_date"], errors="coerce")
    df = df.dropna(subset=["observation_date"]).sort_values("observation_date")
    df["observation_date"] = df["observation_date"].dt.date.astype(str)
    return df.reset_index(drop=True)


# ── Quality check ─────────────────────────────────────────────────────

def quality_check_fred(df: pd.DataFrame, series_id: str) -> dict:
    if df.empty:
        return {
            "rows": 0, "missing_pct": 1.0,
            "duplicate_rows": 0, "first_date": "", "last_date": "",
            "passed": False,
        }
    duplicates = int(df.duplicated(subset=["observation_date"]).sum())
    null_values = int(df["value"].isna().sum())
    missing_pct = null_values / len(df) if len(df) > 0 else 1.0
    return {
        "rows":           len(df),
        "missing_pct":    round(missing_pct, 4),
        "duplicate_rows": duplicates,
        "first_date":     str(df["observation_date"].min()),
        "last_date":      str(df["observation_date"].max()),
        "passed":         len(df) > 0 and missing_pct < 0.1 and duplicates == 0,
    }


# ── Main ingest entry point ───────────────────────────────────────────

def ingest_fred_series(msg: dict, manifest=None) -> bool:
    """
    Process one FRED ingest message.
    msg keys: run_id, symbol_or_series (= series_id), start_date, end_date
    """
    if not FRED_API_KEY:
        logger.error("[fred] FRED_API_KEY not set in environment")
        return False

    run_id    = msg["run_id"]
    series_id = msg["symbol_or_series"]
    start     = msg.get("start_date")
    end       = msg.get("end_date", date.today().isoformat())
    today     = date.today().isoformat()

    logger.info("[fred] ingesting %s  run=%s", series_id, run_id)

    # 1 — Fetch metadata + observations
    try:
        meta = fetch_series_meta(series_id)
        observations = fetch_observations(series_id, start, end)
    except requests.HTTPError as e:
        logger.error("[fred] %s — HTTP error: %s", series_id, e)
        return False
    except Exception as e:
        logger.error("[fred] %s — fetch error: %s", series_id, e)
        return False

    if not observations:
        logger.warning("[fred] %s — no observations returned", series_id)
        return False

    # 2 — Write raw JSON.gz
    raw_payload = {
        "series_id":    series_id,
        "source":       "fred",
        "asset_class":  ASSET_MACRO,
        "start_date":   start,
        "end_date":     end,
        "meta":         meta,
        "observations": observations,
    }
    raw_key = s3.raw_key(ASSET_MACRO, "fred", series_id, today, run_id)
    try:
        s3.put_json_gz(raw_key, raw_payload)
        if manifest:
            manifest.record_write(raw_key)
    except Exception as e:
        logger.warning("[fred] %s — raw write failed: %s", series_id, e)

    # 3 — Normalize to canonical schema
    df = normalize_fred(observations, meta, series_id, run_id)

    # 4 — Quality check
    qc = quality_check_fred(df, series_id)
    if manifest:
        manifest.record_quality(
            symbol=series_id, asset_class=ASSET_MACRO, **qc,
        )
    if not qc["passed"]:
        logger.warning("[fred] %s — quality check failed: %s", series_id, qc)

    # 5 — Write curated Parquet
    curated_key = s3.curated_key(ASSET_MACRO, "fred", series_id)
    try:
        s3.put_parquet(curated_key, df)
        if manifest:
            manifest.record_write(curated_key)
        logger.info("[fred] %s — OK  rows=%d", series_id, len(df))
        return True
    except Exception as e:
        logger.error("[fred] %s — curated write failed: %s", series_id, e)
        return False


# ── Convenience: ingest all FRED series in one call ───────────────────

def ingest_all_fred(run_id: str, start_date: str, manifest=None) -> dict:
    """Ingest all configured FRED series sequentially. Returns counts."""
    ok = fail = 0
    failed: list[str] = []
    for entry in FRED_SERIES:
        sid = entry["series_id"]
        msg = {
            "run_id":           run_id,
            "symbol_or_series": sid,
            "start_date":       start_date,
            "end_date":         date.today().isoformat(),
        }
        if ingest_fred_series(msg, manifest):
            ok += 1
        else:
            fail += 1
            failed.append(sid)
    return {"ok": ok, "fail": fail, "failed": failed}
