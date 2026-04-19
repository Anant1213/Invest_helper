"""
datalayer.catalog
─────────────────
control/catalog/datasets.json — a registry of known datasets in the lake.

Each entry describes one logical dataset:
  id, asset_class, source, description,
  curated_prefix, features_prefix,
  schema_version, last_updated_at

The catalog is loaded on first use and cached in memory.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import datalayer.s3 as s3
from datalayer.schemas import (
    ASSET_EQUITIES, ASSET_ETF, ASSET_FIXED_INCOME, ASSET_MACRO,
)

logger = logging.getLogger(__name__)

_CATALOG_VERSION = "1"

# Built-in dataset definitions — these are always present in a fresh catalog
_BUILTIN_DATASETS: list[dict] = [
    {
        "id":               "us_equities_ohlcv",
        "asset_class":      ASSET_EQUITIES,
        "source":           "yfinance",
        "description":      "150 US equities (50 large/mid/small cap) — 10-year daily OHLCV",
        "curated_prefix":   f"curated/{ASSET_EQUITIES}/source=yfinance/",
        "features_prefix":  f"features/{ASSET_EQUITIES}/feature_set=technical_v1/",
        "schema_version":   "ohlcv_v1",
        "schema_columns":   [
            "trade_date", "symbol", "asset_class",
            "open", "high", "low", "close", "adj_close", "volume",
            "currency", "exchange", "source", "ingested_at_utc", "run_id",
        ],
    },
    {
        "id":               "us_etf_ohlcv",
        "asset_class":      ASSET_ETF,
        "source":           "yfinance",
        "description":      "25 US ETFs (broad market, sectors, international) — 10-year daily OHLCV",
        "curated_prefix":   f"curated/{ASSET_ETF}/source=yfinance/",
        "features_prefix":  f"features/{ASSET_ETF}/feature_set=technical_v1/",
        "schema_version":   "ohlcv_v1",
        "schema_columns":   [
            "trade_date", "symbol", "asset_class",
            "open", "high", "low", "close", "adj_close", "volume",
            "currency", "exchange", "source", "ingested_at_utc", "run_id",
        ],
    },
    {
        "id":               "us_fixed_income_proxies_ohlcv",
        "asset_class":      ASSET_FIXED_INCOME,
        "source":           "yfinance",
        "description":      "10 bond ETF proxies (TLT, IEF, HYG, LQD, etc.) — 10-year daily OHLCV",
        "curated_prefix":   f"curated/{ASSET_FIXED_INCOME}/source=yfinance/",
        "features_prefix":  f"features/{ASSET_FIXED_INCOME}/feature_set=technical_v1/",
        "schema_version":   "ohlcv_v1",
        "schema_columns":   [
            "trade_date", "symbol", "asset_class",
            "open", "high", "low", "close", "adj_close", "volume",
            "currency", "exchange", "source", "ingested_at_utc", "run_id",
        ],
    },
    {
        "id":               "fred_macro_series",
        "asset_class":      ASSET_MACRO,
        "source":           "fred",
        "description":      "14 FRED macro series — Treasury yields, Fed rates, inflation, growth, credit",
        "curated_prefix":   f"curated/{ASSET_MACRO}/source=fred/",
        "features_prefix":  f"features/{ASSET_MACRO}/feature_set=macro_v1/",
        "schema_version":   "fred_v1",
        "schema_columns":   [
            "series_id", "observation_date", "value",
            "realtime_start", "realtime_end", "frequency",
            "units", "seasonal_adjust", "title",
            "source", "ingested_at_utc", "run_id",
        ],
    },
]


def _empty_catalog() -> dict:
    return {
        "version":       _CATALOG_VERSION,
        "created_at":    datetime.now(timezone.utc).isoformat(),
        "last_updated":  datetime.now(timezone.utc).isoformat(),
        "datasets":      {d["id"]: d for d in _BUILTIN_DATASETS},
    }


# ── Public API ────────────────────────────────────────────────────────

def load() -> dict:
    """Load catalog from S3, or return built-in defaults if not yet initialized."""
    try:
        return s3.read_json(s3.catalog_key())
    except Exception:
        logger.debug("[catalog] not found in S3 — using built-in defaults")
        return _empty_catalog()


def init(overwrite: bool = False) -> dict:
    """Write the built-in catalog to S3 if it doesn't exist (or overwrite=True)."""
    if not overwrite and s3.key_exists(s3.catalog_key()):
        logger.info("[catalog] already exists — skipping init")
        return load()
    catalog = _empty_catalog()
    s3.put_json(s3.catalog_key(), catalog)
    logger.info("[catalog] initialized with %d built-in datasets", len(catalog["datasets"]))
    return catalog


def register_dataset(dataset: dict[str, Any]) -> None:
    """Add or update a dataset entry in the catalog."""
    catalog = load()
    ds_id = dataset.get("id")
    if not ds_id:
        raise ValueError("dataset must have an 'id' field")
    catalog["datasets"][ds_id] = {**dataset, "registered_at": s3.now_utc()}
    catalog["last_updated"] = s3.now_utc()
    s3.put_json(s3.catalog_key(), catalog)
    logger.info("[catalog] registered dataset: %s", ds_id)


def get_dataset(dataset_id: str) -> dict | None:
    return load().get("datasets", {}).get(dataset_id)


def list_datasets() -> list[dict]:
    return list(load().get("datasets", {}).values())


def touch_dataset(dataset_id: str, run_id: str, symbol_count: int) -> None:
    """Update last_refreshed metadata on a dataset after a successful run."""
    catalog = load()
    ds = catalog.get("datasets", {}).get(dataset_id)
    if ds:
        ds["last_refreshed_run_id"] = run_id
        ds["last_refreshed_at"]     = s3.now_utc()
        ds["symbol_count"]          = symbol_count
        catalog["last_updated"]     = s3.now_utc()
        s3.put_json(s3.catalog_key(), catalog)
