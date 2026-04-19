"""
datalayer.ingest.fixed_income
──────────────────────────────
Ingest one fixed-income proxy ETF ticker → raw + curated on S3.
These are bond ETFs (TLT, IEF, HYG, etc.) used as fixed-income proxies.
"""
from __future__ import annotations

import logging
from datetime import date

import datalayer.s3 as s3
from datalayer.ingest.base import fetch_ohlcv, normalize_ohlcv, quality_check
from datalayer.schemas import ASSET_FIXED_INCOME

logger = logging.getLogger(__name__)


def ingest_fixed_income(msg: dict, manifest=None) -> bool:
    run_id   = msg["run_id"]
    ticker   = msg["symbol_or_series"]
    start    = msg.get("start_date")
    end      = msg.get("end_date", date.today().isoformat())
    interval = msg.get("interval", "1d")
    today    = date.today().isoformat()

    logger.info("[fi] ingesting %s  run=%s", ticker, run_id)

    raw = fetch_ohlcv(ticker, start, end, interval)
    if raw is None or raw.empty:
        logger.warning("[fi] %s — no data", ticker)
        return False

    raw_payload = {
        "ticker":      ticker,
        "asset_class": ASSET_FIXED_INCOME,
        "source":      "yfinance",
        "start_date":  start,
        "end_date":    end,
        "rows":        len(raw),
        "records":     raw.to_dict(orient="records"),
    }
    raw_key = s3.raw_key(ASSET_FIXED_INCOME, "yfinance", ticker, today, run_id)
    try:
        s3.put_json_gz(raw_key, raw_payload)
        if manifest:
            manifest.record_write(raw_key)
    except Exception as e:
        logger.warning("[fi] %s — raw write failed: %s", ticker, e)

    df = normalize_ohlcv(raw, ticker, ASSET_FIXED_INCOME, run_id, currency="USD")

    qc = quality_check(df)
    if manifest:
        manifest.record_quality(symbol=ticker, asset_class=ASSET_FIXED_INCOME, **qc)
    if not qc["passed"]:
        logger.warning("[fi] %s — quality check failed: %s", ticker, qc)

    curated_key = s3.curated_key(ASSET_FIXED_INCOME, "yfinance", ticker)
    try:
        s3.put_parquet(curated_key, df)
        if manifest:
            manifest.record_write(curated_key)
        logger.info("[fi] %s — OK  rows=%d", ticker, len(df))
        return True
    except Exception as e:
        logger.error("[fi] %s — curated write failed: %s", ticker, e)
        return False
