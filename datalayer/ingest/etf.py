"""
datalayer.ingest.etf
─────────────────────
Ingest one ETF ticker → raw JSON.gz + curated Parquet on S3.
"""
from __future__ import annotations

import logging
from datetime import date

import datalayer.s3 as s3
from datalayer.ingest.base import fetch_ohlcv, normalize_ohlcv, quality_check
from datalayer.schemas import ASSET_ETF

logger = logging.getLogger(__name__)


def ingest_etf(msg: dict, manifest=None) -> bool:
    run_id   = msg["run_id"]
    ticker   = msg["symbol_or_series"]
    start    = msg.get("start_date")
    end      = msg.get("end_date", date.today().isoformat())
    interval = msg.get("interval", "1d")
    today    = date.today().isoformat()

    logger.info("[etf] ingesting %s  run=%s", ticker, run_id)

    raw = fetch_ohlcv(ticker, start, end, interval)
    if raw is None or raw.empty:
        logger.warning("[etf] %s — no data", ticker)
        return False

    raw_payload = {
        "ticker":     ticker,
        "asset_class": ASSET_ETF,
        "source":     "yfinance",
        "start_date": start,
        "end_date":   end,
        "rows":       len(raw),
        "records":    raw.to_dict(orient="records"),
    }
    raw_key = s3.raw_key(ASSET_ETF, "yfinance", ticker, today, run_id)
    try:
        s3.put_json_gz(raw_key, raw_payload)
        if manifest:
            manifest.record_write(raw_key)
    except Exception as e:
        logger.warning("[etf] %s — raw write failed: %s", ticker, e)

    df = normalize_ohlcv(raw, ticker, ASSET_ETF, run_id, currency="USD")

    qc = quality_check(df)
    if manifest:
        manifest.record_quality(symbol=ticker, asset_class=ASSET_ETF, **qc)
    if not qc["passed"]:
        logger.warning("[etf] %s — quality check failed: %s", ticker, qc)

    curated_key = s3.curated_key(ASSET_ETF, "yfinance", ticker)
    try:
        s3.put_parquet(curated_key, df)
        if manifest:
            manifest.record_write(curated_key)
        logger.info("[etf] %s — OK  rows=%d", ticker, len(df))
        return True
    except Exception as e:
        logger.error("[etf] %s — curated write failed: %s", ticker, e)
        return False
