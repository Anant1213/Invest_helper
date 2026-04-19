"""
datalayer.ingest.equities
─────────────────────────
Ingest one US equity ticker → raw JSON.gz + curated Parquet on S3.

Main entry point:
  ingest_equity(msg, manifest) → bool
"""
from __future__ import annotations

import logging
from datetime import date

import datalayer.s3 as s3
from datalayer.ingest.base import fetch_ohlcv, normalize_ohlcv, quality_check
from datalayer.schemas import ASSET_EQUITIES, EQUITIES_CAP

logger = logging.getLogger(__name__)


def ingest_equity(msg: dict, manifest=None) -> bool:
    """
    Process one equities ingest message.

    msg keys: run_id, symbol_or_series, start_date, end_date,
              interval, cap (optional)
    Returns True on success, False on failure.
    """
    run_id = msg["run_id"]
    ticker = msg["symbol_or_series"]
    start  = msg.get("start_date")
    end    = msg.get("end_date", date.today().isoformat())
    interval = msg.get("interval", "1d")
    cap    = msg.get("cap", EQUITIES_CAP.get(ticker, "UNKNOWN"))
    today  = date.today().isoformat()

    logger.info("[equity] ingesting %s  run=%s", ticker, run_id)

    # 1 — Fetch from yfinance
    raw = fetch_ohlcv(ticker, start, end, interval)
    if raw is None or raw.empty:
        logger.warning("[equity] %s — no data", ticker)
        return False

    # 2 — Write raw JSON.gz
    raw_payload = {
        "ticker":     ticker,
        "asset_class": ASSET_EQUITIES,
        "source":     "yfinance",
        "cap":        cap,
        "start_date": start,
        "end_date":   end,
        "interval":   interval,
        "rows":       len(raw),
        "records":    raw.to_dict(orient="records"),  # type: ignore[union-attr]
    }
    raw_key = s3.raw_key(ASSET_EQUITIES, "yfinance", ticker, today, run_id)
    try:
        s3.put_json_gz(raw_key, raw_payload)
        if manifest:
            manifest.record_write(raw_key)
    except Exception as e:
        logger.warning("[equity] %s — raw write failed: %s", ticker, e)
        # Non-fatal — continue to curated write

    # 3 — Normalize → canonical schema
    df = normalize_ohlcv(
        raw, ticker, ASSET_EQUITIES, run_id,
        currency="USD", exchange="",
    )
    # Attach cap tier as a metadata column (outside canonical schema)
    df["cap_tier"] = cap

    # 4 — Quality check
    qc = quality_check(df)
    if manifest:
        manifest.record_quality(
            symbol=ticker,
            asset_class=ASSET_EQUITIES,
            **qc,
        )
    if not qc["passed"]:
        logger.warning("[equity] %s — quality check failed: %s", ticker, qc)

    # 5 — Write curated Parquet
    curated_key = s3.curated_key(ASSET_EQUITIES, "yfinance", ticker)
    try:
        s3.put_parquet(curated_key, df)
        if manifest:
            manifest.record_write(curated_key)
        logger.info("[equity] %s — OK  rows=%d  → %s", ticker, len(df), curated_key)
        return True
    except Exception as e:
        logger.error("[equity] %s — curated write failed: %s", ticker, e)
        return False
