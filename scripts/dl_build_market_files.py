#!/usr/bin/env python3
"""
AssetEra — Build compatibility market/ files from curated/ zone.

Reads all per-symbol parquet files from:
  curated/etf/source=yfinance/symbol=*/data.parquet
  curated/fixed_income/source=yfinance/symbol=*/data.parquet
  curated/equities/source=yfinance/symbol=*/data.parquet

Combines them and writes two compatibility files used by the analytics pipeline:
  market/us_prices.parquet         (ETF + fixed income)
  market/us_equity_prices.parquet  (equities with CapCategory)

Run this after every datalayer ingest run, before running analytics:
  python scripts/dl_build_market_files.py
  python scripts/run_analytics.py

Usage:
  python scripts/dl_build_market_files.py
  python scripts/dl_build_market_files.py --skip-etf
  python scripts/dl_build_market_files.py --skip-equity
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _curated_to_pascal(df):
    """Rename curated columns (snake_case) → PascalCase for analytics compatibility."""
    import pandas as pd
    df = df.rename(columns={
        "trade_date": "Date",
        "symbol":     "Ticker",
        "open":       "Open",
        "high":       "High",
        "low":        "Low",
        "close":      "Close",
        "adj_close":  "Adj Close",
        "volume":     "Volume",
        "cap_tier":   "CapCategory",
    })
    df["Date"] = pd.to_datetime(df["Date"])
    return df


def _load_all_from_prefix(prefix: str) -> list:
    """List all symbol= keys under prefix and return list of DataFrames."""
    import datalayer.s3 as dl_s3
    import pandas as pd

    keys   = dl_s3.list_keys(prefix)
    frames = []
    symbols_found = []

    for key in keys:
        if not key.endswith("data.parquet"):
            continue
        try:
            df = dl_s3.read_parquet(key)
            if df.empty:
                continue
            frames.append(df)
            # Extract symbol name for logging
            for part in key.split("/"):
                if part.startswith("symbol="):
                    symbols_found.append(part[len("symbol="):])
                    break
        except Exception as e:
            logger.warning("Could not read %s: %s", key, e)

    logger.info("  Loaded %d symbols from %s", len(symbols_found), prefix)
    return frames


def build_us_prices() -> int:
    """Combine ETF + fixed_income curated files → market/us_prices.parquet"""
    import datalayer.s3 as dl_s3
    import pandas as pd
    from backend.db.s3_store import put_parquet, market_key

    logger.info("Building market/us_prices.parquet (ETF + fixed income)…")

    frames = []
    frames += _load_all_from_prefix("curated/etf/source=yfinance/")
    frames += _load_all_from_prefix("curated/fixed_income/source=yfinance/")

    if not frames:
        logger.error("No curated ETF or fixed income data found. Run the backfill first.")
        return 0

    combined = pd.concat(frames, ignore_index=True)
    combined = _curated_to_pascal(combined)

    # Keep only expected columns (drop datalayer metadata)
    keep_cols = ["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    keep_cols = [c for c in keep_cols if c in combined.columns]
    combined  = combined[keep_cols]
    combined  = combined.drop_duplicates(subset=["Date", "Ticker"], keep="last")
    combined  = combined.sort_values(["Ticker", "Date"]).reset_index(drop=True)

    key = market_key("US")
    put_parquet(key, combined)
    logger.info("Wrote %d rows (%d tickers) → s3:%s",
                len(combined), combined["Ticker"].nunique(), key)
    return len(combined)


def build_us_equity_prices() -> int:
    """Combine equity curated files → market/us_equity_prices.parquet"""
    import datalayer.s3 as dl_s3
    import pandas as pd
    from backend.db.s3_store import put_parquet, market_key

    logger.info("Building market/us_equity_prices.parquet (equities)…")

    frames = _load_all_from_prefix("curated/equities/source=yfinance/")

    if not frames:
        logger.error("No curated equity data found. Run the backfill first.")
        return 0

    combined = pd.concat(frames, ignore_index=True)
    combined = _curated_to_pascal(combined)

    # Ensure CapCategory column exists (from cap_tier in ingest)
    if "CapCategory" not in combined.columns:
        from datalayer.schemas import EQUITIES_CAP
        combined["CapCategory"] = combined["Ticker"].map(EQUITIES_CAP).fillna("UNKNOWN")

    keep_cols = ["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume", "CapCategory"]
    keep_cols = [c for c in keep_cols if c in combined.columns]
    combined  = combined[keep_cols]
    combined  = combined.drop_duplicates(subset=["Date", "Ticker"], keep="last")
    combined  = combined.sort_values(["Ticker", "Date"]).reset_index(drop=True)

    key = market_key("US_EQ")
    put_parquet(key, combined)
    logger.info("Wrote %d rows (%d tickers) → s3:%s",
                len(combined), combined["Ticker"].nunique(), key)
    return len(combined)


def parse_args():
    p = argparse.ArgumentParser(description="Build market/ files from curated/ datalayer zone")
    p.add_argument("--skip-etf",    action="store_true", help="Skip market/us_prices.parquet")
    p.add_argument("--skip-equity", action="store_true", help="Skip market/us_equity_prices.parquet")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    from datalayer.s3 import is_configured
    if not is_configured():
        logger.error("S3 not configured. Set DATA_BUCKET + AWS credentials in .env")
        return 1

    ok = True

    if not args.skip_etf:
        rows = build_us_prices()
        if rows == 0:
            logger.warning("market/us_prices.parquet — no rows written")
            ok = False

    if not args.skip_equity:
        rows = build_us_equity_prices()
        if rows == 0:
            logger.warning("market/us_equity_prices.parquet — no rows written")
            ok = False

    if ok:
        print("\nDone. Next step: python scripts/run_analytics.py")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
