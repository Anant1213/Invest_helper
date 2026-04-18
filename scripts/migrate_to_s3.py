#!/usr/bin/env python3
"""
Migrate AssetEra market data to S3 (Parquet format).

This script is needed when Supabase/PostgreSQL is unavailable.
It re-fetches 10 years of daily OHLCV for all tickers via yfinance
and writes two parquet files to S3:

  s3://{DATA_BUCKET}/market/us_prices.parquet       — US ETFs
  s3://{DATA_BUCKET}/market/us_equity_prices.parquet — US equities

Then optionally runs the analytics pipeline to populate:
  s3://{DATA_BUCKET}/analytics/{module}_{market}.parquet

Usage:
    python scripts/migrate_to_s3.py                  # full migration
    python scripts/migrate_to_s3.py --skip-etf       # equities only
    python scripts/migrate_to_s3.py --skip-equity    # ETFs only
    python scripts/migrate_to_s3.py --skip-analytics # prices only, no analytics
    python scripts/migrate_to_s3.py --ticker AAPL    # single equity
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv()

import numpy as np
import pandas as pd
import yfinance as yf

from backend.db.s3_store import is_enabled, put_parquet, read_parquet, key_exists, market_key

# ── Tickers ──────────────────────────────────────────────────────────

# US ETFs / fund tickers (same as existing allowlist + extras)
US_ETF_TICKERS = [
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "VEA", "VWO", "EFA", "EEM",
    "GLD", "SLV", "TLT", "IEF", "LQD", "HYG", "USO", "XLF", "XLE", "XLK",
    "XLV", "XLI", "XLY", "XLP", "XLB", "XLU", "XLRE", "VNQ", "ARKK",
]

LARGE_CAP = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "JPM", "LLY",
    "V",    "UNH",  "XOM",  "MA",   "JNJ",   "PG",   "HD",   "COST", "MRK", "ABBV",
    "WMT",  "BAC",  "NFLX", "CRM",  "CVX",   "AMD",  "ORCL", "PEP",  "KO",  "TMO",
    "ACN",  "MCD",  "CSCO", "WFC",  "GE",    "NOW",  "ADBE", "TXN",  "QCOM","DHR",
    "PM",   "CAT",  "AMGN", "INTU", "SPGI",  "MS",   "GS",   "IBM",  "RTX", "BRK-B",
]
MID_CAP = [
    "DECK","SAIA","BURL","GNRC","CLH",   "TXRH","KTOS","LSTR","MATX","BJ",
    "SFM", "ELS", "ITT", "AWI", "NVT",   "UFPI","ATI", "WMS", "GATX","MTB",
    "CINF","CFG", "ZION","RJF", "NTRS",  "AIZ", "STE", "PNW", "ATO", "WTFC",
    "RGEN","BLD", "EXPO","PRI", "HLI",   "OHI", "LCII","HLNE","RNR", "RGLD",
    "CBSH","BOOT","CUBE","LPLA","NEU",   "WEX", "SBCF","MMSI","CRVL","FHB",
]
SMALL_CAP = [
    "ABM",  "AMSF","CAKE","CATO","CENX",  "CHCO","CSWC","DLB", "DXPE","FCPT",
    "FFIN", "HWKN","IPAR","JJSF","KALU",  "KFRC","LGND","MRTN","MTRN","NTGR",
    "OFG",  "PKOH","PRK", "PRDO","RUSHA", "YORW","SMPL","STBA","SYBT","TRMK",
    "UFPT", "WEYS","ZEUS","BANF","HFWA",  "JELD","DGII","ASO", "BLBD","CAL",
    "CULP", "FRPH","HAIN","NMIH","LQDT",  "RAMP","MGNI","DIN", "COFS","SPFI",
]

START_DATE = (date.today() - timedelta(days=365 * 10 + 5)).isoformat()
END_DATE   = date.today().isoformat()


# ── Fetch helpers ─────────────────────────────────────────────────────

def _fetch(ticker: str) -> pd.DataFrame | None:
    try:
        raw = yf.download(
            ticker, start=START_DATE, end=END_DATE,
            interval="1d", progress=False, auto_adjust=True,
        )
        if raw is None or raw.empty:
            return None
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)
        raw = raw.reset_index()
        raw = raw.rename(columns={"index": "Date", "Datetime": "Date"})
        raw["Date"] = pd.to_datetime(raw["Date"], utc=True).dt.tz_convert(None)
        raw["Ticker"] = ticker.upper()
        for c in ["Open", "High", "Low", "Close", "Volume"]:
            if c not in raw.columns:
                raw[c] = np.nan
        raw["AdjClose"] = raw.get("Close", np.nan)
        return raw[["Date", "Ticker", "Open", "High", "Low", "Close", "AdjClose", "Volume"]].dropna(subset=["Close"])
    except Exception as ex:
        print(f"  ERROR  {ticker}: {ex}")
        return None


# ── Batch loader → S3 ─────────────────────────────────────────────────

def _load_batch(
    tickers: list[str],
    key: str,
    extra_col: dict | None = None,
    delay: float = 0.3,
    label: str = "",
) -> int:
    """Fetch all tickers, merge into existing parquet (if any), write to S3."""
    frames: list[pd.DataFrame] = []
    ok = fail = 0

    for ticker in tickers:
        df = _fetch(ticker)
        if df is None or df.empty:
            print(f"  SKIP  {ticker:<12} — no data")
            fail += 1
            continue
        if extra_col:
            for col, val in extra_col.items():
                df[col] = val
        frames.append(df)
        print(f"  OK    {ticker:<12} {len(df):>5} rows  {df['Date'].min().date()} → {df['Date'].max().date()}")
        ok += 1
        time.sleep(delay)

    if not frames:
        return 0

    new_data = pd.concat(frames, ignore_index=True)

    # Merge with existing parquet if present
    if key_exists(key):
        existing = read_parquet(key)
        pk = ["Date", "Ticker"]
        combined = pd.concat([existing, new_data], ignore_index=True)
        combined = combined.drop_duplicates(subset=pk, keep="last")
    else:
        combined = new_data

    combined = combined.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    put_parquet(key, combined)
    print(f"\n  {label}: {ok} ok / {fail} skip — wrote {len(combined):,} rows to s3:{key}")
    return ok


# ── CLI ───────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Migrate AssetEra market data to S3 parquet.")
    p.add_argument("--skip-etf",       action="store_true", help="Skip ETF/fund data")
    p.add_argument("--skip-equity",    action="store_true", help="Skip individual equities")
    p.add_argument("--skip-analytics", action="store_true", help="Skip analytics computation")
    p.add_argument("--ticker",         default="",          help="Single equity ticker")
    p.add_argument("--delay",          type=float, default=0.3, help="Delay between yfinance calls (s)")
    return p.parse_args()


def main() -> int:
    if not is_enabled():
        print("ERROR: S3 not configured. Set DATA_BUCKET + AWS credentials in .env")
        return 1

    from backend.db.s3_store import BUCKET, REGION
    print(f"S3 target : s3://{BUCKET}  (region: {REGION})")
    print(f"Date range: {START_DATE} → {END_DATE}\n")

    args = parse_args()

    # ── Single-ticker mode ─────────────────────────────────────────────
    if args.ticker:
        t = args.ticker.upper()
        cap = "LARGE" if t in LARGE_CAP else "MID" if t in MID_CAP else "SMALL"
        key = market_key("US_EQ")
        _load_batch([t], key, extra_col={"CapCategory": cap}, delay=args.delay, label=f"Single {t}")
        return 0

    # ── ETF data ───────────────────────────────────────────────────────
    if not args.skip_etf:
        print("═" * 60)
        print("US ETFs / Fund data → market/us_prices.parquet")
        print("═" * 60)
        _load_batch(US_ETF_TICKERS, market_key("US"), delay=args.delay, label="US ETFs")

    # ── Equity data ────────────────────────────────────────────────────
    if not args.skip_equity:
        key_eq = market_key("US_EQ")
        for cap, tickers in [("LARGE", LARGE_CAP), ("MID", MID_CAP), ("SMALL", SMALL_CAP)]:
            print(f"\n{'═'*60}")
            print(f"{cap} CAP equities ({len(tickers)} tickers)")
            print("═" * 60)
            _load_batch(tickers, key_eq, extra_col={"CapCategory": cap},
                        delay=args.delay, label=f"{cap} CAP")

    # ── Analytics pipeline ─────────────────────────────────────────────
    if not args.skip_analytics:
        print(f"\n{'═'*60}")
        print("Running analytics pipeline (S3 mode)…")
        print("═" * 60)
        try:
            from backend.stock_research.analytics.pipeline import run as run_analytics
            summary = run_analytics()
            for market, mods in summary.items():
                print(f"  {market}:")
                for mod, rows in mods.items():
                    print(f"    {mod}: {rows:,} rows")
        except Exception as e:
            print(f"  Analytics failed: {e}")
            return 1

    print("\nMigration complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
