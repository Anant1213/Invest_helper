"""
datalayer.ingest.base
─────────────────────
Shared fetch + normalization helpers used by all ingest modules.

Public helpers
──────────────
  fetch_ohlcv(ticker, start, end, interval)  →  pd.DataFrame (canonical OHLCV)
  normalize_ohlcv(raw_df, ticker, asset_class, run_id)  →  pd.DataFrame
  quality_check(df)  →  dict
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── yfinance OHLCV fetch ──────────────────────────────────────────────

def fetch_ohlcv(
    ticker: str,
    start_date: str,
    end_date: str,
    interval: str = "1d",
) -> pd.DataFrame | None:
    """
    Download OHLCV from yfinance and return a minimally cleaned DataFrame.
    Returns None on failure or when no data is available.
    """
    try:
        import yfinance as yf
        raw = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            interval=interval,
            progress=False,
            auto_adjust=True,
        )
        if raw is None or raw.empty:
            logger.debug("[fetch_ohlcv] %s — no data returned", ticker)
            return None

        # Flatten multi-level columns (yfinance 0.2+)
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        raw = raw.reset_index()
        # Normalise date column name
        for col in ("Date", "Datetime", "index"):
            if col in raw.columns:
                raw = raw.rename(columns={col: "_date"})
                break

        raw["_date"] = pd.to_datetime(raw["_date"], utc=True).dt.tz_convert(None)

        # Ensure expected OHLCV columns exist
        for col in ("Open", "High", "Low", "Close", "Volume"):
            if col not in raw.columns:
                raw[col] = np.nan

        adj_close = raw.get("Adj Close", raw["Close"])

        return pd.DataFrame({
            "_date":     raw["_date"],
            "open":      pd.to_numeric(raw["Open"],   errors="coerce"),
            "high":      pd.to_numeric(raw["High"],   errors="coerce"),
            "low":       pd.to_numeric(raw["Low"],    errors="coerce"),
            "close":     pd.to_numeric(raw["Close"],  errors="coerce"),
            "adj_close": pd.to_numeric(adj_close,     errors="coerce"),
            "volume":    pd.to_numeric(raw["Volume"], errors="coerce"),
        }).dropna(subset=["close"])

    except Exception as e:
        logger.warning("[fetch_ohlcv] %s — error: %s", ticker, e)
        return None


# ── Canonical normalization ───────────────────────────────────────────

def normalize_ohlcv(
    raw: pd.DataFrame,
    ticker: str,
    asset_class: str,
    run_id: str,
    currency: str = "USD",
    exchange: str = "",
    source: str = "yfinance",
) -> pd.DataFrame:
    """
    Map a raw fetch result to the canonical OHLCV schema.

    Canonical columns:
      trade_date, symbol, asset_class,
      open, high, low, close, adj_close, volume,
      currency, exchange, source, ingested_at_utc, run_id
    """
    now = datetime.now(timezone.utc).isoformat()
    df = raw.copy()
    df["trade_date"]      = pd.to_datetime(df["_date"]).dt.date.astype(str)
    df["symbol"]          = ticker.upper()
    df["asset_class"]     = asset_class
    df["currency"]        = currency
    df["exchange"]        = exchange
    df["source"]          = source
    df["ingested_at_utc"] = now
    df["run_id"]          = run_id

    cols = [
        "trade_date", "symbol", "asset_class",
        "open", "high", "low", "close", "adj_close", "volume",
        "currency", "exchange", "source", "ingested_at_utc", "run_id",
    ]
    return df[cols].sort_values("trade_date").reset_index(drop=True)


# ── Quality check ─────────────────────────────────────────────────────

def quality_check(df: pd.DataFrame) -> dict:
    """
    Run basic quality checks on a canonical OHLCV DataFrame.

    Returns a dict with:
      rows, missing_pct, duplicate_rows,
      first_date, last_date, passed
    """
    if df.empty:
        return {
            "rows": 0, "missing_pct": 1.0,
            "duplicate_rows": 0, "first_date": "", "last_date": "",
            "passed": False,
        }

    value_cols = ["open", "high", "low", "close", "adj_close"]
    total_cells = len(df) * len(value_cols)
    missing = int(df[value_cols].isna().sum().sum())
    missing_pct = missing / total_cells if total_cells > 0 else 1.0
    duplicates = int(df.duplicated(subset=["trade_date"]).sum())

    passed = (
        len(df) > 0
        and missing_pct < 0.05    # < 5% missing values
        and duplicates == 0
    )

    return {
        "rows":           len(df),
        "missing_pct":    round(missing_pct, 4),
        "duplicate_rows": duplicates,
        "first_date":     str(df["trade_date"].min()),
        "last_date":      str(df["trade_date"].max()),
        "passed":         passed,
    }
