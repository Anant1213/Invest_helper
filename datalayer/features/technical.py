"""
datalayer.features.technical
─────────────────────────────
Compute technical indicators from curated OHLCV data and write
to the features zone in S3.

Feature set name: technical_v1

Indicators computed:
  returns_1d, returns_5d, returns_21d, returns_63d, returns_252d
  vol_21d, vol_63d
  rsi_14
  macd_line, macd_signal, macd_hist
  bb_upper, bb_mid, bb_lower, bb_pct_b, bb_width
  ma_20, ma_50, ma_200
  pct_vs_ma50, pct_vs_ma200
  golden_cross
  atr_14

Output schema (FEATURES_COLUMNS long format):
  date, asset_class, symbol_or_series,
  feature_set, feature_name, feature_value,
  window, source_ref, generated_at_utc, run_id
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd

import datalayer.s3 as s3
from datalayer.schemas import FEATURES_COLUMNS

logger = logging.getLogger(__name__)

FEATURE_SET = "technical_v1"


# ── Indicator computation ─────────────────────────────────────────────

def _compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain  = delta.clip(lower=0).ewm(alpha=1 / period, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(alpha=1 / period, adjust=False).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _compute_macd(close: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    ema12   = close.ewm(span=12, adjust=False).mean()
    ema26   = close.ewm(span=26, adjust=False).mean()
    line    = ema12 - ema26
    signal  = line.ewm(span=9, adjust=False).mean()
    hist    = line - signal
    return line, signal, hist


def _compute_bb(close: pd.Series, period: int = 20, std: float = 2.0):
    mid    = close.rolling(period).mean()
    sigma  = close.rolling(period).std()
    upper  = mid + std * sigma
    lower  = mid - std * sigma
    width  = (upper - lower) / mid.replace(0, np.nan)
    pct_b  = (close - lower) / (upper - lower).replace(0, np.nan)
    return upper, mid, lower, pct_b, width


def _compute_atr(high: pd.Series, low: pd.Series, close: pd.Series,
                 period: int = 14) -> pd.Series:
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low  - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / period, adjust=False).mean()
    return atr / close.replace(0, np.nan)   # normalised by price


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a canonical OHLCV DataFrame (trade_date, close, high, low, adj_close),
    return a wide DataFrame with all indicator columns added.
    """
    df = df.copy().sort_values("trade_date").reset_index(drop=True)
    c = df["adj_close"].astype(float)
    h = df["high"].astype(float)
    lo = df["low"].astype(float)

    # Returns
    df["returns_1d"]   = c.pct_change(1)
    df["returns_5d"]   = c.pct_change(5)
    df["returns_21d"]  = c.pct_change(21)
    df["returns_63d"]  = c.pct_change(63)
    df["returns_252d"] = c.pct_change(252)

    # Volatility (annualised)
    df["vol_21d"]  = c.pct_change().rolling(21).std()  * np.sqrt(252)
    df["vol_63d"]  = c.pct_change().rolling(63).std()  * np.sqrt(252)

    # RSI
    df["rsi_14"] = _compute_rsi(c, 14)

    # MACD
    df["macd_line"], df["macd_signal"], df["macd_hist"] = _compute_macd(c)

    # Bollinger Bands
    (df["bb_upper"], df["bb_mid"], df["bb_lower"],
     df["bb_pct_b"], df["bb_width"]) = _compute_bb(c)

    # Moving averages
    df["ma_20"]  = c.rolling(20).mean()
    df["ma_50"]  = c.rolling(50).mean()
    df["ma_200"] = c.rolling(200).mean()
    df["pct_vs_ma50"]  = (c - df["ma_50"])  / df["ma_50"].replace(0, np.nan)
    df["pct_vs_ma200"] = (c - df["ma_200"]) / df["ma_200"].replace(0, np.nan)
    df["golden_cross"] = (df["ma_50"] > df["ma_200"]).astype(float)

    # ATR (normalised)
    df["atr_14"] = _compute_atr(h, lo, c, 14)

    return df


# ── Wide → Long format ────────────────────────────────────────────────

_INDICATOR_WINDOW = {
    "returns_1d":   "1d",
    "returns_5d":   "5d",
    "returns_21d":  "21d",
    "returns_63d":  "63d",
    "returns_252d": "252d",
    "vol_21d":      "21d",
    "vol_63d":      "63d",
    "rsi_14":       "14d",
    "macd_line":    "12d/26d",
    "macd_signal":  "9d",
    "macd_hist":    "12d/26d/9d",
    "bb_upper":     "20d",
    "bb_mid":       "20d",
    "bb_lower":     "20d",
    "bb_pct_b":     "20d",
    "bb_width":     "20d",
    "ma_20":        "20d",
    "ma_50":        "50d",
    "ma_200":       "200d",
    "pct_vs_ma50":  "50d",
    "pct_vs_ma200": "200d",
    "golden_cross": "50d/200d",
    "atr_14":       "14d",
}


def wide_to_long(
    wide: pd.DataFrame,
    symbol: str,
    asset_class: str,
    run_id: str,
    source_ref: str = "",
) -> pd.DataFrame:
    """Pivot wide indicator DataFrame to canonical long features format."""
    now    = datetime.now(timezone.utc).isoformat()
    cols   = list(_INDICATOR_WINDOW.keys())
    rows   = []
    for _, row in wide.iterrows():
        date_str = str(row["trade_date"])
        for feat in cols:
            val = row.get(feat, np.nan)
            if pd.isna(val):
                continue
            rows.append({
                "date":              date_str,
                "asset_class":       asset_class,
                "symbol_or_series":  symbol,
                "feature_set":       FEATURE_SET,
                "feature_name":      feat,
                "feature_value":     float(val),
                "window":            _INDICATOR_WINDOW[feat],
                "source_ref":        source_ref,
                "generated_at_utc":  now,
                "run_id":            run_id,
            })
    return pd.DataFrame(rows, columns=FEATURES_COLUMNS)


# ── Main entry point ──────────────────────────────────────────────────

def compute_and_write(
    symbol: str,
    asset_class: str,
    run_id: str,
    manifest=None,
) -> bool:
    """
    Read curated OHLCV for `symbol` from S3, compute indicators,
    write long-format features parquet to features zone.
    Returns True on success.
    """
    curated_key = s3.curated_key(asset_class, "yfinance", symbol)
    try:
        df = s3.read_parquet(curated_key)
    except Exception as e:
        logger.error("[features/tech] %s — could not read curated: %s", symbol, e)
        return False

    if df.empty:
        logger.warning("[features/tech] %s — curated data is empty", symbol)
        return False

    try:
        wide    = compute_indicators(df)
        long_df = wide_to_long(wide, symbol, asset_class, run_id,
                               source_ref=curated_key)
    except Exception as e:
        logger.error("[features/tech] %s — compute failed: %s", symbol, e)
        return False

    feat_key = s3.features_key(asset_class, FEATURE_SET, symbol)
    try:
        s3.put_parquet(feat_key, long_df)
        if manifest:
            manifest.record_write(feat_key)
        logger.info("[features/tech] %s — OK  rows=%d", symbol, len(long_df))
        return True
    except Exception as e:
        logger.error("[features/tech] %s — write failed: %s", symbol, e)
        return False
