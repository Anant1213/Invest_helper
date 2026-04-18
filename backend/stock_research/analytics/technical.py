"""
analytics.technical
───────────────────
Technical indicator values per ticker per date.
These are indicator readings (not buy/sell signals) — stored as-is
so any downstream model or UI can apply its own thresholds.

Output table: analytics.technical
Columns:
  ticker, market, date     — composite PK
  rsi_14                   — RSI-14 (Wilder's smoothing)
  bb_upper                 — Bollinger upper band (20d, 2σ)
  bb_mid                   — Bollinger middle band (20d SMA)
  bb_lower                 — Bollinger lower band (20d, 2σ)
  bb_pct_b                 — %B: (price - lower) / (upper - lower), 0–1 inside bands
  bb_width                 — bandwidth: (upper - lower) / mid  (volatility proxy)
  macd_line                — MACD line (EMA12 - EMA26)
  macd_signal              — signal line (EMA9 of MACD)
  macd_hist                — histogram (MACD - signal)
  ma_20                    — 20-day simple moving average
  ma_50                    — 50-day simple moving average
  ma_200                   — 200-day simple moving average
  pct_vs_ma50              — % above/below MA-50
  pct_vs_ma200             — % above/below MA-200
  golden_cross             — True when MA-50 > MA-200
  atr_14                   — Average True Range (14d), normalised by price
  obv                      — On-Balance Volume (cumulative)
"""

from __future__ import annotations

import logging
import numpy as np
import pandas as pd

from backend.stock_research.analytics._db import ensure_table, upsert_df, load_us_prices, load_equity_prices

logger = logging.getLogger(__name__)

TABLE = "analytics.technical"

_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    ticker       TEXT              NOT NULL,
    market       TEXT              NOT NULL,
    date         TIMESTAMP         NOT NULL,
    rsi_14       DOUBLE PRECISION,
    bb_upper     DOUBLE PRECISION,
    bb_mid       DOUBLE PRECISION,
    bb_lower     DOUBLE PRECISION,
    bb_pct_b     DOUBLE PRECISION,
    bb_width     DOUBLE PRECISION,
    macd_line    DOUBLE PRECISION,
    macd_signal  DOUBLE PRECISION,
    macd_hist    DOUBLE PRECISION,
    ma_20        DOUBLE PRECISION,
    ma_50        DOUBLE PRECISION,
    ma_200       DOUBLE PRECISION,
    pct_vs_ma50  DOUBLE PRECISION,
    pct_vs_ma200 DOUBLE PRECISION,
    golden_cross BOOLEAN,
    atr_14       DOUBLE PRECISION,
    obv          DOUBLE PRECISION,
    PRIMARY KEY (ticker, market, date)
);
"""

PK_COLS    = ["ticker", "market", "date"]
VALUE_COLS = [
    "rsi_14",
    "bb_upper", "bb_mid", "bb_lower", "bb_pct_b", "bb_width",
    "macd_line", "macd_signal", "macd_hist",
    "ma_20", "ma_50", "ma_200",
    "pct_vs_ma50", "pct_vs_ma200", "golden_cross",
    "atr_14", "obv",
]


# ── Indicator implementations ─────────────────────────────────────────

def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Wilder's RSI."""
    delta = close.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    # Wilder's EWM: alpha = 1/period
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _bollinger(close: pd.Series, window: int = 20, n_std: float = 2.0):
    """Returns (upper, mid, lower, pct_b, bandwidth)."""
    mid   = close.rolling(window).mean()
    std   = close.rolling(window).std()
    upper = mid + n_std * std
    lower = mid - n_std * std
    band_range = (upper - lower).replace(0, np.nan)
    pct_b = (close - lower) / band_range
    width = band_range / mid.replace(0, np.nan)
    return upper, mid, lower, pct_b, width


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """Returns (macd_line, signal_line, histogram)."""
    line   = _ema(close, fast) - _ema(close, slow)
    sig    = _ema(line, signal)
    hist   = line - sig
    return line, sig, hist


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Normalised ATR: ATR / close price."""
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    return atr / close.replace(0, np.nan)   # normalised


def _obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    """On-Balance Volume."""
    direction = np.sign(close.diff().fillna(0))
    return (direction * volume.fillna(0)).cumsum()


# ── Main compute ──────────────────────────────────────────────────────

def compute(prices: pd.DataFrame, market: str) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame()

    rows = []

    for ticker, grp in prices.groupby("Ticker"):
        g = grp.set_index("Date").sort_index()
        close  = g["Close"]
        high   = g.get("High",   close)
        low    = g.get("Low",    close)
        volume = g.get("Volume", pd.Series(0, index=g.index))

        if len(close.dropna()) < 21:
            continue

        # ── Indicators ──────────────────────────────────────────────
        rsi14 = _rsi(close)

        bb_upper, bb_mid, bb_lower, bb_pct_b, bb_width = _bollinger(close)

        macd_line, macd_sig, macd_hist = _macd(close)

        ma20  = close.rolling(20).mean()
        ma50  = close.rolling(50).mean()
        ma200 = close.rolling(200).mean()

        pct_vs_ma50  = (close / ma50.replace(0, np.nan)  - 1.0) * 100
        pct_vs_ma200 = (close / ma200.replace(0, np.nan) - 1.0) * 100
        golden       = (ma50 > ma200)

        atr14 = _atr(high, low, close)
        obv   = _obv(close, volume)

        df_t = pd.DataFrame({
            "ticker":       ticker,
            "market":       market,
            "date":         close.index,
            "rsi_14":       rsi14.values,
            "bb_upper":     bb_upper.values,
            "bb_mid":       bb_mid.values,
            "bb_lower":     bb_lower.values,
            "bb_pct_b":     bb_pct_b.values,
            "bb_width":     bb_width.values,
            "macd_line":    macd_line.values,
            "macd_signal":  macd_sig.values,
            "macd_hist":    macd_hist.values,
            "ma_20":        ma20.values,
            "ma_50":        ma50.values,
            "ma_200":       ma200.values,
            "pct_vs_ma50":  pct_vs_ma50.values,
            "pct_vs_ma200": pct_vs_ma200.values,
            "golden_cross": golden.values,
            "atr_14":       atr14.values,
            "obv":          obv.values,
        })
        rows.append(df_t)

    if not rows:
        return pd.DataFrame()

    return pd.concat(rows, ignore_index=True)


def save(df: pd.DataFrame) -> int:
    from backend.stock_research.analytics._db import _s3_enabled, write_analytics_to_s3
    if _s3_enabled():
        module = TABLE.split(".")[-1]
        total = 0
        for mkt in df["market"].dropna().unique():
            total += write_analytics_to_s3(df[df["market"] == mkt], module, mkt)
        return total
    ensure_table(_TABLE_SQL, TABLE)
    return upsert_df(df, TABLE, PK_COLS, VALUE_COLS)


def run(market: str, lookback_days: int | None = None) -> int:
    label = f"{lookback_days}d" if lookback_days else "full history"
    logger.info("[technical] loading %s prices (%s)", market, label)
    prices = load_us_prices(lookback_days) if market == "US" else load_equity_prices(lookback_days)

    if prices.empty:
        logger.warning("[technical] no prices found for market=%s", market)
        return 0

    logger.info("[technical] computing for %d tickers", prices["Ticker"].nunique())
    df = compute(prices, market)

    if df.empty:
        logger.warning("[technical] computation produced no rows")
        return 0

    written = save(df)
    logger.info("[technical] saved %d rows for market=%s", written, market)
    return written
