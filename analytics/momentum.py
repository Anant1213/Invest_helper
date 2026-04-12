"""
analytics.momentum
──────────────────
Price momentum and relative-strength signals per ticker.

Output table: analytics.momentum
Columns:
  ticker, market, date     — composite PK
  mom_1m                   — 21-day price return (momentum proxy)
  mom_3m                   — 63-day price return
  mom_6m                   — 126-day price return
  mom_12m                  — 252-day price return (skip last month: days 21-252)
  mom_12m_skip1m           — 12-month momentum skipping most recent month
                             (standard academic factor: Jegadeesh & Titman)
  roc_20d                  — rate of change over 20 days (%)
  roc_60d                  — rate of change over 60 days (%)
  hi_52w_pct               — % distance below 52-week high (negative = below high)
  lo_52w_pct               — % distance above 52-week low  (positive = above low)
  rs_vs_bench              — relative strength: ticker cumret / benchmark cumret
                             over trailing 252d (> 1 = outperforming)
  rank_universe            — rank within market (1 = highest 12m momentum)
"""

from __future__ import annotations

import logging
import numpy as np
import pandas as pd

from analytics._db import ensure_table, upsert_df, load_us_prices, load_nse_prices

logger = logging.getLogger(__name__)

TABLE = "analytics.momentum"

_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    ticker          TEXT              NOT NULL,
    market          TEXT              NOT NULL,
    date            TIMESTAMP         NOT NULL,
    mom_1m          DOUBLE PRECISION,
    mom_3m          DOUBLE PRECISION,
    mom_6m          DOUBLE PRECISION,
    mom_12m         DOUBLE PRECISION,
    mom_12m_skip1m  DOUBLE PRECISION,
    roc_20d         DOUBLE PRECISION,
    roc_60d         DOUBLE PRECISION,
    hi_52w_pct      DOUBLE PRECISION,
    lo_52w_pct      DOUBLE PRECISION,
    rs_vs_bench     DOUBLE PRECISION,
    rank_universe   INTEGER,
    PRIMARY KEY (ticker, market, date)
);
"""

PK_COLS    = ["ticker", "market", "date"]
VALUE_COLS = [
    "mom_1m", "mom_3m", "mom_6m", "mom_12m", "mom_12m_skip1m",
    "roc_20d", "roc_60d",
    "hi_52w_pct", "lo_52w_pct",
    "rs_vs_bench", "rank_universe",
]


def _bench_cumret(prices: pd.DataFrame, market: str, dates: pd.DatetimeIndex, window: int) -> pd.Series:
    """
    Compute trailing `window`-day cumulative return for the benchmark
    aligned to `dates`.
    US  → SPY
    NSE → equal-weight large-cap average
    """
    if market == "US":
        bdf = prices[prices["Ticker"] == "SPY"].set_index("Date")["Close"].sort_index()
    else:
        cap_col = "CapCategory" if "CapCategory" in prices.columns else None
        if cap_col:
            subset = prices[prices[cap_col] == "LARGE"]
        else:
            subset = prices
        pivot = subset.pivot_table(index="Date", columns="Ticker", values="Close")
        bdf   = pivot.mean(axis=1).sort_index()

    bdf = bdf.reindex(dates).ffill()
    return bdf.pct_change(periods=window, fill_method=None)


def compute(prices: pd.DataFrame, market: str) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame()

    # Build a shared date index from the prices for universe ranking
    all_dates = prices["Date"].drop_duplicates().sort_values().reset_index(drop=True)

    # Benchmark relative strength
    bench_252 = _bench_cumret(prices, market, all_dates, window=252)
    bench_sr  = bench_252.rename("bench")

    rows = []

    for ticker, grp in prices.groupby("Ticker"):
        g = grp.set_index("Date").sort_index()["Close"]
        if len(g) < 22:
            continue

        # Momentum at standard horizons
        mom_1m  = g.pct_change(periods=21,  fill_method=None)
        mom_3m  = g.pct_change(periods=63,  fill_method=None)
        mom_6m  = g.pct_change(periods=126, fill_method=None)
        mom_12m = g.pct_change(periods=252, fill_method=None)

        # Skip-1m momentum: return from t-252 to t-21 (Jegadeesh–Titman)
        mom_12m_skip1m = g.shift(21).pct_change(periods=231, fill_method=None).reindex(g.index)

        # Rate of change
        roc_20d = g.pct_change(periods=20, fill_method=None) * 100
        roc_60d = g.pct_change(periods=60, fill_method=None) * 100

        # 52-week high / low distance
        hi_52w = g.rolling(252).max()
        lo_52w = g.rolling(252).min()
        hi_52w_pct = (g / hi_52w - 1.0) * 100   # 0 = at 52w high, negative = below
        lo_52w_pct = (g / lo_52w - 1.0) * 100   # 0 = at 52w low, positive = above

        # Relative strength vs benchmark
        ticker_252 = g.pct_change(periods=252, fill_method=None)
        bench_aligned = bench_sr.reindex(g.index)
        rs = (1 + ticker_252) / (1 + bench_aligned.replace(0, np.nan))

        df_t = pd.DataFrame({
            "ticker":         ticker,
            "market":         market,
            "date":           g.index,
            "mom_1m":         mom_1m.values,
            "mom_3m":         mom_3m.values,
            "mom_6m":         mom_6m.values,
            "mom_12m":        mom_12m.values,
            "mom_12m_skip1m": mom_12m_skip1m.values,
            "roc_20d":        roc_20d.values,
            "roc_60d":        roc_60d.values,
            "hi_52w_pct":     hi_52w_pct.values,
            "lo_52w_pct":     lo_52w_pct.values,
            "rs_vs_bench":    rs.values,
            "rank_universe":  np.nan,   # filled below after all tickers
        })
        rows.append(df_t)

    if not rows:
        return pd.DataFrame()

    result = pd.concat(rows, ignore_index=True)

    # Cross-sectional rank by 12m momentum (higher = stronger momentum, rank 1)
    result["rank_universe"] = (
        result.groupby("date")["mom_12m"]
        .rank(ascending=False, method="min", na_option="bottom")
        .astype("Int64")
    )

    return result


def save(df: pd.DataFrame) -> int:
    ensure_table(_TABLE_SQL, TABLE)
    return upsert_df(df, TABLE, PK_COLS, VALUE_COLS)


def run(market: str, lookback_days: int | None = None) -> int:
    label = f"{lookback_days}d" if lookback_days else "full history"
    logger.info("[momentum] loading %s prices (%s)", market, label)
    prices = load_us_prices(lookback_days) if market == "US" else load_nse_prices(lookback_days)

    if prices.empty:
        logger.warning("[momentum] no prices found for market=%s", market)
        return 0

    logger.info("[momentum] computing for %d tickers", prices["Ticker"].nunique())
    df = compute(prices, market)

    if df.empty:
        logger.warning("[momentum] computation produced no rows")
        return 0

    written = save(df)
    logger.info("[momentum] saved %d rows for market=%s", written, market)
    return written
