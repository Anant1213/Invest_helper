"""
analytics.returns
─────────────────
Computes daily and rolling returns for every ticker in a price DataFrame.

Output table: analytics.returns
Columns:
  ticker, market, date            — composite PK
  ret_1d                          — simple daily return
  log_ret_1d                      — log return (ln(P_t / P_t-1))
  ret_5d                          — 5-trading-day (1 week) return
  ret_21d                         — ~1-month return
  ret_63d                         — ~3-month return
  ret_126d                        — ~6-month return
  ret_252d                        — ~1-year return
"""

from __future__ import annotations

import logging
import numpy as np
import pandas as pd

from analytics._db import ensure_table, upsert_df, load_us_prices, load_nse_prices

logger = logging.getLogger(__name__)

TABLE = "analytics.returns"

_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    ticker      TEXT              NOT NULL,
    market      TEXT              NOT NULL,
    date        TIMESTAMP         NOT NULL,
    ret_1d      DOUBLE PRECISION,
    log_ret_1d  DOUBLE PRECISION,
    ret_5d      DOUBLE PRECISION,
    ret_21d     DOUBLE PRECISION,
    ret_63d     DOUBLE PRECISION,
    ret_126d    DOUBLE PRECISION,
    ret_252d    DOUBLE PRECISION,
    PRIMARY KEY (ticker, market, date)
);
"""

PK_COLS     = ["ticker", "market", "date"]
VALUE_COLS  = ["ret_1d", "log_ret_1d", "ret_5d", "ret_21d", "ret_63d", "ret_126d", "ret_252d"]


def compute(prices: pd.DataFrame, market: str) -> pd.DataFrame:
    """
    Given a long-form OHLCV DataFrame with [Date, Ticker, Close],
    compute return metrics per ticker per date.

    Returns a flat DataFrame ready to upsert.
    """
    if prices.empty:
        return pd.DataFrame()

    rows = []
    for ticker, grp in prices.groupby("Ticker"):
        g = grp.set_index("Date").sort_index()["Close"]
        if len(g) < 2:
            continue

        ret_1d    = g.pct_change(fill_method=None)
        log_ret   = np.log(g / g.shift(1))
        ret_5d    = g.pct_change(periods=5,   fill_method=None)
        ret_21d   = g.pct_change(periods=21,  fill_method=None)
        ret_63d   = g.pct_change(periods=63,  fill_method=None)
        ret_126d  = g.pct_change(periods=126, fill_method=None)
        ret_252d  = g.pct_change(periods=252, fill_method=None)

        df_t = pd.DataFrame({
            "ticker":     ticker,
            "market":     market,
            "date":       g.index,
            "ret_1d":     ret_1d.values,
            "log_ret_1d": log_ret.values,
            "ret_5d":     ret_5d.values,
            "ret_21d":    ret_21d.values,
            "ret_63d":    ret_63d.values,
            "ret_126d":   ret_126d.values,
            "ret_252d":   ret_252d.values,
        })
        rows.append(df_t)

    if not rows:
        return pd.DataFrame()

    return pd.concat(rows, ignore_index=True)


def save(df: pd.DataFrame) -> int:
    ensure_table(_TABLE_SQL, TABLE)
    return upsert_df(df, TABLE, PK_COLS, VALUE_COLS)


def run(market: str, lookback_days: int | None = None) -> int:
    """
    Load prices → compute returns → save to analytics.returns.
    All rolling returns (5d, 21d, 63d, 126d, 252d) are derived from daily
    OHLCV data — no separate monthly/weekly tables are used.
    Returns row count written.
    """
    label = f"{lookback_days}d" if lookback_days else "full history"
    logger.info("[returns] loading %s prices (%s)", market, label)
    prices = load_us_prices(lookback_days) if market == "US" else load_nse_prices(lookback_days)

    if prices.empty:
        logger.warning("[returns] no prices found for market=%s", market)
        return 0

    logger.info("[returns] computing for %d tickers", prices["Ticker"].nunique())
    df = compute(prices, market)

    if df.empty:
        logger.warning("[returns] computation produced no rows")
        return 0

    written = save(df)
    logger.info("[returns] saved %d rows for market=%s", written, market)
    return written
