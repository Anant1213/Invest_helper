"""
analytics.zscore
────────────────
Statistical Z-scores for price, volume and returns.
Useful for mean-reversion signals and cross-sectional screening.

Output table: analytics.zscore
Columns:
  ticker, market, date           — composite PK

  Time-series Z-scores (how far is today's value from its own recent history):
  z_price_21d                    — (price - 21d mean) / 21d std
  z_price_63d                    — (price - 63d mean) / 63d std
  z_volume_21d                   — (volume - 21d mean) / 21d std
  z_ret_21d                      — (daily_ret - 21d mean ret) / 21d std ret

  Cross-sectional Z-scores (how does this ticker compare to its universe TODAY):
  z_cs_ret_1d                    — cross-sectional Z of today's return
  z_cs_price_vs_ma20             — cross-sectional Z of (price / 20d MA - 1)
  z_cs_volume                    — cross-sectional Z of volume vs 21d avg volume

  Percentile rank within the market universe (0–100):
  pct_ret_1d                     — percentile rank of today's 1d return
  pct_mom_3m                     — percentile rank of 3m momentum
"""

from __future__ import annotations

import logging
import numpy as np
import pandas as pd

from analytics._db import ensure_table, upsert_df, load_us_prices, load_nse_prices

logger = logging.getLogger(__name__)

TABLE = "analytics.zscore"

_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    ticker             TEXT              NOT NULL,
    market             TEXT              NOT NULL,
    date               TIMESTAMP         NOT NULL,
    z_price_21d        DOUBLE PRECISION,
    z_price_63d        DOUBLE PRECISION,
    z_volume_21d       DOUBLE PRECISION,
    z_ret_21d          DOUBLE PRECISION,
    z_cs_ret_1d        DOUBLE PRECISION,
    z_cs_price_vs_ma20 DOUBLE PRECISION,
    z_cs_volume        DOUBLE PRECISION,
    pct_ret_1d         DOUBLE PRECISION,
    pct_mom_3m         DOUBLE PRECISION,
    PRIMARY KEY (ticker, market, date)
);
"""

PK_COLS    = ["ticker", "market", "date"]
VALUE_COLS = [
    "z_price_21d", "z_price_63d",
    "z_volume_21d", "z_ret_21d",
    "z_cs_ret_1d", "z_cs_price_vs_ma20", "z_cs_volume",
    "pct_ret_1d", "pct_mom_3m",
]


def _rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    """(value - rolling_mean) / rolling_std over `window` periods."""
    mu  = series.rolling(window).mean()
    std = series.rolling(window).std()
    return (series - mu) / std.replace(0, np.nan)


def _cross_section_zscore(wide: pd.DataFrame) -> pd.DataFrame:
    """
    For a wide DataFrame (rows = dates, cols = tickers),
    return a same-shape DataFrame of cross-sectional Z-scores
    (standardise across tickers for each date).
    """
    mu  = wide.mean(axis=1)
    std = wide.std(axis=1)
    return wide.sub(mu, axis=0).div(std, axis=0)


def _cross_section_pct(wide: pd.DataFrame) -> pd.DataFrame:
    """Percentile rank (0–100) within the universe for each date."""
    return wide.rank(axis=1, pct=True) * 100


def compute(prices: pd.DataFrame, market: str) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame()

    # ── Time-series Z-scores (per ticker) ─────────────────────────────
    ts_rows: list[pd.DataFrame] = []

    for ticker, grp in prices.groupby("Ticker"):
        g = grp.set_index("Date").sort_index()

        close  = g["Close"]
        volume = g["Volume"] if "Volume" in g.columns else pd.Series(np.nan, index=g.index)
        ret_1d = close.pct_change(fill_method=None)

        z_price_21 = _rolling_zscore(close,  21)
        z_price_63 = _rolling_zscore(close,  63)
        z_vol_21   = _rolling_zscore(volume, 21)
        z_ret_21   = _rolling_zscore(ret_1d, 21)

        df_t = pd.DataFrame({
            "ticker":      ticker,
            "market":      market,
            "date":        close.index,
            "z_price_21d": z_price_21.values,
            "z_price_63d": z_price_63.values,
            "z_volume_21d": z_vol_21.values,
            "z_ret_21d":   z_ret_21.values,
        })
        ts_rows.append(df_t)

    if not ts_rows:
        return pd.DataFrame()

    result = pd.concat(ts_rows, ignore_index=True)

    # ── Cross-sectional signals (across tickers per date) ──────────────
    pivot_close  = prices.pivot_table(index="Date", columns="Ticker", values="Close")
    pivot_volume = prices.pivot_table(index="Date", columns="Ticker", values="Volume")

    ret_1d_wide    = pivot_close.pct_change(fill_method=None)
    ma20_wide      = pivot_close.rolling(20).mean()
    price_vs_ma20  = (pivot_close / ma20_wide.replace(0, np.nan) - 1.0)
    vol_ratio_wide = pivot_close.rolling(21).apply(
        lambda x: x[-1], raw=True
    )  # use raw close as proxy; actual vol ratio computed below
    vol_ratio_wide = pivot_volume.div(pivot_volume.rolling(21).mean().replace(0, np.nan))

    cs_ret    = _cross_section_zscore(ret_1d_wide)
    cs_ma20   = _cross_section_zscore(price_vs_ma20)
    cs_vol    = _cross_section_zscore(vol_ratio_wide)

    pct_ret   = _cross_section_pct(ret_1d_wide)
    pct_mom3m = _cross_section_pct(pivot_close.pct_change(periods=63, fill_method=None))

    # Melt cross-sectional wide → long and merge back
    def _melt(wide_df: pd.DataFrame, col_name: str) -> pd.DataFrame:
        return (
            wide_df.reset_index()
            .melt(id_vars="Date", var_name="ticker", value_name=col_name)
            .rename(columns={"Date": "date"})
        )

    cs_long = _melt(cs_ret,    "z_cs_ret_1d")
    cs_long = cs_long.merge(_melt(cs_ma20,  "z_cs_price_vs_ma20"), on=["date", "ticker"], how="left")
    cs_long = cs_long.merge(_melt(cs_vol,   "z_cs_volume"),         on=["date", "ticker"], how="left")
    cs_long = cs_long.merge(_melt(pct_ret,  "pct_ret_1d"),          on=["date", "ticker"], how="left")
    cs_long = cs_long.merge(_melt(pct_mom3m,"pct_mom_3m"),          on=["date", "ticker"], how="left")

    result = result.merge(cs_long, on=["date", "ticker"], how="left")

    return result


def save(df: pd.DataFrame) -> int:
    ensure_table(_TABLE_SQL, TABLE)
    return upsert_df(df, TABLE, PK_COLS, VALUE_COLS)


def run(market: str, lookback_days: int | None = None) -> int:
    label = f"{lookback_days}d" if lookback_days else "full history"
    logger.info("[zscore] loading %s prices (%s)", market, label)
    prices = load_us_prices(lookback_days) if market == "US" else load_nse_prices(lookback_days)

    if prices.empty:
        logger.warning("[zscore] no prices found for market=%s", market)
        return 0

    logger.info("[zscore] computing for %d tickers", prices["Ticker"].nunique())
    df = compute(prices, market)

    if df.empty:
        logger.warning("[zscore] computation produced no rows")
        return 0

    written = save(df)
    logger.info("[zscore] saved %d rows for market=%s", written, market)
    return written
