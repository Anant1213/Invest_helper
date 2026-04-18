"""
analytics.risk
──────────────
Rolling risk metrics per ticker.

Output table: analytics.risk
Columns:
  ticker, market, date             — composite PK
  vol_21d                          — annualised volatility over 21-day window
  vol_63d                          — annualised volatility over 63-day window
  vol_252d                         — annualised volatility over 252-day window
  sharpe_63d                       — rolling Sharpe (63d window, rf=0)
  sharpe_252d                      — rolling Sharpe (252d window, rf=0)
  sortino_252d                     — rolling Sortino (252d window, rf=0)
  max_dd_252d                      — max drawdown over trailing 252 days
  beta_252d                        — market beta vs benchmark (SPY/NSE index)
  alpha_252d                       — annualised Jensen's alpha
  var_95_21d                       — 95% parametric VaR (21d window)
  cvar_95_21d                      — 95% CVaR (expected shortfall, 21d)
"""

from __future__ import annotations

import logging
import numpy as np
import pandas as pd

from backend.stock_research.analytics._db import ensure_table, upsert_df, load_us_prices, load_equity_prices

logger = logging.getLogger(__name__)

TABLE = "analytics.risk"

_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    ticker       TEXT              NOT NULL,
    market       TEXT              NOT NULL,
    date         TIMESTAMP         NOT NULL,
    vol_21d      DOUBLE PRECISION,
    vol_63d      DOUBLE PRECISION,
    vol_252d     DOUBLE PRECISION,
    sharpe_63d   DOUBLE PRECISION,
    sharpe_252d  DOUBLE PRECISION,
    sortino_252d DOUBLE PRECISION,
    max_dd_252d  DOUBLE PRECISION,
    beta_252d    DOUBLE PRECISION,
    alpha_252d   DOUBLE PRECISION,
    var_95_21d   DOUBLE PRECISION,
    cvar_95_21d  DOUBLE PRECISION,
    PRIMARY KEY (ticker, market, date)
);
"""

PK_COLS    = ["ticker", "market", "date"]
VALUE_COLS = [
    "vol_21d", "vol_63d", "vol_252d",
    "sharpe_63d", "sharpe_252d", "sortino_252d",
    "max_dd_252d", "beta_252d", "alpha_252d",
    "var_95_21d", "cvar_95_21d",
]

TRADING_DAYS = 252


def _rolling_max_drawdown(prices: pd.Series, window: int) -> pd.Series:
    """Max drawdown over a rolling window."""
    result = np.full(len(prices), np.nan)
    arr = prices.values
    for i in range(window, len(arr)):
        window_slice = arr[i - window : i + 1]
        peak = np.maximum.accumulate(window_slice)
        dd = (window_slice - peak) / peak
        result[i] = dd.min()
    return pd.Series(result, index=prices.index)


def _rolling_beta_alpha(
    ret: pd.Series,
    bench_ret: pd.Series,
    window: int,
    rf_daily: float = 0.0,
) -> tuple[pd.Series, pd.Series]:
    """
    Rolling OLS beta and annualised alpha vs a benchmark return series.
    Both series must share the same index.
    """
    aligned = pd.DataFrame({"r": ret, "b": bench_ret}).dropna()
    beta_vals  = np.full(len(ret), np.nan)
    alpha_vals = np.full(len(ret), np.nan)

    idx_map = {ts: i for i, ts in enumerate(ret.index)}

    for i in range(window, len(aligned)):
        sl = aligned.iloc[i - window : i]
        b  = sl["b"].values
        r  = sl["r"].values
        b_var = b.var(ddof=1)
        if b_var < 1e-12:
            continue
        cov  = np.cov(r, b, ddof=1)[0, 1]
        beta = cov / b_var
        alpha_daily = (r - rf_daily).mean() - beta * (b - rf_daily).mean()
        orig_idx    = aligned.index[i]
        pos         = idx_map.get(orig_idx)
        if pos is not None:
            beta_vals[pos]  = beta
            alpha_vals[pos] = alpha_daily * TRADING_DAYS  # annualise

    return (
        pd.Series(beta_vals,  index=ret.index),
        pd.Series(alpha_vals, index=ret.index),
    )


def _build_benchmark(prices: pd.DataFrame, market: str) -> pd.Series:
    """
    Build a benchmark return series.
    - market='US'    : SPY (present in us_prices)
    - market='US_EQ' : equal-weighted large-cap returns (S&P 500 proxy,
                       since SPY is not in us_equity_prices)
    """
    if market == "US":
        spy = prices[prices["Ticker"] == "SPY"].set_index("Date")["Close"].sort_index()
        return spy.pct_change(fill_method=None).rename("benchmark")

    # US_EQ — use equal-weight large-cap universe as benchmark
    large = prices[prices["CapCategory"] == "LARGE"] if "CapCategory" in prices.columns else prices
    if large.empty:
        large = prices

    pivot = large.pivot_table(index="Date", columns="Ticker", values="Close")
    rets  = pivot.pct_change(fill_method=None)
    return rets.mean(axis=1).rename("benchmark")


def compute(prices: pd.DataFrame, market: str) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame()

    bench_ret = _build_benchmark(prices, market)
    rows = []

    for ticker, grp in prices.groupby("Ticker"):
        g   = grp.set_index("Date").sort_index()["Close"]
        ret = g.pct_change(fill_method=None)

        if len(ret.dropna()) < 22:
            continue

        ann = np.sqrt(TRADING_DAYS)

        # Volatility
        vol_21  = ret.rolling(21).std()  * ann
        vol_63  = ret.rolling(63).std()  * ann
        vol_252 = ret.rolling(252).std() * ann

        # Sharpe (rf = 0 for simplicity; consumers can adjust)
        mean_21  = ret.rolling(21).mean()
        std_21   = ret.rolling(21).std()
        mean_63  = ret.rolling(63).mean()
        std_63   = ret.rolling(63).std()
        mean_252 = ret.rolling(252).mean()
        std_252  = ret.rolling(252).std()

        sharpe_63  = (mean_63  / std_63.replace(0, np.nan))  * ann
        sharpe_252 = (mean_252 / std_252.replace(0, np.nan)) * ann

        # Sortino (downside deviation)
        down_252 = ret.rolling(252).apply(
            lambda x: np.std(x[x < 0], ddof=1) if (x < 0).any() else np.nan,
            raw=True,
        )
        sortino_252 = (mean_252 / down_252.replace(0, np.nan)) * ann

        # Max drawdown (trailing 252d)
        max_dd = _rolling_max_drawdown(g, 252)

        # Beta / Alpha vs benchmark
        bench_aligned = bench_ret.reindex(ret.index)
        beta, alpha = _rolling_beta_alpha(ret, bench_aligned, window=252)

        # VaR & CVaR — parametric (21d)
        var_95  = mean_21 - 1.645 * std_21   # 95% VaR
        cvar_95 = mean_21 - 2.063 * std_21   # 95% CVaR approx (normal)

        df_t = pd.DataFrame({
            "ticker":       ticker,
            "market":       market,
            "date":         g.index,
            "vol_21d":      vol_21.values,
            "vol_63d":      vol_63.values,
            "vol_252d":     vol_252.values,
            "sharpe_63d":   sharpe_63.values,
            "sharpe_252d":  sharpe_252.values,
            "sortino_252d": sortino_252.values,
            "max_dd_252d":  max_dd.values,
            "beta_252d":    beta.values,
            "alpha_252d":   alpha.values,
            "var_95_21d":   var_95.values,
            "cvar_95_21d":  cvar_95.values,
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
    logger.info("[risk] loading %s prices (%s)", market, label)
    prices = load_us_prices(lookback_days) if market == "US" else load_equity_prices(lookback_days)

    if prices.empty:
        logger.warning("[risk] no prices found for market=%s", market)
        return 0

    logger.info("[risk] computing for %d tickers", prices["Ticker"].nunique())
    df = compute(prices, market)

    if df.empty:
        logger.warning("[risk] computation produced no rows")
        return 0

    written = save(df)
    logger.info("[risk] saved %d rows for market=%s", written, market)
    return written
