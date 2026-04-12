"""
analytics.pipeline
──────────────────
Orchestrates the full analytics computation across all modules and markets.

Run order per market:
  1. returns   — must run first; other modules may depend on return series
  2. risk       — rolling vol/Sharpe/beta
  3. momentum   — price momentum + RS
  4. zscore     — statistical Z-scores
  5. technical  — RSI, BB, MACD, MAs

Each module is independent — a failure in one does not stop the others.
"""

from __future__ import annotations

import logging
import time
from typing import Sequence

from analytics import returns, risk, momentum, zscore, technical

logger = logging.getLogger(__name__)

MODULES = {
    "returns":   returns,
    "risk":      risk,
    "momentum":  momentum,
    "zscore":    zscore,
    "technical": technical,
}

MARKETS = ["US", "NSE"]


def run(
    markets:       Sequence[str] = MARKETS,
    modules:       Sequence[str] | None = None,
    lookback_days: int | None = None,
) -> dict:
    """
    Run analytics pipeline.

    All metrics are derived from daily OHLCV data — there are no separate
    monthly or weekly price tables.  Rolling windows (21d, 63d, 252d …) are
    computed with pandas pct_change(periods=N) on the daily series.

    Args:
        markets:       ['US', 'NSE'] or a subset.
        modules:       module names to run. None = all.
        lookback_days: calendar days of price history to load.
                       None (default) = full history (~10 years of daily data).
                       Use a number (e.g. 400) only for quick incremental runs
                       after the initial full-history load is done.

    Returns:
        Summary dict: {market: {module: row_count}}
    """
    mods_to_run = {k: v for k, v in MODULES.items() if modules is None or k in modules}
    summary: dict = {}
    t0 = time.time()

    for market in markets:
        summary[market] = {}
        logger.info("=== Pipeline starting: market=%s ===", market)

        for mod_name, mod in mods_to_run.items():
            t1 = time.time()
            try:
                rows = mod.run(market=market, lookback_days=lookback_days)
                elapsed = time.time() - t1
                logger.info("  [%s/%s] %d rows in %.1fs", market, mod_name, rows, elapsed)
                summary[market][mod_name] = rows
            except Exception as e:
                logger.error("  [%s/%s] FAILED: %s", market, mod_name, e)
                summary[market][mod_name] = -1

    total = time.time() - t0
    logger.info("=== Pipeline complete in %.1fs ===", total)
    return summary
