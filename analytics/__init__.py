"""
AssetEra Analytics Layer
────────────────────────
Converts raw OHLCV price data into derived analytical metrics.

Tables created in the `analytics` schema:
  analytics.returns    — daily + rolling returns
  analytics.risk       — volatility, Sharpe, Sortino, Beta, VaR, drawdown
  analytics.momentum   — price momentum, ROC, 52w range, relative strength
  analytics.zscore     — time-series and cross-sectional Z-scores
  analytics.technical  — RSI, Bollinger Bands, MACD, moving averages

Quick start:
    from analytics.pipeline import run
    run()                          # all markets, all modules
    run(markets=["NSE"])           # NSE only
    run(modules=["risk", "zscore"]) # specific modules, both markets

Individual modules:
    from analytics import returns, risk, momentum, zscore, technical
    rows = returns.run("US")
"""

from analytics import returns, risk, momentum, zscore, technical
from analytics.pipeline import run

__all__ = ["returns", "risk", "momentum", "zscore", "technical", "run"]
