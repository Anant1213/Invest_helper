"""
backend.stock_research
──────────────────────
Stock Research feature backend.

  analytics_store  — Data access layer: routes to S3+DuckDB or PostgreSQL.
                     Exposes get_snapshot(), get_ohlcv(), get_analytics_history(),
                     get_tickers() for the 150-equity US universe.

  analytics/       — ETL pipeline that computes and writes analytics parquets:
                     returns, risk, momentum, zscore, technical indicators.
"""
