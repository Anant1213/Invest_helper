"""
backend.stock_research.analytics_store
────────────────────────
Data access layer for the analytics schema.
Automatically routes to the active storage backend:

  S3 + DuckDB  — when DATA_BUCKET + AWS credentials are set
  PostgreSQL   — when POSTGRES_URL / DATABASE_URL is set

All public functions return DataFrames.  Callers may cache them freely.

Markets:
  'US'    — us_prices (ETFs + fund constituent stocks)
  'US_EQ' — us_equity_prices (150 US equities, large/mid/small cap)
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ── Backend detection ─────────────────────────────────────────────────

def _s3_enabled() -> bool:
    try:
        from backend.db.s3_store import is_enabled
        return is_enabled()
    except Exception:
        return False


def is_enabled() -> bool:
    """True if any storage backend is configured."""
    if _s3_enabled():
        return True
    try:
        from backend.db.postgres_store import is_enabled as pg_ok
        return pg_ok()
    except Exception:
        return False


def _price_table(market: str) -> str:
    return "us_prices" if market == "US" else "us_equity_prices"


# ══════════════════════════════════════════════════════════════════════
# S3 / DuckDB implementations
# ══════════════════════════════════════════════════════════════════════

_ANALYTICS_MODULES = ["returns", "risk", "momentum", "zscore", "technical"]


def _get_snapshot_s3(market: str, cap_category: str | None = None) -> pd.DataFrame:
    """
    Build the wide snapshot DataFrame from S3 parquet files via DuckDB.
    One row per ticker: latest analytics row joined with latest price.
    """
    from backend.db.s3_store import analytics_key, market_key, key_exists
    from backend.db.duckdb_store import get_conn

    conn = get_conn()

    def _uri(key: str) -> str:
        from backend.db.s3_store import s3_uri
        return s3_uri(key)

    # Check all 5 analytics files exist for this market
    modules_present = [m for m in _ANALYTICS_MODULES if key_exists(analytics_key(m, market))]
    if not modules_present:
        logger.warning("[analytics_store] no S3 analytics files for market=%s", market)
        return pd.DataFrame()

    price_key = market_key(market)
    has_prices = key_exists(price_key)

    # Build CTE per module — latest date only
    ctes = []
    selects = []

    ret_cols = "ticker, ret_1d, ret_5d, ret_21d, ret_63d, ret_126d, ret_252d"
    rsk_cols = ("ticker, vol_21d, vol_63d, vol_252d, sharpe_63d, sharpe_252d, "
                "sortino_252d, max_dd_252d, beta_252d, alpha_252d, var_95_21d, cvar_95_21d")
    mom_cols = ("ticker, mom_1m, mom_3m, mom_6m, mom_12m, mom_12m_skip1m, "
                "hi_52w_pct, lo_52w_pct, rs_vs_bench, rank_universe")
    tec_cols = ("ticker, rsi_14, bb_pct_b, bb_width, macd_line, macd_signal, macd_hist, "
                "ma_20, ma_50, ma_200, pct_vs_ma50, pct_vs_ma200, golden_cross, atr_14")
    zsc_cols = ("ticker, z_price_21d, z_price_63d, z_volume_21d, z_ret_21d, "
                "z_cs_ret_1d, z_cs_price_vs_ma20, pct_ret_1d, pct_mom_3m")

    mod_cols = {
        "returns":   ret_cols,
        "risk":      rsk_cols,
        "momentum":  mom_cols,
        "technical": tec_cols,
        "zscore":    zsc_cols,
    }
    mod_alias = {
        "returns":   "ret",
        "risk":      "rsk",
        "momentum":  "mom",
        "technical": "tec",
        "zscore":    "zsc",
    }

    cte_parts = []
    for mod in _ANALYTICS_MODULES:
        if mod not in modules_present:
            continue
        alias = mod_alias[mod]
        uri = _uri(analytics_key(mod, market))
        cols = mod_cols[mod]
        cte_parts.append(f"""
        {alias}_raw AS (
            SELECT {cols}, date
            FROM read_parquet('{uri}')
            WHERE market = '{market}'
        ),
        {alias}_max AS (SELECT MAX(date) AS max_date FROM {alias}_raw),
        {alias} AS (
            SELECT {alias}_raw.*
            FROM {alias}_raw, {alias}_max
            WHERE {alias}_raw.date = {alias}_max.max_date
        )""")

    if has_prices:
        cte_parts.append(f"""
        px_raw AS (
            SELECT ticker, close AS last_price, date
            FROM read_parquet('{_uri(price_key)}')
        ),
        px AS (
            SELECT ticker, last_price
            FROM px_raw
            WHERE (ticker, date) IN (SELECT ticker, MAX(date) FROM px_raw GROUP BY ticker)
        )""")

    cte_sql = "WITH " + ",\n".join(cte_parts)

    # Detect which modules are present for SELECT / JOIN
    has = {m: m in modules_present for m in _ANALYTICS_MODULES}

    # Base: returns always needed as primary table
    base = "ret" if has["returns"] else (mod_alias[modules_present[0]])

    select_cols = [f"{base}.ticker"]
    if has["returns"]:
        select_cols.append(f"{base}.date AS as_of")
    if has_prices:
        select_cols.append("px.last_price")

    if has["returns"]:
        select_cols += ["ret.ret_1d", "ret.ret_5d", "ret.ret_21d", "ret.ret_63d", "ret.ret_126d", "ret.ret_252d"]
    if has["risk"]:
        select_cols += ["rsk.vol_21d", "rsk.vol_252d", "rsk.sharpe_63d", "rsk.sharpe_252d",
                        "rsk.sortino_252d", "rsk.max_dd_252d", "rsk.beta_252d", "rsk.alpha_252d",
                        "rsk.var_95_21d", "rsk.cvar_95_21d"]
    if has["momentum"]:
        select_cols += ["mom.mom_1m", "mom.mom_3m", "mom.mom_6m", "mom.mom_12m", "mom.mom_12m_skip1m",
                        "mom.hi_52w_pct", "mom.lo_52w_pct", "mom.rs_vs_bench", "mom.rank_universe"]
    if has["technical"]:
        select_cols += ["tec.rsi_14", "tec.bb_pct_b", "tec.bb_width", "tec.macd_hist",
                        "tec.ma_50", "tec.ma_200", "tec.pct_vs_ma50", "tec.pct_vs_ma200",
                        "tec.golden_cross", "tec.atr_14"]
    if has["zscore"]:
        select_cols += ["zsc.z_price_21d", "zsc.z_price_63d", "zsc.z_ret_21d",
                        "zsc.z_cs_ret_1d", "zsc.pct_ret_1d", "zsc.pct_mom_3m"]

    joins = []
    for mod in _ANALYTICS_MODULES:
        if not has[mod] or mod == "returns":
            continue
        alias = mod_alias[mod]
        joins.append(f"LEFT JOIN {alias} USING (ticker)")
    if has_prices:
        joins.append("LEFT JOIN px USING (ticker)")

    # Cap filter for US_EQ
    cap_filter = ""
    if market == "US_EQ" and cap_category:
        price_uri = _uri(price_key)
        cte_parts.append(f"""
        cap AS (
            SELECT DISTINCT ticker, cap_category
            FROM read_parquet('{price_uri}')
            WHERE UPPER(cap_category) = '{cap_category.upper()}'
        )""")
        joins.append(f"JOIN cap USING (ticker)")
        # Rebuild CTE string to include cap
        cte_sql = "WITH " + ",\n".join(cte_parts)

    sql = f"""
    {cte_sql}
    SELECT {', '.join(select_cols)}
    FROM {base}
    {chr(10).join(joins)}
    ORDER BY {base}.ticker
    """

    try:
        df = conn.execute(sql).df()
        if "as_of" in df.columns:
            df["as_of"] = pd.to_datetime(df["as_of"])
        return df
    except Exception as e:
        logger.error("[analytics_store] get_snapshot_s3 failed for market=%s: %s", market, e)
        return pd.DataFrame()


def _get_ohlcv_s3(ticker: str, market: str, lookback_days: int = 504) -> pd.DataFrame:
    from backend.db.s3_store import market_key, key_exists, s3_uri
    from backend.db.duckdb_store import get_conn
    key = market_key(market)
    if not key_exists(key):
        return pd.DataFrame()
    uri = s3_uri(key)
    sql = f"""
        SELECT date AS "Date", open AS "Open", high AS "High", low AS "Low",
               close AS "Close", volume AS "Volume"
        FROM read_parquet('{uri}')
        WHERE UPPER(ticker) = UPPER('{ticker}')
          AND date >= CURRENT_DATE - INTERVAL '{lookback_days} days'
        ORDER BY date
    """
    try:
        df = get_conn().execute(sql).df()
        if df.empty:
            return pd.DataFrame()
        df["Date"] = pd.to_datetime(df["Date"])
        return df.sort_values("Date").reset_index(drop=True)
    except Exception as e:
        logger.error("[analytics_store] get_ohlcv_s3 failed for %s: %s", ticker, e)
        return pd.DataFrame()


def _get_analytics_history_s3(
    ticker: str, market: str, lookback_days: int = 504
) -> dict[str, pd.DataFrame]:
    from backend.db.s3_store import analytics_key, key_exists, s3_uri
    from backend.db.duckdb_store import get_conn

    tables_cols = {
        "returns":   "ret_1d, ret_5d, ret_21d, ret_63d, ret_126d, ret_252d",
        "risk":      "vol_21d, vol_63d, vol_252d, sharpe_63d, sharpe_252d, sortino_252d, max_dd_252d, beta_252d, alpha_252d",
        "momentum":  "mom_1m, mom_3m, mom_6m, mom_12m, hi_52w_pct, lo_52w_pct, rs_vs_bench",
        "zscore":    "z_price_21d, z_price_63d, z_ret_21d, z_cs_ret_1d, pct_ret_1d, pct_mom_3m",
        "technical": "rsi_14, bb_upper, bb_mid, bb_lower, bb_pct_b, bb_width, macd_line, macd_signal, macd_hist, ma_20, ma_50, ma_200, pct_vs_ma50, pct_vs_ma200, golden_cross",
    }
    result: dict[str, pd.DataFrame] = {}
    conn = get_conn()

    for name, cols in tables_cols.items():
        key = analytics_key(name, market)
        if not key_exists(key):
            continue
        uri = s3_uri(key)
        sql = f"""
            SELECT date, {cols}
            FROM read_parquet('{uri}')
            WHERE UPPER(ticker) = UPPER('{ticker}')
              AND market = '{market}'
              AND date >= CURRENT_DATE - INTERVAL '{lookback_days} days'
            ORDER BY date
        """
        try:
            df = conn.execute(sql).df()
            if not df.empty:
                df["date"] = pd.to_datetime(df["date"])
                df = df.set_index("date").sort_index()
                result[name] = df
        except Exception as e:
            logger.error("[analytics_store] history_s3 %s/%s/%s: %s", name, market, ticker, e)

    return result


def _get_tickers_s3(market: str, cap_category: str | None = None) -> list[str]:
    from backend.db.s3_store import market_key, key_exists, s3_uri
    from backend.db.duckdb_store import get_conn
    key = market_key(market)
    if not key_exists(key):
        return []
    uri = s3_uri(key)
    if cap_category and market == "US_EQ":
        sql = f"SELECT DISTINCT ticker FROM read_parquet('{uri}') WHERE UPPER(cap_category) = UPPER('{cap_category}') ORDER BY ticker"
    else:
        sql = f"SELECT DISTINCT ticker FROM read_parquet('{uri}') ORDER BY ticker"
    try:
        df = get_conn().execute(sql).df()
        return df["ticker"].tolist()
    except Exception as e:
        logger.error("[analytics_store] get_tickers_s3 failed: %s", e)
        return []


# ══════════════════════════════════════════════════════════════════════
# PostgreSQL implementations (legacy / fallback)
# ══════════════════════════════════════════════════════════════════════

_SNAPSHOT_SQL = """
WITH ld AS (
    SELECT MAX(date) AS max_date FROM analytics.returns WHERE market = %(market)s
),
ret AS (
    SELECT ticker, ret_1d, ret_5d, ret_21d, ret_63d, ret_126d, ret_252d
    FROM analytics.returns, ld WHERE market = %(market)s AND date = ld.max_date
),
rsk AS (
    SELECT ticker, vol_21d, vol_63d, vol_252d, sharpe_63d, sharpe_252d,
           sortino_252d, max_dd_252d, beta_252d, alpha_252d, var_95_21d, cvar_95_21d
    FROM analytics.risk, ld WHERE market = %(market)s AND date = ld.max_date
),
mom AS (
    SELECT ticker, mom_1m, mom_3m, mom_6m, mom_12m, mom_12m_skip1m,
           hi_52w_pct, lo_52w_pct, rs_vs_bench, rank_universe
    FROM analytics.momentum, ld WHERE market = %(market)s AND date = ld.max_date
),
tec AS (
    SELECT ticker, rsi_14, bb_pct_b, bb_width, macd_line, macd_signal, macd_hist,
           ma_20, ma_50, ma_200, pct_vs_ma50, pct_vs_ma200, golden_cross, atr_14
    FROM analytics.technical, ld WHERE market = %(market)s AND date = ld.max_date
),
zsc AS (
    SELECT ticker, z_price_21d, z_price_63d, z_volume_21d, z_ret_21d,
           z_cs_ret_1d, z_cs_price_vs_ma20, pct_ret_1d, pct_mom_3m
    FROM analytics.zscore, ld WHERE market = %(market)s AND date = ld.max_date
),
px AS (
    SELECT ticker, close AS last_price
    FROM {price_table}
    WHERE (ticker, date) IN (
        SELECT ticker, MAX(date) FROM {price_table} GROUP BY ticker
    )
)
SELECT
    ret.ticker,
    ld.max_date                          AS as_of,
    px.last_price,
    ret.ret_1d, ret.ret_5d, ret.ret_21d, ret.ret_63d, ret.ret_126d, ret.ret_252d,
    rsk.vol_21d, rsk.vol_252d, rsk.sharpe_63d, rsk.sharpe_252d, rsk.sortino_252d,
    rsk.max_dd_252d, rsk.beta_252d, rsk.alpha_252d, rsk.var_95_21d, rsk.cvar_95_21d,
    mom.mom_1m, mom.mom_3m, mom.mom_6m, mom.mom_12m, mom.mom_12m_skip1m,
    mom.hi_52w_pct, mom.lo_52w_pct, mom.rs_vs_bench, mom.rank_universe,
    tec.rsi_14, tec.bb_pct_b, tec.bb_width, tec.macd_hist,
    tec.ma_50, tec.ma_200, tec.pct_vs_ma50, tec.pct_vs_ma200, tec.golden_cross, tec.atr_14,
    zsc.z_price_21d, zsc.z_price_63d, zsc.z_ret_21d,
    zsc.z_cs_ret_1d, zsc.pct_ret_1d, zsc.pct_mom_3m
FROM ret
CROSS JOIN ld
LEFT JOIN rsk USING (ticker)
LEFT JOIN mom USING (ticker)
LEFT JOIN tec USING (ticker)
LEFT JOIN zsc USING (ticker)
LEFT JOIN px  USING (ticker)
{cap_filter}
ORDER BY ret.ticker
"""


def _get_snapshot_pg(market: str, cap_category: str | None = None) -> pd.DataFrame:
    from backend.db.postgres_store import _connect
    if cap_category and market == "US_EQ":
        cap_join = f"""
        JOIN (SELECT DISTINCT ticker, cap_category FROM us_equity_prices) cap
          ON ret.ticker = cap.ticker AND cap.cap_category = '{cap_category.upper()}'
        """
    elif market == "US_EQ":
        cap_join = """
        LEFT JOIN (SELECT DISTINCT ticker, cap_category FROM us_equity_prices) cap
          ON ret.ticker = cap.ticker
        """
    else:
        cap_join = ""

    sql = _SNAPSHOT_SQL.format(price_table=_price_table(market), cap_filter=cap_join)
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"market": market})
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
        df = pd.DataFrame(rows, columns=cols)
        df["as_of"] = pd.to_datetime(df["as_of"])
        if "cap_category" not in df.columns and market == "US_EQ" and not df.empty:
            df["cap_category"] = None
        return df
    except Exception as e:
        logger.error("get_snapshot_pg failed for market=%s: %s", market, e)
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════
# Public API  (backend-agnostic)
# ══════════════════════════════════════════════════════════════════════

def get_snapshot(market: str, cap_category: str | None = None) -> pd.DataFrame:
    """
    One row per ticker: latest values from all 5 analytics tables joined.
    For US_EQ market, optionally filter by cap_category ('LARGE','MID','SMALL').
    """
    if not is_enabled():
        return pd.DataFrame()
    if _s3_enabled():
        return _get_snapshot_s3(market, cap_category)
    return _get_snapshot_pg(market, cap_category)


def get_ohlcv(ticker: str, market: str, lookback_days: int = 504) -> pd.DataFrame:
    """
    Raw OHLCV for a single ticker from the appropriate price table/file.
    lookback_days=504 ≈ 2 trading years.
    """
    if not is_enabled():
        return pd.DataFrame()
    if _s3_enabled():
        return _get_ohlcv_s3(ticker, market, lookback_days)
    # Postgres fallback
    try:
        from backend.db.postgres_store import _connect
        table = _price_table(market)
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT date, open, high, low, close, volume
                    FROM {table}
                    WHERE ticker = %(ticker)s
                      AND date >= NOW() - INTERVAL '{lookback_days} days'
                    ORDER BY date
                """, {"ticker": ticker.upper()})
                rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Volume"])
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_convert(None)
        return df.sort_values("Date").reset_index(drop=True)
    except Exception as e:
        logger.error("get_ohlcv_pg failed for %s/%s: %s", market, ticker, e)
        return pd.DataFrame()


def get_analytics_history(
    ticker: str, market: str, lookback_days: int = 504
) -> dict[str, pd.DataFrame]:
    """
    Returns dict keyed by module name, each a DataFrame indexed by date.
    Keys: 'returns', 'risk', 'momentum', 'zscore', 'technical'
    """
    if not is_enabled():
        return {}
    if _s3_enabled():
        return _get_analytics_history_s3(ticker, market, lookback_days)
    # Postgres fallback
    result = {}
    tables = {
        "returns":   "ret_1d, ret_5d, ret_21d, ret_63d, ret_126d, ret_252d",
        "risk":      "vol_21d, vol_63d, vol_252d, sharpe_63d, sharpe_252d, sortino_252d, max_dd_252d, beta_252d, alpha_252d",
        "momentum":  "mom_1m, mom_3m, mom_6m, mom_12m, hi_52w_pct, lo_52w_pct, rs_vs_bench",
        "zscore":    "z_price_21d, z_price_63d, z_ret_21d, z_cs_ret_1d, pct_ret_1d, pct_mom_3m",
        "technical": "rsi_14, bb_upper, bb_mid, bb_lower, bb_pct_b, bb_width, macd_line, macd_signal, macd_hist, ma_20, ma_50, ma_200, pct_vs_ma50, pct_vs_ma200, golden_cross",
    }
    try:
        from backend.db.postgres_store import _connect
        with _connect() as conn:
            for name, cols in tables.items():
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT date, {cols}
                        FROM analytics.{name}
                        WHERE ticker = %(ticker)s AND market = %(market)s
                          AND date >= NOW() - INTERVAL '{lookback_days} days'
                        ORDER BY date
                    """, {"ticker": ticker.upper(), "market": market})
                    rows = cur.fetchall()
                    col_names = ["date"] + [c.strip() for c in cols.split(",")]
                if rows:
                    df = pd.DataFrame(rows, columns=col_names)
                    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert(None)
                    df = df.set_index("date").sort_index()
                    result[name] = df
    except Exception as e:
        logger.error("get_analytics_history_pg failed for %s/%s: %s", market, ticker, e)
    return result


def get_tickers(market: str, cap_category: str | None = None) -> list[str]:
    """Return sorted list of tickers in a market/cap segment."""
    if not is_enabled():
        return []
    if _s3_enabled():
        return _get_tickers_s3(market, cap_category)
    # Postgres fallback
    table = _price_table(market)
    try:
        from backend.db.postgres_store import _connect
        with _connect() as conn:
            with conn.cursor() as cur:
                if cap_category and market == "US_EQ":
                    cur.execute(
                        f"SELECT DISTINCT ticker FROM {table} WHERE cap_category = %s ORDER BY ticker",
                        (cap_category.upper(),),
                    )
                else:
                    cur.execute(f"SELECT DISTINCT ticker FROM {table} ORDER BY ticker")
                return [r[0] for r in cur.fetchall()]
    except Exception as e:
        logger.error("get_tickers_pg failed: %s", e)
        return []
