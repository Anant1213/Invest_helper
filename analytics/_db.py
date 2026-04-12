"""
Shared database helpers for the analytics layer.

All analytics tables live in the `analytics` schema.
Raw price tables (us_prices, nse_prices) stay in `public`.

Each analytics table uses (ticker, market, date) as its composite PK.
market is always 'US' or 'NSE'.
"""

from __future__ import annotations

import logging
from typing import Sequence

import pandas as pd

from backend.postgres_store import _connect, is_enabled

logger = logging.getLogger(__name__)

# ── Schema bootstrap ──────────────────────────────────────────────────

_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS analytics;"

# Track which module schemas have been created this process lifetime.
_CREATED: set[str] = set()


def ensure_schema():
    """Create analytics schema if it doesn't exist."""
    if "schema" in _CREATED:
        return
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_SCHEMA_SQL)
            conn.commit()
        _CREATED.add("schema")
    except Exception as e:
        logger.error("Could not create analytics schema: %s", e)
        raise


def ensure_table(table_sql: str, table_key: str):
    """Run a CREATE TABLE IF NOT EXISTS statement once per process."""
    if table_key in _CREATED:
        return
    ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(table_sql)
            conn.commit()
        _CREATED.add(table_key)
    except Exception as e:
        logger.error("Could not create table %s: %s", table_key, e)
        raise


# ── Generic upsert helper ─────────────────────────────────────────────

def upsert_df(
    df: pd.DataFrame,
    table: str,             # fully qualified: analytics.returns
    pk_cols: Sequence[str], # e.g. ["ticker", "market", "date"]
    value_cols: Sequence[str],
    batch_size: int = 2000,
) -> int:
    """
    Upsert a DataFrame into an analytics table.
    Returns the number of rows written.
    """
    if df is None or df.empty:
        return 0

    all_cols = list(pk_cols) + list(value_cols)
    df = df[all_cols].copy()
    df = df.dropna(subset=list(pk_cols))

    col_list  = ", ".join(all_cols)
    ph_list   = ", ".join(["%s"] * len(all_cols))
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in value_cols)
    pk_str    = ", ".join(pk_cols)

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({ph_list})
        ON CONFLICT ({pk_str}) DO UPDATE SET
        {update_set};
    """

    records = [
        tuple(
            r[c].to_pydatetime() if hasattr(r[c], "to_pydatetime")
            else (None if pd.isna(r[c]) else r[c])
            for c in all_cols
        )
        for _, r in df.iterrows()
    ]

    written = 0
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                for i in range(0, len(records), batch_size):
                    batch = records[i : i + batch_size]
                    cur.executemany(sql, batch)
                    written += len(batch)
            conn.commit()
    except Exception as e:
        logger.error("Upsert failed for %s: %s", table, e)
        return 0

    return written


# ── Incremental date helper ───────────────────────────────────────────

def last_date_in_table(table: str, market: str) -> pd.Timestamp | None:
    """
    Return the latest date currently in an analytics table for a given market.
    Used to decide how far back to re-fetch raw prices before recomputing.
    """
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT MAX(date) FROM {table} WHERE market = %s", (market,)
                )
                row = cur.fetchone()
        if row and row[0]:
            return pd.Timestamp(row[0])
    except Exception:
        pass
    return None


# ── Price loader helpers ──────────────────────────────────────────────

def load_us_prices(lookback_days: int | None = None) -> pd.DataFrame:
    """
    Load all daily OHLCV from us_prices.
    All analytics are computed from daily data — no separate monthly/weekly
    tables exist; rolling windows (21d, 63d, 252d …) are calculated directly
    from daily rows using pct_change(periods=N).

    Args:
        lookback_days: if set, only load the most recent N calendar days.
                       None (default) = load full history.
    """
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                if lookback_days:
                    from datetime import date, timedelta
                    cutoff = date.today() - timedelta(days=lookback_days)
                    cur.execute("""
                        SELECT date, ticker, open, high, low, close, adj_close, volume
                        FROM us_prices
                        WHERE date >= %s
                        ORDER BY ticker, date
                    """, (cutoff,))
                else:
                    cur.execute("""
                        SELECT date, ticker, open, high, low, close, adj_close, volume
                        FROM us_prices
                        ORDER BY ticker, date
                    """)
                rows = cur.fetchall()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"])
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_convert(None)
        return df.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    except Exception as e:
        logger.error("Failed loading us_prices: %s", e)
        return pd.DataFrame()


def load_nse_prices(lookback_days: int | None = None) -> pd.DataFrame:
    """
    Load all daily OHLCV from nse_prices.
    All analytics are computed from daily data — rolling windows (21d, 63d, 252d …)
    are derived from daily rows.

    Args:
        lookback_days: if set, only load the most recent N calendar days.
                       None (default) = load full history.
    """
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                if lookback_days:
                    from datetime import date, timedelta
                    cutoff = date.today() - timedelta(days=lookback_days)
                    cur.execute("""
                        SELECT date, ticker, open, high, low, close, adj_close, volume, cap_category
                        FROM nse_prices
                        WHERE date >= %s
                        ORDER BY ticker, date
                    """, (cutoff,))
                else:
                    cur.execute("""
                        SELECT date, ticker, open, high, low, close, adj_close, volume, cap_category
                        FROM nse_prices
                        ORDER BY ticker, date
                    """)
                rows = cur.fetchall()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume", "CapCategory"])
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_convert(None)
        return df.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    except Exception as e:
        logger.error("Failed loading nse_prices: %s", e)
        return pd.DataFrame()
