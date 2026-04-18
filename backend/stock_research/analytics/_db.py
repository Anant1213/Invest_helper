"""
analytics._db
─────────────
Shared database helpers for the analytics layer.

Supports two storage backends, selected automatically:
  1. S3 + DuckDB (parquet)  — when DATA_BUCKET + AWS creds are set
  2. PostgreSQL (Supabase)  — when POSTGRES_URL / DATABASE_URL is set

Write path  : functions ending in save() call upsert_df() (Postgres)
              or write_analytics_to_s3() (S3).
Read path   : load_us_prices() / load_equity_prices() use whichever
              backend is active.

All analytics tables live in the `analytics` schema (Postgres) or as
individual parquet files (S3):  analytics/{module}_{market}.parquet
"""

from __future__ import annotations

import logging
from typing import Sequence

import pandas as pd

logger = logging.getLogger(__name__)


# ── Backend detection ─────────────────────────────────────────────────

def _s3_enabled() -> bool:
    try:
        from backend.db.s3_store import is_enabled
        return is_enabled()
    except Exception:
        return False


def _pg_enabled() -> bool:
    try:
        from backend.db.postgres_store import is_enabled
        return is_enabled()
    except Exception:
        return False


# ── Schema bootstrap (Postgres only) ─────────────────────────────────

_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS analytics;"
_CREATED: set[str] = set()


def ensure_schema():
    if not _pg_enabled() or "schema" in _CREATED:
        return
    try:
        from backend.db.postgres_store import _connect
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_SCHEMA_SQL)
            conn.commit()
        _CREATED.add("schema")
    except Exception as e:
        logger.error("Could not create analytics schema: %s", e)
        raise


def ensure_table(table_sql: str, table_key: str):
    """Run a CREATE TABLE IF NOT EXISTS statement once per process (Postgres only)."""
    if not _pg_enabled() or table_key in _CREATED:
        return
    ensure_schema()
    try:
        from backend.db.postgres_store import _connect
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(table_sql)
            conn.commit()
        _CREATED.add(table_key)
    except Exception as e:
        logger.error("Could not create table %s: %s", table_key, e)
        raise


# ── Upsert helper (Postgres) ──────────────────────────────────────────

def upsert_df(
    df: pd.DataFrame,
    table: str,
    pk_cols: Sequence[str],
    value_cols: Sequence[str],
    batch_size: int = 2000,
) -> int:
    """
    Upsert a DataFrame into a Postgres analytics table.
    Returns the number of rows written.
    """
    if not _pg_enabled():
        return 0
    if df is None or df.empty:
        return 0

    all_cols = list(pk_cols) + list(value_cols)
    df = df[all_cols].copy().dropna(subset=list(pk_cols))

    col_list   = ", ".join(all_cols)
    ph_list    = ", ".join(["%s"] * len(all_cols))
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in value_cols)
    pk_str     = ", ".join(pk_cols)

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({ph_list})
        ON CONFLICT ({pk_str}) DO UPDATE SET {update_set};
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
        from backend.db.postgres_store import _connect
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


# ── S3 write helper ───────────────────────────────────────────────────

def write_analytics_to_s3(df: pd.DataFrame, module: str, market: str) -> int:
    """
    Merge new rows with existing parquet (if any) and write back to S3.
    Returns number of rows in final file.
    """
    if df is None or df.empty:
        return 0
    try:
        from backend.db.s3_store import analytics_key, put_parquet, read_parquet, key_exists
        key = analytics_key(module, market)

        if key_exists(key):
            existing = read_parquet(key)
            # Merge: new rows take precedence on (ticker, market, date) PK
            combined = pd.concat([existing, df], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=["ticker", "market", "date"], keep="last"
            ).sort_values(["ticker", "date"])
        else:
            combined = df.sort_values(["ticker", "date"])

        put_parquet(key, combined)
        logger.info("[_db] wrote %d rows → s3:%s", len(combined), key)
        return len(combined)
    except Exception as e:
        logger.error("write_analytics_to_s3 failed for %s/%s: %s", module, market, e)
        return 0


# ── Last date helpers ─────────────────────────────────────────────────

def last_date_in_table(table: str, market: str) -> pd.Timestamp | None:
    """Return the latest analytics date for incremental refresh (Postgres mode)."""
    if not _pg_enabled():
        return None
    try:
        from backend.db.postgres_store import _connect
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


def last_date_in_s3(module: str, market: str) -> pd.Timestamp | None:
    """Return the latest analytics date in an S3 parquet file."""
    try:
        from backend.db.s3_store import analytics_key, key_exists, read_parquet
        key = analytics_key(module, market)
        if not key_exists(key):
            return None
        df = read_parquet(key)
        if df.empty or "date" not in df.columns:
            return None
        return pd.Timestamp(df["date"].max())
    except Exception:
        return None


# ── Price loader helpers ──────────────────────────────────────────────

def load_us_prices(lookback_days: int | None = None) -> pd.DataFrame:
    """
    Load all daily OHLCV from us_prices (US ETFs / fund stocks).
    Uses S3 backend if configured, else falls back to Postgres.
    lookback_days=None → full history.
    """
    if _s3_enabled():
        return _load_prices_s3("US", lookback_days)
    return _load_us_prices_pg(lookback_days)


def load_equity_prices(lookback_days: int | None = None) -> pd.DataFrame:
    """
    Load all daily OHLCV from us_equity_prices (US large/mid/small cap stocks).
    Uses S3 backend if configured, else falls back to Postgres.
    """
    if _s3_enabled():
        return _load_prices_s3("US_EQ", lookback_days)
    return _load_equity_prices_pg(lookback_days)


# Legacy alias
load_nse_prices = load_equity_prices


# ── S3 price loader ───────────────────────────────────────────────────

def _load_prices_s3(market: str, lookback_days: int | None = None) -> pd.DataFrame:
    try:
        from backend.db.s3_store import market_key, read_parquet, key_exists
        key = market_key(market)
        if not key_exists(key):
            logger.warning("[_db] S3 price file not found: %s", key)
            return pd.DataFrame()
        df = read_parquet(key)

        if lookback_days:
            from datetime import date, timedelta
            cutoff = pd.Timestamp(date.today() - timedelta(days=lookback_days))
            df = df[pd.to_datetime(df["Date"]) >= cutoff]

        df["Date"] = pd.to_datetime(df["Date"])
        return df.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    except Exception as e:
        logger.error("[_db] _load_prices_s3 failed for %s: %s", market, e)
        return pd.DataFrame()


# ── Postgres price loaders (fallback) ─────────────────────────────────

def _load_us_prices_pg(lookback_days: int | None = None) -> pd.DataFrame:
    try:
        from backend.db.postgres_store import _connect
        with _connect() as conn:
            with conn.cursor() as cur:
                if lookback_days:
                    from datetime import date, timedelta
                    cutoff = date.today() - timedelta(days=lookback_days)
                    cur.execute(
                        "SELECT date, ticker, open, high, low, close, adj_close, volume "
                        "FROM us_prices WHERE date >= %s ORDER BY ticker, date",
                        (cutoff,),
                    )
                else:
                    cur.execute(
                        "SELECT date, ticker, open, high, low, close, adj_close, volume "
                        "FROM us_prices ORDER BY ticker, date"
                    )
                rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(
            rows, columns=["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
        )
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_convert(None)
        return df.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    except Exception as e:
        logger.error("Failed loading us_prices (pg): %s", e)
        return pd.DataFrame()


def _load_equity_prices_pg(lookback_days: int | None = None) -> pd.DataFrame:
    try:
        from backend.db.postgres_store import _connect
        with _connect() as conn:
            with conn.cursor() as cur:
                if lookback_days:
                    from datetime import date, timedelta
                    cutoff = date.today() - timedelta(days=lookback_days)
                    cur.execute(
                        "SELECT date, ticker, open, high, low, close, adj_close, volume, cap_category "
                        "FROM us_equity_prices WHERE date >= %s ORDER BY ticker, date",
                        (cutoff,),
                    )
                else:
                    cur.execute(
                        "SELECT date, ticker, open, high, low, close, adj_close, volume, cap_category "
                        "FROM us_equity_prices ORDER BY ticker, date"
                    )
                rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(
            rows,
            columns=["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume", "CapCategory"],
        )
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_convert(None)
        return df.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    except Exception as e:
        logger.error("Failed loading us_equity_prices (pg): %s", e)
        return pd.DataFrame()
