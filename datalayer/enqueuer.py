"""
datalayer.enqueuer
──────────────────
Generate and publish all ingest messages for a daily run.

One message per symbol/series — published to the appropriate SQS queue
(or local queue in dev mode).

Usage
─────
  from datalayer.enqueuer import enqueue_all, enqueue_asset_class
  summary = enqueue_all(run_id="20260419T070000Z", start_date="2016-04-14")

  # Single asset class
  summary = enqueue_asset_class("equities", run_id, start_date)
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone

from datalayer.queue import get_queue, publish_batch
from datalayer.schemas import (
    ASSET_CLASS_SOURCE,
    ASSET_EQUITIES, ASSET_ETF, ASSET_FIXED_INCOME, ASSET_MACRO,
    ALL_EQUITIES, EQUITIES_CAP,
    ETF_TICKERS,
    FIXED_INCOME_TICKERS,
    FRED_SERIES_IDS,
    HISTORY_START,
    TODAY,
    make_message,
)

logger = logging.getLogger(__name__)


def _run_id_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ── Per-asset-class enqueue ───────────────────────────────────────────

def enqueue_equities(
    run_id: str,
    start_date: str = HISTORY_START,
    end_date: str = TODAY,
) -> dict:
    queue    = get_queue(ASSET_EQUITIES)
    source   = ASSET_CLASS_SOURCE[ASSET_EQUITIES]
    messages = [
        make_message(
            ASSET_EQUITIES, ticker, source, run_id,
            start_date=start_date, end_date=end_date,
            cap=EQUITIES_CAP.get(ticker),
        )
        for ticker in ALL_EQUITIES
    ]
    result = publish_batch(queue, messages)
    logger.info("[enqueuer] equities  ok=%d  fail=%d", result["ok"], result["fail"])
    return result


def enqueue_etfs(
    run_id: str,
    start_date: str = HISTORY_START,
    end_date: str = TODAY,
) -> dict:
    queue    = get_queue(ASSET_ETF)
    source   = ASSET_CLASS_SOURCE[ASSET_ETF]
    messages = [
        make_message(ASSET_ETF, ticker, source, run_id,
                     start_date=start_date, end_date=end_date)
        for ticker in ETF_TICKERS
    ]
    result = publish_batch(queue, messages)
    logger.info("[enqueuer] etf  ok=%d  fail=%d", result["ok"], result["fail"])
    return result


def enqueue_fixed_income(
    run_id: str,
    start_date: str = HISTORY_START,
    end_date: str = TODAY,
) -> dict:
    queue    = get_queue(ASSET_FIXED_INCOME)
    source   = ASSET_CLASS_SOURCE[ASSET_FIXED_INCOME]
    messages = [
        make_message(ASSET_FIXED_INCOME, ticker, source, run_id,
                     start_date=start_date, end_date=end_date)
        for ticker in FIXED_INCOME_TICKERS
    ]
    result = publish_batch(queue, messages)
    logger.info("[enqueuer] fixed_income  ok=%d  fail=%d", result["ok"], result["fail"])
    return result


def enqueue_fred(
    run_id: str,
    start_date: str = HISTORY_START,
    end_date: str = TODAY,
) -> dict:
    queue    = get_queue(ASSET_MACRO)
    source   = ASSET_CLASS_SOURCE[ASSET_MACRO]
    messages = [
        make_message(ASSET_MACRO, sid, source, run_id,
                     start_date=start_date, end_date=end_date)
        for sid in FRED_SERIES_IDS
    ]
    result = publish_batch(queue, messages)
    logger.info("[enqueuer] fred  ok=%d  fail=%d", result["ok"], result["fail"])
    return result


# ── Routing dispatch ──────────────────────────────────────────────────

_HANDLERS = {
    ASSET_EQUITIES:     enqueue_equities,
    ASSET_ETF:          enqueue_etfs,
    ASSET_FIXED_INCOME: enqueue_fixed_income,
    ASSET_MACRO:        enqueue_fred,
}


def enqueue_asset_class(
    asset_class: str,
    run_id: str | None = None,
    start_date: str = HISTORY_START,
    end_date: str = TODAY,
) -> dict:
    run_id = run_id or _run_id_now()
    fn = _HANDLERS.get(asset_class)
    if fn is None:
        raise ValueError(f"Unknown asset_class: {asset_class!r}")
    return fn(run_id, start_date=start_date, end_date=end_date)


# ── Full daily run ────────────────────────────────────────────────────

def enqueue_all(
    run_id: str | None = None,
    start_date: str = HISTORY_START,
    end_date: str = TODAY,
    asset_classes: list[str] | None = None,
) -> dict:
    """
    Enqueue ingest messages for all (or specified) asset classes.

    Returns summary dict:
      {
        "run_id":    str,
        "equities":  {"ok": int, "fail": int},
        "etf":       {...},
        "fixed_income": {...},
        "macro":     {...},
        "total_ok":  int,
        "total_fail": int,
      }
    """
    run_id = run_id or _run_id_now()
    targets = asset_classes or [ASSET_EQUITIES, ASSET_ETF, ASSET_FIXED_INCOME, ASSET_MACRO]

    summary: dict = {"run_id": run_id}
    total_ok = total_fail = 0

    for ac in targets:
        res = enqueue_asset_class(ac, run_id, start_date=start_date, end_date=end_date)
        summary[ac] = res
        total_ok   += res.get("ok", 0)
        total_fail += res.get("fail", 0)

    summary["total_ok"]   = total_ok
    summary["total_fail"] = total_fail

    logger.info(
        "[enqueuer] run %s  total_enqueued=%d  failed_to_enqueue=%d",
        run_id, total_ok, total_fail,
    )
    return summary
