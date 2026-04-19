"""
datalayer.worker
────────────────
Worker: process one ingest message end-to-end.

Flow per message
────────────────
  1. Deserialise message  (run_id, asset_class, source, symbol_or_series, dates)
  2. Route to correct ingest module
  3. Optionally compute technical features after ingest
  4. Ack (delete) message from SQS only after S3 writes succeed

Worker loop (run_worker_loop)
────────────────────────────
  - Poll one asset-class queue
  - Process messages until queue is empty or max_messages reached
  - Supports both SQS mode and local (in-process) mode

Usage
─────
  # Process a single message directly (testing / local run):
  from datalayer.worker import process_message
  ok = process_message(msg)

  # Run a continuous worker loop:
  from datalayer.worker import run_worker_loop
  run_worker_loop(asset_class="equities", max_messages=200)
"""
from __future__ import annotations

import logging

from datalayer.schemas import (
    ASSET_EQUITIES, ASSET_ETF, ASSET_FIXED_INCOME, ASSET_MACRO,
)

logger = logging.getLogger(__name__)


# ── Routing ───────────────────────────────────────────────────────────

def process_message(msg: dict, manifest=None, compute_features: bool = True) -> bool:
    """
    Route one ingest message to the correct handler.
    Returns True on success.
    """
    asset_class = msg.get("asset_class", "")
    symbol      = msg.get("symbol_or_series", "")
    run_id      = msg.get("run_id", "")

    if asset_class == ASSET_EQUITIES:
        from datalayer.ingest.equities import ingest_equity
        ok = ingest_equity(msg, manifest)
    elif asset_class == ASSET_ETF:
        from datalayer.ingest.etf import ingest_etf
        ok = ingest_etf(msg, manifest)
    elif asset_class == ASSET_FIXED_INCOME:
        from datalayer.ingest.fixed_income import ingest_fixed_income
        ok = ingest_fixed_income(msg, manifest)
    elif asset_class == ASSET_MACRO:
        from datalayer.ingest.fred import ingest_fred_series
        ok = ingest_fred_series(msg, manifest)
    else:
        logger.error("[worker] unknown asset_class: %s", asset_class)
        return False

    # Optionally compute features right after ingest
    if ok and compute_features and asset_class != ASSET_MACRO:
        try:
            from datalayer.features.technical import compute_and_write
            compute_and_write(symbol, asset_class, run_id, manifest)
        except Exception as e:
            # Non-fatal: features failure doesn't fail the ingest
            logger.warning("[worker] feature compute failed for %s: %s", symbol, e)

    return ok


# ── Worker loop ───────────────────────────────────────────────────────

def run_worker_loop(
    asset_class: str,
    max_messages: int = 500,
    compute_features: bool = True,
    manifest=None,
) -> dict:
    """
    Poll the queue for `asset_class` and process messages until:
      - the queue is empty, OR
      - max_messages have been processed.

    Returns {"ok": int, "fail": int, "processed": int}.
    """
    from datalayer.queue import get_queue, receive_messages, delete_message

    queue   = get_queue(asset_class)
    ok = fail = processed = 0

    logger.info("[worker-loop] starting  asset_class=%s  queue=%s", asset_class, queue)

    while processed < max_messages:
        batch = list(receive_messages(queue, max_messages=1, wait_seconds=5))
        if not batch:
            logger.info("[worker-loop] queue empty — stopping")
            break

        for receipt_handle, msg in batch:
            success = False
            try:
                success = process_message(msg, manifest, compute_features)
            except Exception as e:
                logger.error("[worker-loop] unhandled error for %s: %s",
                             msg.get("symbol_or_series"), e)

            if success:
                ok += 1
                delete_message(queue, receipt_handle)
            else:
                fail += 1
                # Don't ack on failure — SQS visibility timeout will re-expose
                # (after max receives it goes to DLQ)
            processed += 1

    logger.info("[worker-loop] done  ok=%d  fail=%d  total=%d", ok, fail, processed)
    return {"ok": ok, "fail": fail, "processed": processed}
