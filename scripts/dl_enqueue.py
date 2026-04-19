#!/usr/bin/env python3
"""
AssetEra Data Layer — Enqueuer CLI
===================================
Publish ingest messages to SQS (or local queue in dev mode).

Usage
─────
  # Full daily refresh (all asset classes, today only):
  python scripts/dl_enqueue.py

  # Historical backfill (default: 10 years):
  python scripts/dl_enqueue.py --backfill

  # Single asset class:
  python scripts/dl_enqueue.py --asset-class equities

  # Custom date range:
  python scripts/dl_enqueue.py --start 2020-01-01 --end 2026-04-20

  # Init S3 catalog then enqueue:
  python scripts/dl_enqueue.py --init-catalog

  # Dry run (print messages, don't publish):
  python scripts/dl_enqueue.py --dry-run

Environment variables required (S3 mode):
  DATA_BUCKET, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

Queue env vars (optional — defaults used if not set):
  QUEUE_EQUITIES, QUEUE_ETF, QUEUE_FIXED_INCOME, QUEUE_FRED
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AssetEra data layer enqueuer")
    p.add_argument(
        "--asset-class", "-a",
        choices=["equities", "etf", "fixed_income", "macro", "all"],
        default="all",
        help="Asset class to enqueue (default: all)",
    )
    p.add_argument(
        "--start", "-s",
        default=None,
        help="Start date YYYY-MM-DD (default: today for daily, 10yr ago for backfill)",
    )
    p.add_argument(
        "--end", "-e",
        default=date.today().isoformat(),
        help="End date YYYY-MM-DD (default: today)",
    )
    p.add_argument(
        "--backfill",
        action="store_true",
        help="Full historical backfill (10 years). Sets start to 10yr ago.",
    )
    p.add_argument(
        "--run-id",
        default=None,
        help="Override run_id (default: current UTC timestamp)",
    )
    p.add_argument(
        "--init-catalog",
        action="store_true",
        help="Initialize S3 catalog before enqueueing",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print messages without publishing",
    )
    p.add_argument(
        "--run-inline",
        action="store_true",
        help="Skip queuing and process all messages immediately (local mode)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    from datalayer.schemas import HISTORY_START
    from datalayer.s3 import is_configured

    if not is_configured() and not args.dry_run and not args.run_inline:
        logger.error(
            "S3 not configured. Set DATA_BUCKET + AWS credentials in .env\n"
            "  Use --dry-run to preview messages without S3, or\n"
            "  use --run-inline for local in-process execution."
        )
        return 1

    # Date range
    start = args.start or (HISTORY_START if args.backfill else
                           (date.today() - timedelta(days=5)).isoformat())
    end   = args.end

    logger.info("Run window: %s → %s", start, end)

    # Init catalog
    if args.init_catalog:
        from datalayer.catalog import init as catalog_init
        catalog_init()
        logger.info("S3 catalog initialized")

    # Determine asset classes
    from datalayer.schemas import (
        ASSET_EQUITIES, ASSET_ETF, ASSET_FIXED_INCOME, ASSET_MACRO,
    )
    if args.asset_class == "all":
        targets = [ASSET_EQUITIES, ASSET_ETF, ASSET_FIXED_INCOME, ASSET_MACRO]
    else:
        targets = [args.asset_class]

    # Dry run: just print
    if args.dry_run:
        from datalayer.schemas import (
            ALL_EQUITIES, EQUITIES_CAP, ETF_TICKERS,
            FIXED_INCOME_TICKERS, FRED_SERIES_IDS,
            ASSET_CLASS_SOURCE, make_message,
        )
        from datalayer.enqueuer import _run_id_now
        run_id = args.run_id or _run_id_now()
        universe_map = {
            ASSET_EQUITIES:     ALL_EQUITIES,
            ASSET_ETF:          ETF_TICKERS,
            ASSET_FIXED_INCOME: FIXED_INCOME_TICKERS,
            ASSET_MACRO:        FRED_SERIES_IDS,
        }
        total = 0
        for ac in targets:
            symbols = universe_map[ac]
            for sym in symbols:
                msg = make_message(ac, sym, ASSET_CLASS_SOURCE[ac], run_id,
                                   start_date=start, end_date=end)
                print(json.dumps(msg))
                total += 1
        logger.info("Dry run — would publish %d messages", total)
        return 0

    # Inline (local) mode: process messages directly without a queue
    if args.run_inline:
        from datalayer.manifest import RunManifest
        from datalayer.worker import process_message
        from datalayer.schemas import (
            ALL_EQUITIES, EQUITIES_CAP, ETF_TICKERS,
            FIXED_INCOME_TICKERS, FRED_SERIES_IDS,
            ASSET_CLASS_SOURCE, make_message,
        )
        from datalayer.enqueuer import _run_id_now

        run_id   = args.run_id or _run_id_now()
        run_date = date.today().isoformat()
        manifest = RunManifest(run_id=run_id, run_date=run_date)
        manifest.start()

        universe_map = {
            ASSET_EQUITIES:     ALL_EQUITIES,
            ASSET_ETF:          ETF_TICKERS,
            ASSET_FIXED_INCOME: FIXED_INCOME_TICKERS,
            ASSET_MACRO:        FRED_SERIES_IDS,
        }
        ok = fail = 0
        failed: list[str] = []
        for ac in targets:
            symbols = universe_map[ac]
            logger.info("Processing %d symbols for %s …", len(symbols), ac)
            for sym in symbols:
                msg = make_message(ac, sym, ASSET_CLASS_SOURCE[ac], run_id,
                                   start_date=start, end_date=end)
                success = process_message(msg, manifest, compute_features=True)
                if success:
                    ok += 1
                else:
                    fail += 1
                    failed.append(f"{ac}:{sym}")

        manifest.finish(ok, fail, failed)
        _print_summary(run_id, targets, ok, fail, failed)
        return 0 if fail == 0 else 1

    # Normal SQS enqueue mode
    from datalayer.enqueuer import enqueue_all, enqueue_asset_class
    from datalayer.enqueuer import _run_id_now

    run_id = args.run_id or _run_id_now()
    logger.info("Enqueueing run_id=%s …", run_id)

    if args.asset_class == "all":
        summary = enqueue_all(run_id=run_id, start_date=start, end_date=end)
    else:
        result  = enqueue_asset_class(args.asset_class, run_id,
                                       start_date=start, end_date=end)
        summary = {"run_id": run_id, args.asset_class: result,
                   "total_ok": result["ok"], "total_fail": result["fail"]}

    _print_summary(
        summary["run_id"], targets,
        summary["total_ok"], summary["total_fail"], [],
    )
    return 0 if summary["total_fail"] == 0 else 1


def _print_summary(run_id, targets, ok, fail, failed):
    print("\n" + "=" * 55)
    print(f"  run_id       : {run_id}")
    print(f"  asset classes: {', '.join(targets)}")
    print(f"  enqueued OK  : {ok}")
    print(f"  failed       : {fail}")
    if failed:
        print(f"  failed list  : {', '.join(failed[:20])}")
        if len(failed) > 20:
            print(f"                  ... and {len(failed) - 20} more")
    print("=" * 55)


if __name__ == "__main__":
    raise SystemExit(main())
