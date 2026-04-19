#!/usr/bin/env python3
"""
AssetEra Data Layer — Worker CLI
==================================
Continuously polls one SQS queue and processes ingest messages.

Usage
─────
  # Process equities queue until empty:
  python scripts/dl_worker.py --asset-class equities

  # Process up to 50 messages then exit:
  python scripts/dl_worker.py --asset-class equities --max 50

  # Skip feature computation (faster, ingest only):
  python scripts/dl_worker.py --asset-class etf --no-features

  # Run all asset-class queues sequentially:
  python scripts/dl_worker.py --asset-class all

  # Compute features for already-ingested symbols (no new ingest):
  python scripts/dl_worker.py --features-only --asset-class equities

  # List recent run statuses:
  python scripts/dl_worker.py --list-runs

Environment variables required:
  DATA_BUCKET, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AssetEra data layer worker")
    p.add_argument(
        "--asset-class", "-a",
        choices=["equities", "etf", "fixed_income", "macro", "all"],
        default="all",
        help="Queue to consume (default: all)",
    )
    p.add_argument(
        "--max", "-m",
        type=int,
        default=1000,
        help="Max messages to process before stopping (default: 1000)",
    )
    p.add_argument(
        "--no-features",
        action="store_true",
        help="Skip feature computation after ingest",
    )
    p.add_argument(
        "--features-only",
        action="store_true",
        help="Recompute features for all curated symbols (no new ingest)",
    )
    p.add_argument(
        "--run-id",
        default=None,
        help="run_id for this worker session (default: auto-generated)",
    )
    p.add_argument(
        "--list-runs",
        action="store_true",
        help="List recent run statuses and exit",
    )
    return p.parse_args()


def _list_runs() -> None:
    from datalayer.manifest import list_runs
    today = date.today().isoformat()
    runs  = list_runs(today)
    if not runs:
        print(f"No runs found for {today}")
        return
    print(f"\nRuns for {today}:")
    for r in runs:
        status = r.get("status", "?")
        ok     = r.get("ok_count", "?")
        fail   = r.get("fail_count", "?")
        upd    = r.get("updated_at", "")[:19]
        print(f"  [{status:8s}]  run_id={r.get('run_id','')}  ok={ok}  fail={fail}  updated={upd}")


def _features_only(asset_class: str, run_id: str) -> dict:
    """Recompute technical features for all curated symbols without re-ingesting."""
    from datalayer.s3 import list_keys
    from datalayer.features.technical import compute_and_write
    from datalayer.schemas import ASSET_CLASS_SOURCE

    source  = ASSET_CLASS_SOURCE.get(asset_class, "yfinance")
    prefix  = f"curated/{asset_class}/source={source}/"
    keys    = list_keys(prefix)
    # Extract symbol names from key pattern curated/{ac}/source={src}/symbol={sym}/data.parquet
    symbols = []
    for k in keys:
        parts = k.split("/")
        for part in parts:
            if part.startswith("symbol="):
                symbols.append(part.removeprefix("symbol="))
                break

    logger.info("[features-only] %d symbols for %s", len(symbols), asset_class)
    ok = fail = 0
    for sym in symbols:
        try:
            success = compute_and_write(sym, asset_class, run_id)
            if success:
                ok += 1
            else:
                fail += 1
        except Exception as e:
            logger.error("[features-only] %s: %s", sym, e)
            fail += 1

    return {"ok": ok, "fail": fail}


def main() -> int:
    args = parse_args()

    if args.list_runs:
        _list_runs()
        return 0

    from datalayer.s3 import is_configured
    if not is_configured():
        logger.error(
            "S3 not configured. Set DATA_BUCKET + AWS credentials in .env"
        )
        return 1

    from datalayer.manifest import RunManifest
    from datalayer.enqueuer import _run_id_now
    from datalayer.schemas import (
        ASSET_EQUITIES, ASSET_ETF, ASSET_FIXED_INCOME, ASSET_MACRO,
    )

    run_id   = args.run_id or _run_id_now()
    run_date = date.today().isoformat()
    manifest = RunManifest(run_id=run_id, run_date=run_date, pipeline="worker")
    manifest.start()

    targets = (
        [ASSET_EQUITIES, ASSET_ETF, ASSET_FIXED_INCOME, ASSET_MACRO]
        if args.asset_class == "all"
        else [args.asset_class]
    )

    total_ok = total_fail = 0

    # ── Features-only mode ────────────────────────────────────────────
    if args.features_only:
        for ac in targets:
            if ac == ASSET_MACRO:
                from datalayer.features.macro import (
                    compute_and_write_series, compute_and_write_composite,
                )
                from datalayer.schemas import FRED_SERIES_IDS
                for sid in FRED_SERIES_IDS:
                    if compute_and_write_series(sid, run_id, manifest):
                        total_ok += 1
                    else:
                        total_fail += 1
                compute_and_write_composite(run_id, manifest)
            else:
                res = _features_only(ac, run_id)
                total_ok   += res["ok"]
                total_fail += res["fail"]

        manifest.finish(total_ok, total_fail)
        _print_summary(run_id, targets, total_ok, total_fail)
        return 0 if total_fail == 0 else 1

    # ── Normal worker loop mode ───────────────────────────────────────
    from datalayer.worker import run_worker_loop

    for ac in targets:
        logger.info("Starting worker loop for: %s", ac)
        res = run_worker_loop(
            asset_class      = ac,
            max_messages     = args.max,
            compute_features = not args.no_features,
            manifest         = manifest,
        )
        total_ok   += res["ok"]
        total_fail += res["fail"]

    # Compute macro composite features after all FRED series are ingested
    if ASSET_MACRO in targets and not args.no_features:
        try:
            from datalayer.features.macro import compute_and_write_composite
            compute_and_write_composite(run_id, manifest)
        except Exception as e:
            logger.warning("[worker] macro composite features failed: %s", e)

    manifest.finish(total_ok, total_fail)
    _print_summary(run_id, targets, total_ok, total_fail)
    return 0 if total_fail == 0 else 1


def _print_summary(run_id, targets, ok, fail):
    print("\n" + "=" * 55)
    print(f"  run_id       : {run_id}")
    print(f"  asset classes: {', '.join(targets)}")
    print(f"  succeeded    : {ok}")
    print(f"  failed       : {fail}")
    print("=" * 55)


if __name__ == "__main__":
    raise SystemExit(main())
