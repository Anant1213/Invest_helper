#!/usr/bin/env python3
"""
CLI driver for the AssetEra analytics pipeline.

Usage examples:
    # Run everything (both markets, all 5 modules)
    python scripts/run_analytics.py

    # Only NSE, only risk + momentum modules
    python scripts/run_analytics.py --market NSE --module risk momentum

    # US only, all modules, 1 year of lookback
    python scripts/run_analytics.py --market US --lookback 365

    # Single module across both markets (useful for debugging)
    python scripts/run_analytics.py --module zscore
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)

from backend.stock_research.analytics.pipeline import run, MARKETS, MODULES  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run AssetEra analytics pipeline.")
    p.add_argument(
        "--market", nargs="+", choices=MARKETS, default=MARKETS,
        help="Markets to process (default: US NSE)",
    )
    p.add_argument(
        "--module", nargs="+", choices=list(MODULES.keys()), default=None,
        help="Modules to run (default: all). Choices: " + " ".join(MODULES.keys()),
    )
    p.add_argument(
        "--lookback", type=int, default=None,
        help=(
            "Calendar days of price history to load. "
            "Default: None = full history (all daily data). "
            "Use e.g. --lookback 400 for quick incremental daily runs."
        ),
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    print(f"Markets : {args.market}")
    print(f"Modules : {args.module or 'all'}")
    print(f"Lookback: {args.lookback or 'full history (all daily data)'}\n")

    summary = run(
        markets=args.market,
        modules=args.module,
        lookback_days=args.lookback,
    )

    print("\n── Summary ──")
    any_failed = False
    for market, mods in summary.items():
        print(f"\n  {market}:")
        for mod, rows in mods.items():
            if rows >= 0:
                print(f"    {mod:<12} {rows:>8,} rows")
            else:
                print(f"    {mod:<12}   FAILED")
                any_failed = True

    return 1 if any_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
