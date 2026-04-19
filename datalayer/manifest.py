"""
datalayer.manifest
──────────────────
Run lifecycle tracking — writes to control/ zone in S3.

Every ingest run has:
  status.json        — overall run status (pending / running / done / failed)
  written_objects.json — list of every S3 key written in this run
  quality_report.json  — per-symbol quality checks

Usage
─────
  from datalayer.manifest import RunManifest
  m = RunManifest(run_id="20260419T070000Z", run_date="2026-04-19")
  m.start()
  m.record_write("curated/equities/...")
  m.finish(ok_count=150, fail_count=2, failed_symbols=["RUSHA"])
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import datalayer.s3 as s3

logger = logging.getLogger(__name__)


@dataclass
class RunManifest:
    run_id:   str
    run_date: str          # YYYY-MM-DD
    pipeline: str = "ingest"

    _written:  list[str]        = field(default_factory=list, repr=False)
    _quality:  list[dict]       = field(default_factory=list, repr=False)
    _metadata: dict[str, Any]   = field(default_factory=dict, repr=False)

    # ── Lifecycle ─────────────────────────────────────────────────────

    def start(self, total_symbols: int = 0) -> None:
        """Write initial RUNNING status to control/runs/."""
        self._metadata["total_symbols"] = total_symbols
        self._write_status("running")
        logger.info("[manifest] run %s started — %d symbols", self.run_id, total_symbols)

    def finish(
        self,
        ok_count: int,
        fail_count: int,
        failed_symbols: list[str] | None = None,
    ) -> None:
        """Write final DONE (or PARTIAL) status and close the manifest."""
        self._metadata.update({
            "ok_count":       ok_count,
            "fail_count":     fail_count,
            "failed_symbols": failed_symbols or [],
        })
        status = "done" if fail_count == 0 else "partial"
        self._write_status(status)
        self._write_objects()
        logger.info(
            "[manifest] run %s %s — ok=%d fail=%d",
            self.run_id, status, ok_count, fail_count,
        )

    def fail(self, error: str) -> None:
        """Mark run as failed (unrecoverable error)."""
        self._metadata["error"] = error
        self._write_status("failed")
        logger.error("[manifest] run %s failed: %s", self.run_id, error)

    # ── Recording ─────────────────────────────────────────────────────

    def record_write(self, key: str) -> None:
        """Register an S3 key written during this run."""
        self._written.append(key)

    def record_quality(
        self,
        symbol: str,
        asset_class: str,
        rows: int,
        missing_pct: float,
        duplicate_rows: int,
        first_date: str,
        last_date: str,
        passed: bool,
    ) -> None:
        self._quality.append({
            "symbol":         symbol,
            "asset_class":    asset_class,
            "rows":           rows,
            "missing_pct":    round(missing_pct, 4),
            "duplicate_rows": duplicate_rows,
            "first_date":     first_date,
            "last_date":      last_date,
            "passed":         passed,
        })

    # ── S3 writes ─────────────────────────────────────────────────────

    def _write_status(self, status: str) -> None:
        payload = {
            "run_id":    self.run_id,
            "run_date":  self.run_date,
            "pipeline":  self.pipeline,
            "status":    status,
            "updated_at": s3.now_utc(),
            **self._metadata,
        }
        try:
            s3.put_json(s3.run_status_key(self.run_date, self.run_id), payload)
        except Exception as e:
            logger.warning("[manifest] could not write status: %s", e)

    def _write_objects(self) -> None:
        try:
            s3.put_json(
                s3.run_manifest_key(self.run_date, self.run_id),
                {"run_id": self.run_id, "keys": self._written},
            )
        except Exception as e:
            logger.warning("[manifest] could not write object manifest: %s", e)
        if self._quality:
            try:
                s3.put_json(
                    s3.quality_key(self.run_date, self.run_id),
                    {"run_id": self.run_id, "checks": self._quality},
                )
            except Exception as e:
                logger.warning("[manifest] could not write quality report: %s", e)


# ── Checkpoint helpers ─────────────────────────────────────────────────
# A checkpoint stores the last successfully processed symbol per pipeline/asset_class.
# Workers can resume from checkpoints after failures.

def read_checkpoint(pipeline: str, asset_class: str) -> dict:
    key = s3.checkpoint_key(pipeline, asset_class)
    try:
        return s3.read_json(key)
    except Exception:
        return {}


def write_checkpoint(pipeline: str, asset_class: str, payload: dict) -> None:
    key = s3.checkpoint_key(pipeline, asset_class)
    try:
        s3.put_json(key, {**payload, "updated_at": s3.now_utc()})
    except Exception as e:
        logger.warning("[checkpoint] write failed: %s", e)


# ── Read helpers ──────────────────────────────────────────────────────

def get_run_status(run_date: str, run_id: str) -> dict:
    try:
        return s3.read_json(s3.run_status_key(run_date, run_id))
    except Exception:
        return {}


def list_runs(run_date: str) -> list[dict]:
    """Return all run statuses for a given date."""
    prefix = f"control/runs/date={run_date}/"
    runs = []
    for key in s3.list_keys(prefix):
        if key.endswith("status.json"):
            try:
                runs.append(s3.read_json(key))
            except Exception:
                pass
    return sorted(runs, key=lambda r: r.get("run_id", ""))
