"""
backend.data_workbench.quality
───────────────────────
Rule-based data quality checks.
Runs against the profile_json dict (not the raw DataFrame directly)
so it can operate even after the DataFrame is freed.
"""

from __future__ import annotations

from typing import Any


def run_quality_checks(profile: dict, df_head: "pd.DataFrame | None" = None) -> dict:
    """
    Return a quality_json dict:
    {
      "score": 0–100,
      "issues": [{"rule": str, "severity": "info"|"warning"|"high", "message": str, "columns": [...]}, ...]
    }
    """
    issues: list[dict] = []
    n_rows = profile.get("row_count", 0) or 1

    # ── Rule 1: High null rate ───────────────────────────────────────
    for col in profile.get("columns", []):
        null_pct = col.get("null_pct", 0)
        if null_pct > 0.5:
            issues.append({
                "rule": "high_null_rate",
                "severity": "high",
                "message": f"'{col['column_name']}' has {null_pct*100:.1f}% missing values.",
                "columns": [col["column_name"]],
            })
        elif null_pct > 0.2:
            issues.append({
                "rule": "moderate_null_rate",
                "severity": "warning",
                "message": f"'{col['column_name']}' has {null_pct*100:.1f}% missing values.",
                "columns": [col["column_name"]],
            })
        elif null_pct > 0:
            issues.append({
                "rule": "low_null_rate",
                "severity": "info",
                "message": f"'{col['column_name']}' has {null_pct*100:.1f}% missing values.",
                "columns": [col["column_name"]],
            })

    # ── Rule 2: Duplicate rows ───────────────────────────────────────
    dup_pct = profile.get("duplicate_pct", 0)
    if dup_pct > 0.2:
        issues.append({
            "rule": "high_duplicate_rows",
            "severity": "high",
            "message": f"{dup_pct*100:.1f}% of rows are exact duplicates.",
            "columns": [],
        })
    elif dup_pct > 0.05:
        issues.append({
            "rule": "moderate_duplicate_rows",
            "severity": "warning",
            "message": f"{dup_pct*100:.1f}% of rows are exact duplicates.",
            "columns": [],
        })

    # ── Rule 3: ID-like columns (non-analytic) ───────────────────────
    for col in profile.get("columns", []):
        if col.get("unique_pct", 0) > 0.95 and col.get("inferred_type") in ("categorical", "text"):
            issues.append({
                "rule": "id_like_column",
                "severity": "info",
                "message": f"'{col['column_name']}' has >95% unique values — likely an ID column, not analytic.",
                "columns": [col["column_name"]],
            })

    # ── Rule 4: Extreme skewness ─────────────────────────────────────
    for col in profile.get("columns", []):
        if col.get("inferred_type") != "numeric":
            continue
        skew = abs(col.get("skewness", 0) or 0)
        if skew > 10:
            issues.append({
                "rule": "extreme_skewness",
                "severity": "warning",
                "message": f"'{col['column_name']}' is highly skewed (skew={skew:.1f}). Consider log-transform.",
                "columns": [col["column_name"]],
            })

    # ── Rule 5: Highly correlated pairs ─────────────────────────────
    for pair in profile.get("correlations", []):
        if pair["abs_corr"] > 0.95:
            issues.append({
                "rule": "high_correlation",
                "severity": "info",
                "message": (
                    f"'{pair['col1']}' and '{pair['col2']}' are highly correlated "
                    f"(|r|={pair['abs_corr']:.2f}). One may be redundant."
                ),
                "columns": [pair["col1"], pair["col2"]],
            })

    # ── Rule 6: PII detected ─────────────────────────────────────────
    for col in profile.get("columns", []):
        if col.get("is_pii"):
            issues.append({
                "rule": "pii_detected",
                "severity": "warning",
                "message": f"'{col['column_name']}' may contain PII ({col.get('semantic_label','?')}). Handle with care.",
                "columns": [col["column_name"]],
            })

    # ── Rule 7: Single-value columns ────────────────────────────────
    for col in profile.get("columns", []):
        if col.get("distinct_count", 99) == 1:
            issues.append({
                "rule": "constant_column",
                "severity": "info",
                "message": f"'{col['column_name']}' has only 1 distinct value — constant column.",
                "columns": [col["column_name"]],
            })

    # ── Score ────────────────────────────────────────────────────────
    high_count    = sum(1 for i in issues if i["severity"] == "high")
    warning_count = sum(1 for i in issues if i["severity"] == "warning")
    score = max(0, 100 - high_count * 20 - warning_count * 5)

    return {
        "score":    score,
        "issues":   issues,
        "summary": {
            "high":    high_count,
            "warning": warning_count,
            "info":    len(issues) - high_count - warning_count,
        },
    }
