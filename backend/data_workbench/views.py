"""
backend.data_workbench.views
─────────────────────
Rule-based auto chart spec generation.

Priority ladder (lower = higher priority):
  Basic (10–39)       — always attempt: KPI cards, distributions, top-N
  Intermediate (40–69) — requires 2+ numeric or date+metric
  Advanced (70–99)     — correlations, scatter, heatmap

Each spec is a plain dict that pages/6_Data_Workbench.py knows how to render.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def generate_view_specs(
    profile: dict,
    llm_hints: dict | None = None,
) -> list[dict]:
    """
    Build a list of view spec dicts from the profile + optional LLM hints.
    Returns specs sorted by priority.
    """
    specs: list[dict] = []

    numeric  = profile.get("numeric_columns",      [])
    dates    = profile.get("date_columns",         [])
    cats     = profile.get("categorical_columns",  [])
    n_rows   = profile.get("row_count",            0)
    col_map  = {c["column_name"]: c for c in profile.get("columns", [])}

    # ── Basic: Dataset overview KPIs ──────────────────────────────────
    specs.append({
        "title":       "Dataset Overview",
        "level":       "basic",
        "chart_type":  "kpi",
        "priority":    10,
        "x_col":       None,
        "y_col":       None,
        "color_col":   None,
        "filters":     [],
        "explanation": "Row count, column count, memory, and quality score at a glance.",
    })

    # ── Basic: Missing data heatmap ───────────────────────────────────
    null_cols = [c for c in profile.get("columns", []) if c.get("null_pct", 0) > 0]
    if null_cols:
        specs.append({
            "title":       "Missing Data Overview",
            "level":       "basic",
            "chart_type":  "missingness",
            "priority":    11,
            "x_col":       None,
            "y_col":       None,
            "color_col":   None,
            "filters":     [],
            "explanation": "Columns and their missing-value rates.",
        })

    # ── Basic: Numeric distributions (top 4) ─────────────────────────
    for i, col in enumerate(numeric[:4]):
        specs.append({
            "title":       f"Distribution: {col}",
            "level":       "basic",
            "chart_type":  "histogram",
            "priority":    20 + i,
            "x_col":       col,
            "y_col":       None,
            "color_col":   None,
            "filters":     [],
            "explanation": f"Value distribution of '{col}'.",
        })

    # ── Basic: Top-N categorical ──────────────────────────────────────
    for i, col in enumerate(cats[:3]):
        specs.append({
            "title":       f"Top Values: {col}",
            "level":       "basic",
            "chart_type":  "bar",
            "priority":    30 + i,
            "x_col":       col,
            "y_col":       "count",
            "color_col":   None,
            "filters":     [],
            "explanation": f"Most frequent values in '{col}'.",
        })

    # ── Intermediate: Time trends ─────────────────────────────────────
    if dates and numeric:
        for i, met in enumerate(numeric[:3]):
            specs.append({
                "title":       f"{met} over time",
                "level":       "intermediate",
                "chart_type":  "line",
                "priority":    40 + i,
                "x_col":       dates[0],
                "y_col":       met,
                "color_col":   None,
                "filters":     [],
                "explanation": f"Trend of '{met}' indexed by '{dates[0]}'.",
            })

    # ── Intermediate: Category breakdown ─────────────────────────────
    if cats and numeric:
        for i, (cat, met) in enumerate(zip(cats[:2], numeric[:2])):
            specs.append({
                "title":       f"{met} by {cat}",
                "level":       "intermediate",
                "chart_type":  "bar",
                "priority":    50 + i,
                "x_col":       cat,
                "y_col":       met,
                "color_col":   None,
                "aggregate":   "mean",
                "filters":     [],
                "explanation": f"Average '{met}' broken down by '{cat}'.",
            })

    # ── Intermediate: Correlation heatmap ─────────────────────────────
    if len(numeric) >= 3:
        specs.append({
            "title":       "Correlation Heatmap",
            "level":       "intermediate",
            "chart_type":  "heatmap",
            "priority":    60,
            "x_col":       None,
            "y_col":       None,
            "color_col":   None,
            "columns":     numeric[:12],
            "filters":     [],
            "explanation": "Pairwise Pearson correlations among numeric columns.",
        })

    # ── Advanced: Scatter plots (top correlated pairs) ────────────────
    corr_pairs = profile.get("correlations", [])
    for i, pair in enumerate(corr_pairs[:2]):
        specs.append({
            "title":       f"{pair['col1']} vs {pair['col2']}",
            "level":       "advanced",
            "chart_type":  "scatter",
            "priority":    70 + i,
            "x_col":       pair["col1"],
            "y_col":       pair["col2"],
            "color_col":   cats[0] if cats else None,
            "filters":     [],
            "explanation": f"Scatter of most correlated pair (|r|={pair['abs_corr']}).",
        })

    # ── Advanced: Rolling trend (if date + numeric) ───────────────────
    if dates and numeric:
        specs.append({
            "title":       f"Rolling Average: {numeric[0]}",
            "level":       "advanced",
            "chart_type":  "rolling_line",
            "priority":    80,
            "x_col":       dates[0],
            "y_col":       numeric[0],
            "color_col":   None,
            "window":      7,
            "filters":     [],
            "explanation": f"7-period rolling mean of '{numeric[0]}'.",
        })

    # ── Merge LLM suggested views (if any) ───────────────────────────
    if llm_hints and "suggested_views" in llm_hints:
        for i, sv in enumerate(llm_hints["suggested_views"]):
            specs.append({
                "title":       sv.get("title", f"AI View {i+1}"),
                "level":       "intermediate",
                "chart_type":  sv.get("chart_type", "bar"),
                "priority":    55 + i,
                "x_col":       sv.get("x_col"),
                "y_col":       sv.get("y_col"),
                "color_col":   sv.get("color_col"),
                "filters":     [],
                "explanation": sv.get("explanation", ""),
                "ai_generated": True,
            })

    specs.sort(key=lambda s: s["priority"])
    return specs
