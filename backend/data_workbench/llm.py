"""
backend.data_workbench.llm
───────────────────
OpenAI-powered dataset narrative, KPI suggestions, and view hints.

Design rules:
  - Never send raw row data to the LLM — only aggregate stats.
  - PII columns are masked before any sample is included.
  - LLM output is structured JSON so downstream code can parse it safely.
  - Falls back gracefully when OPENAI_API_KEY is not set.
"""

from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger(__name__)

_SYSTEM = """You are a senior data analyst assistant. You receive a compact statistical
profile of a dataset and return a structured JSON response.
Be concise, practical, and honest about uncertainty.
Never invent data that was not provided."""

_SCHEMA = """\
{
  "summary": "2-3 sentence plain-English overview of what this dataset represents",
  "key_observations": ["observation 1", "observation 2", "..."],
  "suggested_kpis": [{"name": "KPI name", "column": "col_name", "how": "brief formula or method"}],
  "suggested_views": [
    {
      "title": "chart title",
      "chart_type": "line|bar|histogram|scatter|heatmap|table",
      "x_col": "column name or null",
      "y_col": "column name or null",
      "color_col": "column name or null",
      "explanation": "one sentence on what to look for"
    }
  ],
  "next_questions": ["question 1", "question 2", "question 3"],
  "warnings": ["any data concerns not already in quality report"]
}"""


def _build_prompt(
    profile: dict,
    quality: dict,
    context_hint: str = "",
    max_top_values: int = 5,
) -> str:
    n_rows   = profile.get("row_count", "?")
    n_cols   = profile.get("column_count", "?")
    dup_pct  = round(profile.get("duplicate_pct", 0) * 100, 1)
    mem_mb   = profile.get("memory_mb", 0)
    score    = quality.get("score", "?")

    col_lines: list[str] = []
    pii_cols = set()
    for col in profile.get("columns", []):
        if col.get("is_pii"):
            pii_cols.add(col["column_name"])

    for col in profile.get("columns", []):
        name  = col["column_name"]
        dtype = col["inferred_type"]
        sem   = col.get("semantic_label", "")
        null_pct = round(col.get("null_pct", 0) * 100, 1)

        line = f"  {name} [{dtype}"
        if sem:
            line += f"/{sem}"
        line += f"] null={null_pct}%"

        if name in pii_cols:
            line += " [PII—masked]"
        elif dtype == "numeric":
            mn = col.get("min_value")
            mx = col.get("max_value")
            mean = col.get("mean_value")
            std  = col.get("std_value")
            if mn is not None:
                line += f" min={mn:.2g} max={mx:.2g} mean={mean:.2g} std={std:.2g}"
        elif dtype == "categorical":
            top = col.get("top_values", {})
            top_str = ", ".join(f"{k}({v})" for k, v in list(top.items())[:max_top_values])
            line += f" top=[{top_str}]"
        elif dtype == "datetime":
            line += f" range=[{col.get('min_value')} → {col.get('max_value')}]"

        col_lines.append(line)

    issues_lines = [
        f"  [{i['severity'].upper()}] {i['message']}"
        for i in quality.get("issues", [])[:8]
    ]

    parts = [
        f"DATASET PROFILE",
        f"Rows: {n_rows:,} | Columns: {n_cols} | Memory: {mem_mb} MB | Quality score: {score}/100",
        f"Duplicate rows: {dup_pct}%",
    ]
    if context_hint:
        parts.append(f"User context: {context_hint}")
    parts.append("\nCOLUMNS:")
    parts.extend(col_lines)
    if issues_lines:
        parts.append("\nQUALITY ISSUES (top 8):")
        parts.extend(issues_lines)

    top_corr = profile.get("correlations", [])[:5]
    if top_corr:
        parts.append("\nTOP CORRELATIONS:")
        for c in top_corr:
            parts.append(f"  {c['col1']} ↔ {c['col2']}: r={c['abs_corr']}")

    parts.append(f"\nReturn ONLY the JSON object matching this schema:\n{_SCHEMA}")
    return "\n".join(parts)


def get_llm_summary(
    profile: dict,
    quality: dict,
    context_hint: str = "",
) -> dict:
    """
    Call OpenAI and return the parsed JSON response dict.
    Returns a minimal fallback dict if the API call fails.
    """
    if not os.getenv("OPENAI_API_KEY"):
        return _fallback(profile)

    from backend.data_workbench.config import cfg
    model = cfg().openai_model

    prompt = _build_prompt(profile, quality, context_hint)

    try:
        from openai import OpenAI
        client = OpenAI()
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": _SYSTEM},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.3,
            max_tokens=cfg().llm_max_tokens,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content or "{}"
        return json.loads(raw)
    except Exception as e:
        logger.warning("LLM call failed: %s", e)
        return _fallback(profile)


def _fallback(profile: dict) -> dict:
    """Minimal deterministic fallback when LLM is unavailable."""
    numeric  = profile.get("numeric_columns",  [])
    date_cols = profile.get("date_columns",    [])
    cat_cols  = profile.get("categorical_columns", [])

    kpis = []
    for col in numeric[:3]:
        kpis.append({"name": f"Avg {col}", "column": col, "how": f"mean({col})"})

    views = []
    if date_cols and numeric:
        views.append({
            "title": f"{numeric[0]} over time",
            "chart_type": "line",
            "x_col": date_cols[0],
            "y_col": numeric[0],
            "color_col": None,
            "explanation": "Trend of the primary numeric column over time.",
        })
    if numeric:
        views.append({
            "title": f"Distribution of {numeric[0]}",
            "chart_type": "histogram",
            "x_col": numeric[0],
            "y_col": None,
            "color_col": None,
            "explanation": "Value distribution of the primary numeric column.",
        })
    if cat_cols and numeric:
        views.append({
            "title": f"{numeric[0]} by {cat_cols[0]}",
            "chart_type": "bar",
            "x_col": cat_cols[0],
            "y_col": numeric[0],
            "color_col": None,
            "explanation": "Breakdown by category.",
        })

    return {
        "summary": (
            f"Dataset with {profile.get('row_count', '?')} rows and "
            f"{profile.get('column_count', '?')} columns. "
            "LLM summary unavailable — set OPENAI_API_KEY for AI-powered insights."
        ),
        "key_observations": [],
        "suggested_kpis":   kpis,
        "suggested_views":  views,
        "next_questions":   [],
        "warnings":         [],
    }
