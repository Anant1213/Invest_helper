"""
Data Workbench — Upload, profile, explore and query any structured dataset.
Designed for data scientists, analysts, and beginners alike.
"""

from __future__ import annotations

import io
import json
import time

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from backend.ui import apply_styles, page_header, section_header, CHART_LAYOUT

st.set_page_config(
    page_title="Data Workbench — AssetEra",
    page_icon="🧠",
    layout="wide",
)
apply_styles()

# ── Extra CSS ──────────────────────────────────────────────────────────
st.markdown("""
<style>
.dw-kpi { display:flex; flex-wrap:wrap; gap:10px; margin-bottom:1rem; }
.dw-kpi-card {
  flex:1 1 140px; min-width:130px;
  background:var(--bg-card); border:1px solid var(--border);
  border-radius:var(--r-md); padding:.75rem 1rem;
}
.dw-kpi-label { font-size:.65rem; color:var(--text-2); text-transform:uppercase; letter-spacing:.07em; margin-bottom:.25rem; }
.dw-kpi-value { font-size:1.15rem; font-weight:700; font-family:var(--mono); color:var(--text); }

.issue-high    { border-left:4px solid var(--red);    padding:6px 10px; background:var(--red-dim);    border-radius:0 var(--r-sm) var(--r-sm) 0; margin:4px 0; font-size:.84rem; }
.issue-warning { border-left:4px solid var(--yellow);  padding:6px 10px; background:var(--yellow-dim);  border-radius:0 var(--r-sm) var(--r-sm) 0; margin:4px 0; font-size:.84rem; }
.issue-info    { border-left:4px solid var(--accent);  padding:6px 10px; background:var(--accent-dim); border-radius:0 var(--r-sm) var(--r-sm) 0; margin:4px 0; font-size:.84rem; }

.ai-badge {
  display:inline-block; font-size:.62rem; font-weight:700;
  color:var(--accent); border:1px solid var(--accent);
  border-radius:10px; padding:1px 7px; margin-left:6px;
  text-transform:uppercase; letter-spacing:.06em;
}

.sql-box {
  font-family:var(--mono); font-size:.8rem;
  background:var(--bg-overlay); color:var(--text-2);
  border:1px solid var(--border); border-radius:var(--r-md);
  padding:.7rem 1rem; margin:.5rem 0; overflow-x:auto;
  white-space:pre-wrap;
}
</style>
""", unsafe_allow_html=True)

page_header("Data Workbench", "Upload · Profile · Explore · Ask", badge="BETA")


# ── Helpers ────────────────────────────────────────────────────────────

def _kpi_card(label: str, value: str) -> str:
    return (
        f'<div class="dw-kpi-card">'
        f'<div class="dw-kpi-label">{label}</div>'
        f'<div class="dw-kpi-value">{value}</div>'
        f'</div>'
    )


def _issue_html(issue: dict) -> str:
    sev = issue.get("severity", "info").lower()
    msg = issue.get("message", "")
    return f'<div class="issue-{sev}">{msg}</div>'


def _chart_layout(**extra) -> dict:
    lyt = dict(CHART_LAYOUT)
    lyt.update(extra)
    return lyt


# ── Session state init ────────────────────────────────────────────────

for _k in ("project_id", "dataset_id", "profile", "quality", "views", "llm"):
    if _k not in st.session_state:
        st.session_state[_k] = None


# ── Sidebar: Project selector ─────────────────────────────────────────

st.sidebar.markdown("### Projects")

from backend.data_workbench.store import (
    create_project, list_projects, get_project,
    create_dataset, list_datasets, get_dataset, delete_dataset,
    get_profile, list_views, list_columns,
)

projects = list_projects()

with st.sidebar.expander("Create new project", expanded=(not projects)):
    proj_name = st.text_input("Project name", key="new_proj_name")
    proj_desc = st.text_area("Description (optional)", key="new_proj_desc", height=60)
    if st.button("Create", key="btn_create_proj"):
        if proj_name.strip():
            p = create_project(proj_name.strip(), proj_desc.strip())
            st.session_state["project_id"] = p["id"]
            st.rerun()

if projects:
    proj_names = [p["name"] for p in projects]
    proj_ids   = [p["id"]   for p in projects]
    default_idx = 0
    if st.session_state["project_id"] in proj_ids:
        default_idx = proj_ids.index(st.session_state["project_id"])
    sel_proj = st.sidebar.selectbox(
        "Active project", proj_names, index=default_idx, key="sel_proj"
    )
    st.session_state["project_id"] = proj_ids[proj_names.index(sel_proj)]
else:
    st.sidebar.info("Create a project to get started.")
    st.stop()

project_id = st.session_state["project_id"]

# Dataset selector
datasets = list_datasets(project_id)
st.sidebar.markdown("### Datasets")
if datasets:
    ds_names = [d["name"] for d in datasets]
    ds_ids   = [d["id"]   for d in datasets]
    default_ds = 0
    if st.session_state["dataset_id"] in ds_ids:
        default_ds = ds_ids.index(st.session_state["dataset_id"])
    sel_ds = st.sidebar.selectbox("Active dataset", ds_names, index=default_ds, key="sel_ds")
    st.session_state["dataset_id"] = ds_ids[ds_names.index(sel_ds)]
    if st.sidebar.button("Delete dataset", key="btn_del_ds"):
        delete_dataset(st.session_state["dataset_id"])
        st.session_state["dataset_id"] = None
        st.rerun()
else:
    st.sidebar.info("No datasets yet. Upload data in the Upload tab.")

dataset_id = st.session_state["dataset_id"]

st.sidebar.markdown("---")
st.sidebar.markdown(
    "<p style='font-size:.76rem;color:var(--text-2);line-height:1.6;'>"
    "Supported formats: CSV, XLSX, JSON, Parquet.<br>"
    "Max upload: 200 MB.<br>"
    "Metadata stored locally in SQLite.<br>"
    "Data stored in S3 (when configured)."
    "</p>",
    unsafe_allow_html=True,
)

# ── Main tabs ──────────────────────────────────────────────────────────

tab_upload, tab_overview, tab_quality, tab_views, tab_ask = st.tabs([
    "📤  Upload",
    "📊  Overview",
    "🛡️  Data Quality",
    "🎨  Auto Views",
    "💬  Ask Data",
])


# ══════════════════════════════════════════════════════════════════════
# TAB 1 — UPLOAD
# ══════════════════════════════════════════════════════════════════════
with tab_upload:
    section_header("Upload a Dataset")

    from backend.data_workbench.ingest import classify_file, parse_file, sha256_bytes, normalize_columns

    ds_name    = st.text_input("Dataset name", placeholder="e.g. Retail Transactions Q1 2024", key="ds_name")
    ctx_hint   = st.text_area(
        "Context (optional — helps the AI understand your data)",
        placeholder="e.g. Daily e-commerce orders from our B2C platform, Jan–Mar 2024",
        height=70, key="ctx_hint",
    )
    uploaded   = st.file_uploader(
        "Choose a file",
        type=["csv", "xlsx", "xls", "json", "parquet"],
        key="uploaded_file",
    )

    if uploaded:
        fsize_mb = uploaded.size / 1e6
        st.info(f"File: **{uploaded.name}** | {fsize_mb:.1f} MB | Type: {classify_file(uploaded.name)}")

        if st.button("Process & Profile", key="btn_process", type="primary"):
            if not ds_name.strip():
                st.error("Please enter a dataset name.")
            else:
                content = uploaded.read()
                sha     = sha256_bytes(content)

                # Create dataset record
                ds = create_dataset(project_id, ds_name.strip(), context_hint=ctx_hint.strip())
                ds_id = ds["id"]

                from backend.data_workbench.store import create_upload, update_dataset
                create_upload(ds_id, uploaded.name, len(content), sha)
                update_dataset(ds_id, file_count=1)

                progress = st.progress(0, text="Parsing file…")

                # Ingest
                from backend.data_workbench.ingest import ingest
                result = ingest(ds_id, uploaded.name, content)

                if not result["ok"]:
                    st.error(f"Parse error: {result.get('error')}")
                    delete_dataset(ds_id)
                else:
                    progress.progress(40, text="Profiling…")

                    # Profile
                    from backend.data_workbench.profile import build_profile
                    sample_df = result.get("sample_df", result["df"])
                    profile = build_profile(sample_df, ds_id, ctx_hint.strip())

                    progress.progress(65, text="Running quality checks…")

                    # Quality
                    from backend.data_workbench.quality import run_quality_checks
                    quality = run_quality_checks(profile)

                    progress.progress(75, text="Asking AI for insights…")

                    # LLM
                    from backend.data_workbench.llm import get_llm_summary
                    llm = get_llm_summary(profile, quality, ctx_hint.strip())

                    progress.progress(88, text="Generating auto views…")

                    # Views
                    from backend.data_workbench.views import generate_view_specs
                    from backend.data_workbench.store import save_profile, save_views
                    specs = generate_view_specs(profile, llm)
                    save_profile(ds_id, profile, quality, llm.get("summary", ""), json.dumps(llm))
                    save_views(ds_id, specs)
                    update_dataset(ds_id, status="ready")

                    progress.progress(100, text="Done!")

                    st.session_state["dataset_id"] = ds_id
                    st.session_state["profile"]    = profile
                    st.session_state["quality"]    = quality
                    st.session_state["llm"]        = llm
                    st.session_state["views"]      = specs

                    st.success(
                        f"Processed **{result['rows']:,}** rows × **{result['columns']}** columns. "
                        f"Quality score: **{quality['score']}/100**. "
                        "Switch to the Overview tab to explore."
                    )
                    time.sleep(0.8)
                    st.rerun()

    # Recent uploads in this project
    if datasets:
        st.markdown('<hr style="border-color:var(--border);margin:1.5rem 0;">', unsafe_allow_html=True)
        section_header("Recent Datasets")
        for ds in datasets[:8]:
            col1, col2, col3 = st.columns([3, 1, 1])
            col1.markdown(f"**{ds['name']}**  `{ds['status']}`")
            col2.markdown(f"{ds.get('row_count') or '—':,} rows")
            col3.markdown(f"{ds.get('column_count') or '—'} cols")


# ══════════════════════════════════════════════════════════════════════
# TAB 2 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════
with tab_overview:
    if not dataset_id:
        st.info("Upload or select a dataset from the sidebar.")
        st.stop()

    ds = get_dataset(dataset_id)
    if not ds:
        st.info("Dataset not found.")
        st.stop()

    # Load from DB if not in session
    if st.session_state["profile"] is None:
        p = get_profile(dataset_id)
        if p:
            st.session_state["profile"] = p["profile_json"]
            st.session_state["quality"] = p["quality_json"]
            try:
                st.session_state["llm"] = json.loads(p.get("llm_hints") or "{}")
            except Exception:
                st.session_state["llm"] = {}

    profile = st.session_state["profile"]
    quality = st.session_state["quality"]
    llm     = st.session_state["llm"] or {}

    if not profile:
        st.warning("No profile data found. Re-process the dataset from the Upload tab.")
        st.stop()

    # ── KPI cards ──────────────────────────────────────────────────────
    section_header(f"{ds['name']}  ·  Overview")

    q_score = (quality or {}).get("score", "—")
    q_color = "#00C896" if isinstance(q_score, int) and q_score >= 80 else (
              "#FFB020" if isinstance(q_score, int) and q_score >= 60 else "#FF3560")

    kpi_html = '<div class="dw-kpi">'
    kpi_html += _kpi_card("Rows",          f"{profile.get('row_count', '—'):,}")
    kpi_html += _kpi_card("Columns",       str(profile.get("column_count", "—")))
    kpi_html += _kpi_card("Memory",        f"{profile.get('memory_mb', '—')} MB")
    kpi_html += _kpi_card("Duplicate Rows",f"{profile.get('duplicate_pct', 0)*100:.1f}%")
    kpi_html += _kpi_card("Numeric Cols",  str(len(profile.get("numeric_columns", []))))
    kpi_html += _kpi_card("Date Cols",     str(len(profile.get("date_columns", []))))
    kpi_html += _kpi_card("Category Cols", str(len(profile.get("categorical_columns", []))))
    kpi_html += f'<div class="dw-kpi-card"><div class="dw-kpi-label">Quality Score</div><div class="dw-kpi-value" style="color:{q_color};">{q_score}/100</div></div>'
    kpi_html += '</div>'
    st.markdown(kpi_html, unsafe_allow_html=True)

    # ── AI Narrative ───────────────────────────────────────────────────
    if llm.get("summary"):
        st.markdown('<hr style="border-color:var(--border);margin:.8rem 0;">', unsafe_allow_html=True)
        section_header("AI Summary")
        st.markdown(
            f'<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:var(--r-lg);padding:1rem 1.2rem;font-size:.92rem;line-height:1.7;color:var(--text);">'
            f'{llm["summary"]}'
            f'</div>',
            unsafe_allow_html=True,
        )
        if llm.get("key_observations"):
            st.markdown("**Key Observations**")
            for obs in llm["key_observations"]:
                st.markdown(f"- {obs}")

    # ── Schema table ───────────────────────────────────────────────────
    st.markdown('<hr style="border-color:var(--border);margin:.8rem 0;">', unsafe_allow_html=True)
    section_header("Schema")

    cols_data = list_columns(dataset_id)
    if cols_data:
        schema_df = pd.DataFrame([{
            "Column":    c["column_name"],
            "Type":      c["inferred_type"],
            "Semantic":  c.get("semantic_label") or "—",
            "Null %":    f"{(c.get('null_pct') or 0)*100:.1f}%",
            "Distinct":  f"{c.get('distinct_count') or 0:,}",
            "Min":       c.get("min_value") or "—",
            "Max":       c.get("max_value") or "—",
            "Mean":      f"{c['mean_value']:.4g}" if c.get("mean_value") is not None else "—",
        } for c in cols_data])
        st.dataframe(schema_df, use_container_width=True, hide_index=True, height=min(500, 45 + len(schema_df)*35))

    # ── AI Suggested KPIs ─────────────────────────────────────────────
    if llm.get("suggested_kpis"):
        st.markdown('<hr style="border-color:var(--border);margin:.8rem 0;">', unsafe_allow_html=True)
        section_header("Suggested KPIs")
        for kpi in llm["suggested_kpis"][:6]:
            st.markdown(f"- **{kpi.get('name')}** — {kpi.get('how', '')}")

    # ── Next questions ─────────────────────────────────────────────────
    if llm.get("next_questions"):
        st.markdown('<hr style="border-color:var(--border);margin:.8rem 0;">', unsafe_allow_html=True)
        section_header("Suggested Next Questions")
        for q in llm["next_questions"][:5]:
            if st.button(f"→ {q}", key=f"nq_{hash(q)}"):
                st.session_state["prefill_question"] = q
                st.rerun()


# ══════════════════════════════════════════════════════════════════════
# TAB 3 — DATA QUALITY
# ══════════════════════════════════════════════════════════════════════
with tab_quality:
    if not dataset_id or not st.session_state.get("quality"):
        if dataset_id:
            p = get_profile(dataset_id)
            if p:
                st.session_state["quality"] = p["quality_json"]
        if not st.session_state.get("quality"):
            st.info("Upload or select a dataset from the sidebar.")
            st.stop()

    quality = st.session_state["quality"]
    profile = st.session_state["profile"] or {}

    section_header("Quality Report")

    score = quality.get("score", 0)
    summary = quality.get("summary", {})
    score_color = "#00C896" if score >= 80 else ("#FFB020" if score >= 60 else "#FF3560")

    sc1, sc2, sc3, sc4 = st.columns(4)
    sc1.markdown(
        f'<div class="dw-kpi-card"><div class="dw-kpi-label">Overall Score</div>'
        f'<div class="dw-kpi-value" style="color:{score_color};">{score}/100</div></div>',
        unsafe_allow_html=True,
    )
    sc2.metric("High Issues",    summary.get("high", 0))
    sc3.metric("Warnings",       summary.get("warning", 0))
    sc4.metric("Info",           summary.get("info", 0))

    # Issues list
    issues = quality.get("issues", [])
    if issues:
        st.markdown('<hr style="border-color:var(--border);margin:.8rem 0;">', unsafe_allow_html=True)
        section_header("Issues")
        for sev in ("high", "warning", "info"):
            grp = [i for i in issues if i.get("severity") == sev]
            if grp:
                label = {"high": "🔴 High", "warning": "🟡 Warning", "info": "🔵 Info"}[sev]
                st.markdown(f"**{label}** ({len(grp)})")
                html = "".join(_issue_html(i) for i in grp)
                st.markdown(html, unsafe_allow_html=True)
    else:
        st.success("No issues detected — this dataset looks clean!")

    # ── Missingness chart ──────────────────────────────────────────────
    col_profiles = profile.get("columns", [])
    null_data = [(c["column_name"], round(c.get("null_pct", 0) * 100, 2))
                 for c in col_profiles if c.get("null_pct", 0) > 0]

    if null_data:
        st.markdown('<hr style="border-color:var(--border);margin:.8rem 0;">', unsafe_allow_html=True)
        section_header("Missing Values by Column")
        ndf = pd.DataFrame(null_data, columns=["Column", "Missing %"]).sort_values("Missing %", ascending=False)
        fig = go.Figure(go.Bar(
            x=ndf["Missing %"], y=ndf["Column"],
            orientation="h",
            marker_color=[
                "#FF3560" if v > 50 else ("#FFB020" if v > 20 else "#2962FF")
                for v in ndf["Missing %"]
            ],
        ))
        layout_m = _chart_layout(height=max(200, len(ndf) * 28 + 80), title="Missing Values (%)")
        fig.update_layout(**layout_m)
        st.plotly_chart(fig, use_container_width=True)

    # Download quality JSON
    st.download_button(
        "Download Quality Report (JSON)",
        data=json.dumps(quality, indent=2).encode(),
        file_name="quality_report.json",
        mime="application/json",
        key="dl_quality",
    )


# ══════════════════════════════════════════════════════════════════════
# TAB 4 — AUTO VIEWS
# ══════════════════════════════════════════════════════════════════════
with tab_views:
    if not dataset_id:
        st.info("Upload or select a dataset from the sidebar.")
        st.stop()

    ds = get_dataset(dataset_id)
    curated_key = ds.get("curated_key") if ds else None
    if not curated_key:
        st.warning("No curated parquet found for this dataset. Re-process from the Upload tab.")
        st.stop()

    from backend.data_workbench.ingest import read_curated
    @st.cache_data(ttl=300, show_spinner=False)
    def _load_df(key: str) -> pd.DataFrame:
        return read_curated("", key)

    df = _load_df(curated_key)
    if df.empty:
        st.warning("Could not load the curated dataset.")
        st.stop()

    views = list_views(dataset_id)
    profile = st.session_state["profile"] or (get_profile(dataset_id) or {}).get("profile_json", {})

    if not views:
        st.info("No views generated yet. Re-process the dataset from the Upload tab.")
        st.stop()

    section_header("Auto-Generated Views")

    # Level filter
    level_filter = st.radio(
        "Level", ["All", "Basic", "Intermediate", "Advanced"],
        horizontal=True, key="view_level",
    )

    filtered_views = [
        v for v in views
        if level_filter == "All" or v.get("view_level", "").title() == level_filter
    ]

    for i, view in enumerate(filtered_views):
        spec = view.get("spec") or {}
        title = view.get("view_name", f"View {i+1}")
        chart_type = spec.get("chart_type", "bar")
        level = view.get("view_level", "basic")
        explanation = view.get("explanation", "")
        ai_badge = '<span class="ai-badge">AI</span>' if spec.get("ai_generated") else ""

        level_color = {"basic": "#00C896", "intermediate": "#FFB020", "advanced": "#FF3560"}.get(level, "#7A8BA0")
        st.markdown(
            f'<div style="border-left:3px solid {level_color};padding:4px 10px;margin:8px 0 4px;">'
            f'<b>{title}</b>{ai_badge}'
            f'<span style="font-size:.72rem;color:var(--text-2);margin-left:10px;">{level.upper()}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        if explanation:
            st.markdown(
                f'<p style="font-size:.82rem;color:var(--text-2);margin:0 0 6px 14px;">{explanation}</p>',
                unsafe_allow_html=True,
            )

        try:
            fig = _render_chart(spec, df, profile)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key=f"view_{i}")
        except Exception as e:
            st.caption(f"Could not render chart: {e}")

        st.markdown('<hr style="border-color:var(--border-dim);margin:4px 0;">', unsafe_allow_html=True)


def _render_chart(spec: dict, df: pd.DataFrame, profile: dict) -> go.Figure | None:
    chart_type = spec.get("chart_type", "bar")
    x = spec.get("x_col")
    y = spec.get("y_col")
    color_col = spec.get("color_col")
    title = spec.get("title", "")

    layout = _chart_layout(height=320, title=title)

    # ── KPI ──────────────────────────────────────────────────────────
    if chart_type == "kpi":
        return None  # handled by KPI cards in Overview tab

    # ── Missingness bar ────────────────────────────────────────────────
    if chart_type == "missingness":
        data = [(c["column_name"], round(c.get("null_pct", 0) * 100, 2))
                for c in profile.get("columns", []) if c.get("null_pct", 0) > 0]
        if not data:
            return None
        ndf = pd.DataFrame(data, columns=["col", "missing_pct"]).sort_values("missing_pct", ascending=False)
        fig = go.Figure(go.Bar(x=ndf["missing_pct"], y=ndf["col"], orientation="h",
                               marker_color="#2962FF", opacity=0.8))
        layout["height"] = max(200, len(ndf) * 26 + 60)
        fig.update_layout(**layout)
        return fig

    # ── Histogram ─────────────────────────────────────────────────────
    if chart_type == "histogram" and x and x in df.columns:
        s = pd.to_numeric(df[x], errors="coerce").dropna()
        fig = go.Figure(go.Histogram(x=s, nbinsx=40, marker_color="#2962FF", opacity=0.8))
        fig.update_layout(**layout)
        return fig

    # ── Bar ────────────────────────────────────────────────────────────
    if chart_type == "bar" and x and x in df.columns:
        if y == "count" or y not in df.columns:
            vc = df[x].value_counts().head(20).reset_index()
            vc.columns = [x, "count"]
            fig = px.bar(vc, x=x, y="count", color_discrete_sequence=["#2962FF"])
        else:
            agg = spec.get("aggregate", "mean")
            grp = df.groupby(x)[y].agg(agg).reset_index().rename(columns={y: f"{agg}_{y}"}).head(20)
            fig = px.bar(grp, x=x, y=f"{agg}_{y}", color_discrete_sequence=["#2962FF"])
        fig.update_layout(**layout)
        return fig

    # ── Line ───────────────────────────────────────────────────────────
    if chart_type in ("line", "rolling_line") and x and y and x in df.columns and y in df.columns:
        dfc = df[[x, y]].copy()
        try:
            dfc[x] = pd.to_datetime(dfc[x], errors="coerce")
            dfc = dfc.dropna().sort_values(x)
        except Exception:
            pass
        dfc[y] = pd.to_numeric(dfc[y], errors="coerce")

        if chart_type == "rolling_line":
            window = spec.get("window", 7)
            dfc[f"roll_{window}"] = dfc[y].rolling(window, min_periods=1).mean()
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=dfc[x], y=dfc[y], name=y,
                                     line=dict(color="#2962FF", width=1), opacity=0.4))
            fig.add_trace(go.Scatter(x=dfc[x], y=dfc[f"roll_{window}"], name=f"{window}-period MA",
                                     line=dict(color="#00C896", width=2)))
        else:
            fig = go.Figure(go.Scatter(x=dfc[x], y=dfc[y], name=y,
                                       line=dict(color="#2962FF", width=1.5)))
        fig.update_layout(**layout)
        return fig

    # ── Heatmap ────────────────────────────────────────────────────────
    if chart_type == "heatmap":
        cols = spec.get("columns", profile.get("numeric_columns", []))[:12]
        cols = [c for c in cols if c in df.columns]
        if len(cols) < 2:
            return None
        corr = df[cols].apply(pd.to_numeric, errors="coerce").corr()
        fig = go.Figure(go.Heatmap(
            z=corr.values, x=corr.columns, y=corr.index,
            colorscale="RdBu", zmid=0,
            text=corr.round(2).values,
            texttemplate="%{text}", textfont=dict(size=9),
        ))
        layout["height"] = max(350, len(cols) * 35 + 80)
        fig.update_layout(**layout)
        return fig

    # ── Scatter ────────────────────────────────────────────────────────
    if chart_type == "scatter" and x and y and x in df.columns and y in df.columns:
        dfc = df.sample(min(2000, len(df)), random_state=42)
        kw = dict(x=x, y=y)
        if color_col and color_col in dfc.columns:
            kw["color"] = color_col
        fig = px.scatter(dfc, **kw, opacity=0.65, color_discrete_sequence=["#2962FF"])
        fig.update_layout(**layout)
        return fig

    return None


# ══════════════════════════════════════════════════════════════════════
# TAB 5 — ASK DATA
# ══════════════════════════════════════════════════════════════════════
with tab_ask:
    if not dataset_id:
        st.info("Upload or select a dataset from the sidebar.")
        st.stop()

    ds = get_dataset(dataset_id)
    curated_key = ds.get("curated_key") if ds else None
    if not curated_key:
        st.warning("No curated parquet found. Re-process from the Upload tab.")
        st.stop()

    from backend.data_workbench.queries import ask_dataset, run_sql

    section_header("Ask a Question About Your Data")

    # Pre-fill from "suggested questions" clicks
    prefill = st.session_state.pop("prefill_question", "")

    example_qs = [
        "What are the top 10 rows by the largest numeric column?",
        "How many distinct values does each categorical column have?",
        "Show average values grouped by the first categorical column.",
        "What is the overall count of records?",
    ]

    st.markdown(
        "<p style='font-size:.83rem;color:var(--text-2);margin-bottom:.5rem;'>Examples:</p>",
        unsafe_allow_html=True,
    )
    cols_eq = st.columns(len(example_qs))
    for i, eq in enumerate(example_qs):
        if cols_eq[i].button(eq, key=f"eq_{i}"):
            prefill = eq

    question = st.text_area(
        "Your question",
        value=prefill,
        placeholder="e.g. Which product category had the highest average order value?",
        height=90,
        key="ask_question",
    )

    mode = st.radio("Mode", ["Natural Language (AI)", "Direct SQL"], horizontal=True, key="ask_mode")

    if mode == "Direct SQL":
        direct_sql = st.text_area(
            "SQL (use FROM dataset)",
            height=100,
            placeholder="SELECT category, AVG(amount) FROM dataset GROUP BY category ORDER BY 2 DESC LIMIT 10",
            key="direct_sql",
        )

    run_query = st.button("Run", key="btn_run_query", type="primary")

    if run_query:
        with st.spinner("Running query…"):
            if mode == "Direct SQL":
                result = run_sql(direct_sql, curated_key)
                result["sql_generated"] = direct_sql
                result["explanation"] = ""
            else:
                if not question.strip():
                    st.warning("Enter a question first.")
                    st.stop()
                result = ask_dataset(dataset_id, question.strip())

        if not result.get("ok"):
            st.error(f"Query error: {result.get('error')}")
        else:
            # SQL used
            sql_disp = result.get("sql_executed") or result.get("sql_generated", "")
            if sql_disp:
                with st.expander("SQL executed", expanded=True):
                    st.markdown(f'<div class="sql-box">{sql_disp}</div>', unsafe_allow_html=True)

            # Explanation
            if result.get("explanation"):
                st.markdown(
                    f'<div style="background:var(--bg-card);border:1px solid var(--border);'
                    f'border-radius:var(--r-md);padding:.75rem 1rem;font-size:.88rem;margin:.6rem 0;">'
                    f'{result["explanation"]}</div>',
                    unsafe_allow_html=True,
                )

            df_result = result.get("df", pd.DataFrame())
            if not df_result.empty:
                st.markdown(
                    f"<p style='font-size:.8rem;color:var(--text-2);'>{result['row_count']:,} rows returned</p>",
                    unsafe_allow_html=True,
                )
                st.dataframe(df_result, use_container_width=True, hide_index=True)

                # Auto-chart result
                num_cols = df_result.select_dtypes(include="number").columns.tolist()
                cat_cols = df_result.select_dtypes(include="object").columns.tolist()

                if cat_cols and num_cols and len(df_result) <= 50:
                    fig_r = px.bar(
                        df_result.head(20),
                        x=cat_cols[0], y=num_cols[0],
                        color_discrete_sequence=["#2962FF"],
                    )
                    fig_r.update_layout(**_chart_layout(height=300, title="Query Result Chart"))
                    st.plotly_chart(fig_r, use_container_width=True)
                elif num_cols and not cat_cols and len(df_result) > 10:
                    fig_r = go.Figure(go.Histogram(
                        x=df_result[num_cols[0]].dropna(),
                        nbinsx=30, marker_color="#2962FF", opacity=0.8,
                    ))
                    fig_r.update_layout(**_chart_layout(height=280, title="Result Distribution"))
                    st.plotly_chart(fig_r, use_container_width=True)

                # Download
                st.download_button(
                    "Download result CSV",
                    data=df_result.to_csv(index=False).encode(),
                    file_name="query_result.csv",
                    mime="text/csv",
                    key="dl_result",
                )
            else:
                st.info("Query returned no rows.")
