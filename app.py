
from __future__ import annotations

import os
import time
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src import config as C
from src.etl import etl_run, load_processed
from src.metrics import Filters, apply_filters, compute_kpis, daily_series, dry_period_stats, top_n


# ------------------------- Streamlit config -------------------------
st.set_page_config(
    page_title="Dubai Real Estate Transactions | Executive Dashboard",
    page_icon="🏙️",
    layout="wide",
)

# Basic CSS: white/light-blue vibe, clean executive look
st.markdown(
    """
    <style>
      .block-container { padding-top: 1.3rem; }
      [data-testid="stMetricValue"] { font-size: 1.35rem; }
      .sobha-accent { color: #1e88e5; font-weight: 700; }
      .small-note { color: #6b7280; font-size: 0.9rem; }
      .card { padding: 1rem; border: 1px solid #e5e7eb; border-radius: 14px; background: white; }
    </style>
    """,
    unsafe_allow_html=True
)

# ------------------------- Paths -------------------------
ROOT = Path(__file__).parent
RAW_XLSX = ROOT / "data" / "raw" / "transactions.xlsx"

# Pick best processed format
PROCESSED_PARQUET = ROOT / "data" / "processed" / "transactions_clean.parquet"
PROCESSED_PKL = ROOT / "data" / "processed" / "transactions_clean.pkl"
PROCESSED_DEFAULT = PROCESSED_PARQUET if PROCESSED_PARQUET.exists() else PROCESSED_PKL

ALIAS_CSV = ROOT / "config" / "developer_aliases.csv"


def _processed_path_preference() -> Path:
    # If pyarrow exists, prefer parquet, else pickle.
    try:
        import pyarrow  # noqa: F401
        return PROCESSED_PARQUET
    except Exception:
        return PROCESSED_PKL


@st.cache_data(show_spinner=False)
def load_data(force_rebuild: bool = False) -> Tuple[pd.DataFrame, Dict]:
    """
    Loads cleaned data. Rebuilds if:
      - forced
      - processed file missing
      - raw file is newer than processed
    """
    processed_path = _processed_path_preference()
    processed_exists = processed_path.exists()

    raw_mtime = RAW_XLSX.stat().st_mtime if RAW_XLSX.exists() else 0
    processed_mtime = processed_path.stat().st_mtime if processed_exists else 0

    needs_build = force_rebuild or (not processed_exists) or (raw_mtime > processed_mtime)

    audit = {}
    if needs_build:
        audit = etl_run(
            raw_xlsx_path=str(RAW_XLSX),
            out_path=str(processed_path),
            developer_alias_csv=str(ALIAS_CSV) if ALIAS_CSV.exists() else None,
        )
    df = load_processed(str(processed_path))
    return df, audit


def fmt_aed(x: float | int | None) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"{x:,.0f}"


def fmt_float(x: float | int | None, digits: int = 2) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"{x:,.{digits}f}"


def scenario_filter_widgets(prefix: str, df: pd.DataFrame) -> Filters:
    """
    Renders a full filter set, returns Filters object.
    prefix makes widget keys unique for scenario comparisons.
    """
    latest_date = pd.to_datetime(df[C.COL_DATE]).max().date()
    earliest_date = pd.to_datetime(df[C.COL_DATE]).min().date()

    # Date quick presets
    preset = st.radio(
        "Date preset",
        ["Last 7 days", "Last 30 days", "Last 90 days", "Custom"],
        horizontal=True,
        key=f"{prefix}_date_preset",
    )

    if preset == "Last 7 days":
        dmin = latest_date - pd.Timedelta(days=6)
        dmax = latest_date
    elif preset == "Last 30 days":
        dmin = latest_date - pd.Timedelta(days=29)
        dmax = latest_date
    elif preset == "Last 90 days":
        dmin = latest_date - pd.Timedelta(days=89)
        dmax = latest_date
    else:
        dmin, dmax = st.date_input(
            "Date range",
            value=(earliest_date, latest_date),
            min_value=earliest_date,
            max_value=latest_date,
            key=f"{prefix}_date_range",
        )

    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        tx_bucket = st.selectbox(
            "Transaction Type",
            options=C.TX_BUCKET_FILTER_OPTIONS,
            index=0,
            key=f"{prefix}_tx_bucket",
        )

    with col2:
        prop_type = st.selectbox(
            "Type",
            options=C.PROPERTY_TYPE_FILTER_OPTIONS,
            index=0,
            key=f"{prefix}_prop_type",
        )

    with col3:
        include_outliers = st.toggle("Include outliers", value=False, key=f"{prefix}_outliers")

    # Bedrooms
    bedrooms = st.multiselect(
        "Bedrooms",
        options=C.ALLOWED_BEDROOMS,
        default=C.ALLOWED_BEDROOMS,
        key=f"{prefix}_bedrooms",
    )

    # Range sliders: compute global min/max for nicer UX
    s_min, s_max = float(df[C.COL_SIZE_SQF].min()), float(df[C.COL_SIZE_SQF].max())
    a_min, a_max = float(df[C.COL_AMOUNT_AED].min()), float(df[C.COL_AMOUNT_AED].max())
    p_min, p_max = float(df[C.COL_AED_PSF].min()), float(df[C.COL_AED_PSF].max())

    c1, c2, c3 = st.columns(3)
    with c1:
        size_range = st.slider(
            "Size (Sqf)",
            min_value=float(s_min),
            max_value=float(s_max),
            value=(float(s_min), float(s_max)),
            step=1.0,
            key=f"{prefix}_size_range",
        )
    with c2:
        amount_range = st.slider(
            "Amount (AED)",
            min_value=float(a_min),
            max_value=float(a_max),
            value=(float(a_min), float(a_max)),
            step=float(max(1000.0, (a_max - a_min) / 2000.0)),
            key=f"{prefix}_amount_range",
        )
    with c3:
        psf_range = st.slider(
            "AED/Sqf",
            min_value=float(p_min),
            max_value=float(p_max),
            value=(float(p_min), float(p_max)),
            step=float(max(1.0, (p_max - p_min) / 2000.0)),
            key=f"{prefix}_psf_range",
        )

    # Multiselects for Community / Developer / Property (top frequent first)
    top_communities = df[C.COL_COMMUNITY].value_counts().head(500).index.tolist()
    top_devs = df["Developer Group"].value_counts().head(500).index.tolist()
    top_props = df[C.COL_PROPERTY].value_counts().head(800).index.tolist()

    c4, c5, c6 = st.columns(3)
    with c4:
        community = st.multiselect(
            "Community (optional)",
            options=top_communities,
            default=[],
            key=f"{prefix}_community",
        )
    with c5:
        developer = st.multiselect(
            "Developer (optional)",
            options=top_devs,
            default=[],
            key=f"{prefix}_developer",
        )
    with c6:
        prop = st.multiselect(
            "Property / Project (optional)",
            options=top_props,
            default=[],
            key=f"{prefix}_property",
        )

    return Filters(
        date_min=pd.to_datetime(dmin),
        date_max=pd.to_datetime(dmax),
        transaction_bucket=tx_bucket,
        property_type=prop_type,
        bedrooms=bedrooms,
        community=community or None,
        developer_group=developer or None,
        prop=prop or None,
        size_range=size_range,
        amount_range=amount_range,
        psf_range=psf_range,
        include_outliers=include_outliers,
    )


# ------------------------- Header -------------------------
st.title("Dubai Real Estate Transactions Dashboard")
st.caption("Clean, filter, compare and track momentum across communities, developers and projects.")

# Data load + rebuild
with st.sidebar:
    st.subheader("Data")
    rebuild = st.button("🔄 Rebuild cleaned dataset now")
    df, audit = load_data(force_rebuild=rebuild)
    latest_date = pd.to_datetime(df[C.COL_DATE]).max().date()
    st.markdown(f"**Latest transaction date:** <span class='sobha-accent'>{latest_date}</span>", unsafe_allow_html=True)
    st.markdown(f"<span class='small-note'>Rows (cleaned): {len(df):,}</span>", unsafe_allow_html=True)
    if audit:
        with st.expander("ETL audit log"):
            st.json(audit)

# ------------------------- Compare controls -------------------------
st.sidebar.subheader("Compare scenarios")
enable_compare = st.sidebar.toggle("Compare filter sets", value=False)
if enable_compare:
    num_scenarios = st.sidebar.slider("Number of scenarios", min_value=2, max_value=4, value=2)
else:
    num_scenarios = 1

dry_x = st.sidebar.select_slider(
    "Dry-day threshold (x% of peak day)",
    options=[0, 2, 5, 10, 20],
    value=5,
)
peak_def = st.sidebar.selectbox(
    "Peak definition",
    options=["Max day (100%)", "80th percentile (robust)"],
    index=0,
)
peak_quantile = 1.0 if peak_def.startswith("Max") else 0.8

dry_basis = st.sidebar.selectbox(
    "Dry-day basis",
    options=["Transactions (count)", "Sales amount (AED)"],
    index=0,
)
dry_basis_key = "count" if dry_basis.startswith("Transactions") else "amount"

# ------------------------- Filters UI -------------------------
scenarios: List[Dict] = []
for i in range(num_scenarios):
    with st.expander(f"Scenario {i+1} filters", expanded=(i == 0)):
        f = scenario_filter_widgets(prefix=f"s{i+1}", df=df)
    scenarios.append({"name": f"Scenario {i+1}", "filters": f})

# ------------------------- Compute scenario outputs -------------------------
scenario_results = []
for s in scenarios:
    f: Filters = s["filters"]
    dff = apply_filters(df, f)
    kpis = compute_kpis(dff)
    daily_cnt = daily_series(dff, basis="count")
    daily_amt = daily_series(dff, basis="amount")
    daily_for_dry = daily_series(dff, basis=dry_basis_key)
    dry_stats = dry_period_stats(daily_for_dry, x_percent=float(dry_x), peak_quantile=float(peak_quantile))

    scenario_results.append(
        {
            "name": s["name"],
            "df": dff,
            "kpis": kpis,
            "daily_cnt": daily_cnt,
            "daily_amt": daily_amt,
            "dry": dry_stats,
        }
    )

# ------------------------- KPI Row (Scenario 1 is primary) -------------------------
primary = scenario_results[0]
k = primary["kpis"]
d = primary["dry"]

kpi_cols = st.columns(6)
kpi_cols[0].metric("Transactions", f"{k['Transactions']:,}")
kpi_cols[1].metric("Sales velocity", f"{k['Sales Velocity (txn/day)']:.2f} txn/day")
kpi_cols[2].metric("Total sales", f"AED {fmt_aed(k['Total Sales Amount (AED)'])}")
kpi_cols[3].metric("Avg selling price", f"AED {fmt_aed(k['Avg Selling Price (AED)'])}")
kpi_cols[4].metric("Median PSF", f"{fmt_aed(k['Median PSF (AED/Sqf)'])}")
kpi_cols[5].metric("Longest dry streak", f"{d['longest_dry_streak_days']} days")


# Scenario KPI comparison (shows all requested KPI fields per scenario)
if enable_compare:
    st.markdown("### Scenario comparison")
    cmp_rows = []
    for s in scenario_results:
        kp = s["kpis"]
        dry = s["dry"]
        cmp_rows.append({
            "Scenario": s["name"],
            "Transactions": kp["Transactions"],
            "Sales Velocity (txn/day)": round(kp["Sales Velocity (txn/day)"], 3) if kp["Sales Velocity (txn/day)"] is not None else None,
            "Total Sales Amount (AED)": kp["Total Sales Amount (AED)"],
            "Avg Selling Price (AED)": kp["Avg Selling Price (AED)"],
            "Median Selling Price (AED)": kp["Median Selling Price (AED)"],
            "Mode Selling Price (AED)": kp["Mode Selling Price (AED)"],
            "Avg Size (Sqf)": kp["Avg Size (Sqf)"],
            "Median Size (Sqf)": kp["Median Size (Sqf)"],
            "Mode Size (Sqf)": kp["Mode Size (Sqf)"],
            "Avg PSF (AED/Sqf)": kp["Avg PSF (AED/Sqf)"],
            "Median PSF (AED/Sqf)": kp["Median PSF (AED/Sqf)"],
            "Mode PSF (AED/Sqf)": kp["Mode PSF (AED/Sqf)"],
            "Longest Dry Streak (days)": dry["longest_dry_streak_days"],
            "Dry Days (count)": dry["dry_days_total"],
        })
    cmp_df = pd.DataFrame(cmp_rows)
    st.dataframe(cmp_df, use_container_width=True, hide_index=True)

st.markdown("---")

# ------------------------- Overlapped charts -------------------------
st.subheader("Trends (overlaid when comparing scenarios)")

def overlay_chart(series_key: str, title: str, y_label: str) -> go.Figure:
    fig = go.Figure()
    for s in scenario_results:
        dd = s[series_key]
        fig.add_trace(go.Scatter(
            x=dd["Date"],
            y=dd["Value"],
            mode="lines",
            name=s["name"],
        ))
    fig.update_layout(
        template="plotly_white",
        title=title,
        xaxis_title="Date",
        yaxis_title=y_label,
        legend_title="Scenario",
        height=420,
        margin=dict(l=20, r=20, t=60, b=20),
    )
    return fig

c1, c2 = st.columns(2)
with c1:
    st.plotly_chart(overlay_chart("daily_cnt", "Daily transactions", "Transactions"), use_container_width=True)
with c2:
    st.plotly_chart(overlay_chart("daily_amt", "Daily total sales amount", "AED"), use_container_width=True)

# Dry-day overlay (basis selectable)
st.subheader("Dry-period diagnostic")
fig_dry = go.Figure()
for s in scenario_results:
    dd = daily_series(s["df"], basis=dry_basis_key)
    stats = dry_period_stats(dd, x_percent=float(dry_x), peak_quantile=float(peak_quantile))
    thr = stats["threshold_value"]
    fig_dry.add_trace(go.Scatter(x=dd["Date"], y=dd["Value"], mode="lines", name=s["name"]))
    fig_dry.add_trace(go.Scatter(
        x=[dd["Date"].min(), dd["Date"].max()],
        y=[thr, thr],
        mode="lines",
        line=dict(dash="dash"),
        name=f"{s['name']} threshold",
        showlegend=False,
    ))
fig_dry.update_layout(
    template="plotly_white",
    title=f"Dry days: value ≤ (x% of peak) | x={dry_x}% | peak={peak_def} | basis={dry_basis}",
    xaxis_title="Date",
    yaxis_title="Value",
    height=420,
    margin=dict(l=20, r=20, t=60, b=20),
)
st.plotly_chart(fig_dry, use_container_width=True)

st.markdown("---")

# ------------------------- Hotspots (Scenario 1) -------------------------
st.subheader("Hotspots (Scenario 1)")
df1 = primary["df"]

h1 = top_n(df1, C.COL_COMMUNITY, n=5)
h2 = top_n(df1, "Developer Group", n=5)
h3 = top_n(df1, C.COL_PROPERTY, n=5)

tab1, tab2, tab3 = st.tabs(["Communities", "Developers", "Properties / Projects"])

with tab1:
    cA, cB = st.columns(2)
    with cA:
        st.markdown("**Top 5 by transactions**")
        st.dataframe(h1["by_count"], use_container_width=True, hide_index=True)
    with cB:
        st.markdown("**Top 5 by total amount**")
        st.dataframe(h1["by_amount"], use_container_width=True, hide_index=True)

with tab2:
    cA, cB = st.columns(2)
    with cA:
        st.markdown("**Top 5 by transactions**")
        st.dataframe(h2["by_count"], use_container_width=True, hide_index=True)
    with cB:
        st.markdown("**Top 5 by total amount**")
        st.dataframe(h2["by_amount"], use_container_width=True, hide_index=True)

with tab3:
    cA, cB = st.columns(2)
    with cA:
        st.markdown("**Top 5 by transactions**")
        st.dataframe(h3["by_count"], use_container_width=True, hide_index=True)
    with cB:
        st.markdown("**Top 5 by total amount**")
        st.dataframe(h3["by_amount"], use_container_width=True, hide_index=True)


# ------------------------- Details table -------------------------
st.subheader("Transactions (Scenario 1)")
st.caption("This table respects your filters. Use it for drill-down and quick validation.")
display_cols = [
    C.COL_DATE,
    "Transaction Bucket",
    C.COL_PROPERTY_TYPE,
    C.COL_COMMUNITY,
    C.COL_PROPERTY,
    C.COL_BEDROOMS,
    C.COL_SIZE_SQF,
    C.COL_AMOUNT_AED,
    C.COL_AED_PSF,
    "Developer Group",
    "Developer Raw",
    C.COL_TIMES_SOLD,
]
available_cols = [c for c in display_cols if c in df1.columns]
st.dataframe(
    df1[available_cols].head(2000),
    use_container_width=True,
    hide_index=True,
)

st.markdown(
    "<div class='small-note'>Tip: If the charts look flat, your filters may be too tight. "
    "Try widening the date range or clearing Community/Developer/Property selections.</div>",
    unsafe_allow_html=True
)
