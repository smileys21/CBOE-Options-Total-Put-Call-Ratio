"""
CBOE Put/Call Ratio ($CPC) — Weekly from OCC
=============================================
Source: OCC weekly equity options volume reports
Properly handles quoted CSV fields with commas inside them.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import requests
import csv
import re
import os
import time
import io
from datetime import datetime, timedelta

st.set_page_config(page_title="$CPC Put/Call Ratio", page_icon="📊", layout="wide")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
    .stApp { background-color: #0a0e17; }
    h1, h2, h3, .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
        font-family: 'IBM Plex Sans', sans-serif !important; color: #e2e8f0 !important;
    }
    p, span, div, .stMarkdown p {
        font-family: 'IBM Plex Sans', sans-serif !important; color: #94a3b8 !important;
    }
    .block-container { padding-top: 1.5rem; }
    .metric-card {
        background: linear-gradient(135deg, #111827 0%, #1a1f2e 100%);
        border: 1px solid #1e293b; border-radius: 8px; padding: 16px 20px; margin-bottom: 8px;
    }
    .metric-label { font-size: 0.75rem; color: #64748b !important; text-transform: uppercase;
                    letter-spacing: 0.05em; margin-bottom: 4px; }
    .metric-value { font-size: 1.5rem; font-weight: 600; font-family: 'IBM Plex Mono', monospace !important; }
    .metric-green { color: #34d399 !important; }
    .metric-red { color: #f87171 !important; }
    .metric-neutral { color: #e2e8f0 !important; }
    .source-note {
        font-size: 0.7rem; color: #475569 !important; font-family: 'IBM Plex Mono', monospace !important;
        border-top: 1px solid #1e293b; padding-top: 8px; margin-top: 12px;
    }
</style>
""", unsafe_allow_html=True)

PLOT_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="#0a0e17", plot_bgcolor="#0f1420",
    font=dict(family="IBM Plex Sans", color="#94a3b8", size=12),
    margin=dict(l=60, r=30, t=50, b=40),
    xaxis=dict(gridcolor="#1e293b", zerolinecolor="#1e293b", showgrid=True, gridwidth=1),
    yaxis=dict(gridcolor="#1e293b", zerolinecolor="#334155", showgrid=True, gridwidth=1),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="rgba(0,0,0,0)", font=dict(size=11)),
)

CACHE_DIR = os.path.join(os.path.expanduser("~"), ".cpc_cache")
os.makedirs(CACHE_DIR, exist_ok=True)
CACHE_FILE = os.path.join(CACHE_DIR, "cpc_weekly.csv")


def metric_card(label, value, color_class="metric-neutral"):
    return f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value {color_class}">{value}</div></div>'


# ── OCC Parsing (fixed) ─────────────────────────────────────────────────────

def parse_weekly(text):
    """
    Parse OCC weekly volume report using csv.reader to handle quoted fields.
    Sums ALL 'TOTAL CALLS' and 'TOTAL PUTS' rows across sections
    (equity regular + equity flex) for a true total ratio.
    """
    total_calls = 0
    total_puts = 0

    reader = csv.reader(io.StringIO(text))
    for row in reader:
        if not row:
            continue
        label = row[0].strip().upper()
        if "TOTAL CALLS" in label and len(row) >= 2:
            try:
                total_calls += int(row[1].strip().replace(",", ""))
            except (ValueError, IndexError):
                pass
        elif "TOTAL PUTS" in label and len(row) >= 2:
            try:
                total_puts += int(row[1].strip().replace(",", ""))
            except (ValueError, IndexError):
                pass

    return total_calls, total_puts


def get_week_date(text):
    m = re.search(r"Week Ending\s+(\d{2}/\d{2}/\d{4})", text)
    return datetime.strptime(m.group(1), "%m/%d/%Y") if m else None


def fetch_week(friday):
    ds = friday.strftime("%Y%m%d")
    url = f"https://marketdata.theocc.com/weekly-volume-reports?reportDate={ds}&reportType=options&reportClass=equity&format=csv"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200 and len(r.content) > 100:
            text = r.content.decode("utf-8", errors="replace")
            calls, puts = parse_weekly(text)
            if calls > 1000 and puts > 1000:
                week_date = get_week_date(text) or friday
                return {"date": week_date, "calls": calls, "puts": puts, "ratio": puts / calls}
    except Exception:
        pass
    return None


def get_fridays(start, end):
    d = start
    while d.weekday() != 4:
        d += timedelta(days=1)
    fridays = []
    while d <= end:
        fridays.append(d)
        d += timedelta(days=7)
    return fridays


def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            return pd.read_csv(CACHE_FILE, parse_dates=["Date"], index_col="Date")
        except Exception:
            pass
    return pd.DataFrame()


def save_cache(df):
    df.to_csv(CACHE_FILE)


def build_dataset(years_needed, progress_bar=None, status_text=None):
    cached = load_cache()
    end = datetime.now()
    start = end - timedelta(days=int(years_needed * 365) + 30)

    if not cached.empty:
        cache_age = (end - cached.index.max()).days
        if cache_age <= 9 and cached.index.min() <= pd.Timestamp(start + timedelta(days=14)):
            return cached

    fridays = get_fridays(start, end)
    cached_dates = set(cached.index.date) if not cached.empty else set()
    missing = [f for f in fridays if f.date() not in cached_dates]

    if not missing and not cached.empty:
        return cached

    if status_text:
        status_text.text(f"Downloading {len(missing)} weeks from OCC...")

    rows = []
    fails = 0
    for i, friday in enumerate(missing):
        if progress_bar:
            progress_bar.progress((i + 1) / len(missing),
                                  text=f"Week ending {friday.strftime('%Y-%m-%d')} ({i+1}/{len(missing)})")
        result = fetch_week(friday)
        if result:
            rows.append({"Date": pd.Timestamp(result["date"]),
                         "PC_Ratio": result["ratio"],
                         "Puts": result["puts"], "Calls": result["calls"]})
            fails = 0
        else:
            fails += 1
            if fails > 12:
                break
        if i % 3 == 0:
            time.sleep(0.3)

    if rows:
        new_df = pd.DataFrame(rows).set_index("Date")
        combined = pd.concat([cached, new_df])
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()
        save_cache(combined)
        return combined

    return cached if not cached.empty else pd.DataFrame()


# ── Chart ────────────────────────────────────────────────────────────────────

def plot_cpc(df, years, ma_short, ma_long, show_raw):
    cutoff = df.index.max() - pd.DateOffset(years=years)
    df = df.loc[cutoff:].copy()
    df["MA_Short"] = df["PC_Ratio"].rolling(ma_short).mean()
    df["MA_Long"] = df["PC_Ratio"].rolling(ma_long).mean()

    fig = go.Figure()
    if show_raw:
        fig.add_trace(go.Scatter(
            x=df.index, y=df["PC_Ratio"], mode="lines", name="Weekly P/C",
            line=dict(color="#475569", width=1), opacity=0.5))
    fig.add_trace(go.Scatter(
        x=df.index, y=df["MA_Short"], mode="lines", name=f"{ma_short}-wk MA",
        line=dict(color="#38bdf8", width=2)))
    fig.add_trace(go.Scatter(
        x=df.index, y=df["MA_Long"], mode="lines", name=f"{ma_long}-wk MA",
        line=dict(color="#f59e0b", width=1.5, dash="dot")))

    fig.add_hline(y=1.0, line_dash="dash", line_color="#334155", line_width=1,
                  annotation_text="1.0", annotation_font_color="#64748b")
    fig.add_hline(y=0.8, line_dash="dot", line_color="#22c55e", line_width=1,
                  annotation_text="0.8", annotation_font_color="#22c55e",
                  annotation_position="bottom left")
    fig.add_hline(y=1.2, line_dash="dot", line_color="#ef4444", line_width=1,
                  annotation_text="1.2", annotation_font_color="#ef4444",
                  annotation_position="top left")

    fig.update_layout(**PLOT_LAYOUT,
                      title=dict(text="Put/Call Ratio ($CPC · weekly from OCC)",
                                 font=dict(size=16, color="#e2e8f0")),
                      yaxis_title="Put/Call Ratio", height=500)
    return fig, df


# ── Sidebar ──────────────────────────────────────────────────────────────────

st.sidebar.markdown("## ⚙️ Controls")
cpc_years = st.sidebar.selectbox("Lookback", [1, 2, 3, 5], index=1, key="yr")
cpc_ma_short = st.sidebar.slider("Short MA (weeks)", 2, 12, 4, key="ma_s")
cpc_ma_long = st.sidebar.slider("Long MA (weeks)", 4, 26, 10, key="ma_l")
cpc_show_raw = st.sidebar.checkbox("Show raw weekly ratio", True, key="raw")
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Force refresh"):
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
    st.rerun()


# ── Main ─────────────────────────────────────────────────────────────────────

st.markdown("# 📊 Put/Call Ratio ($CPC)")
st.markdown("*Weekly equity options volume from OCC · Puts ÷ Calls*")
st.divider()

cached = load_cache()
needs_download = cached.empty or (datetime.now() - cached.index.max()).days > 9

if needs_download:
    progress = st.progress(0)
    status = st.empty()
    data = build_dataset(cpc_years + 0.5, progress_bar=progress, status_text=status)
    progress.empty()
    status.empty()
else:
    data = cached

if data is not None and not data.empty:
    data = data[(data["PC_Ratio"] > 0.2) & (data["PC_Ratio"] < 3.0)]

    fig, plot_data = plot_cpc(data, cpc_years, cpc_ma_short, cpc_ma_long, cpc_show_raw)

    latest = plot_data["PC_Ratio"].iloc[-1]
    ma_s = plot_data["MA_Short"].iloc[-1]
    ma_l = plot_data["MA_Long"].iloc[-1]
    if pd.isna(ma_s): ma_s = 0
    if pd.isna(ma_l): ma_l = 0
    pct = (plot_data["PC_Ratio"].dropna() <= latest).mean() * 100

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(metric_card("Latest P/C", f"{latest:.3f}",
                            "metric-red" if latest > 1.0 else "metric-green"), unsafe_allow_html=True)
    c2.markdown(metric_card(f"{cpc_ma_short}-wk MA", f"{ma_s:.3f}"), unsafe_allow_html=True)
    c3.markdown(metric_card(f"{cpc_ma_long}-wk MA", f"{ma_l:.3f}"), unsafe_allow_html=True)
    c4.markdown(metric_card(f"Pctile ({cpc_years}Y)", f"{pct:.0f}th"), unsafe_allow_html=True)

    st.plotly_chart(fig, use_container_width=True)

    st.markdown(
        f'<div class="source-note">'
        f'Source: OCC weekly equity options volume · '
        f'{data.index.min().strftime("%Y-%m-%d")} to {data.index.max().strftime("%Y-%m-%d")} · '
        f'{len(plot_data)} weeks shown · '
        f'&gt;1.0 = bearish sentiment · &lt;0.8 = complacency</div>',
        unsafe_allow_html=True)

    with st.expander("📖 Notes"):
        st.markdown(f"""
**Data:** Weekly total put and call contract volumes for equity options, from OCC. 
Ratio = total puts ÷ total calls across all exchanges cleared by OCC.

**vs StockCharts $CPC:** StockCharts shows the *daily* CBOE-only ratio. This shows *weekly* across all OCC-cleared exchanges. 
The trend and levels are comparable but not identical. The 4-week MA here ≈ the 20-day MA on StockCharts.

**Cache:** `~/.cpc_cache/cpc_weekly.csv` — first load fetches ~{int(cpc_years*52)} weeks, subsequent loads only grab new weeks.
""")
else:
    st.error("No data available. Check internet or click Force refresh in sidebar.")
