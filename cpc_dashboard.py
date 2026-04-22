"""
CBOE Total Put/Call Ratio ($CPC)
================================
Source: OCC daily volume statistics (marketdata.theocc.com)
First run downloads historical data (~5-10 min). Cached after that.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import requests
import os
import time
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
CACHE_FILE = os.path.join(CACHE_DIR, "cpc_data.csv")


def metric_card(label, value, color_class="metric-neutral"):
    return f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value {color_class}">{value}</div></div>'


# ── OCC Data ─────────────────────────────────────────────────────────────────

def parse_occ_daily(text):
    """Parse OCC daily-volume-statistics CSV for total put/call volumes."""
    lines = text.strip().split("\n")

    # Find header row with put/call columns
    header_idx = None
    for i, line in enumerate(lines):
        if "put" in line.lower() and "call" in line.lower():
            header_idx = i
            break

    if header_idx is None:
        return 0, 0

    headers = [h.strip().lower() for h in lines[header_idx].split(",")]
    put_idx = call_idx = None
    for j, h in enumerate(headers):
        if "put" in h and put_idx is None:
            put_idx = j
        if "call" in h and call_idx is None:
            call_idx = j

    if put_idx is None or call_idx is None:
        return 0, 0

    total_puts = total_calls = 0
    for i in range(header_idx + 1, len(lines)):
        parts = lines[i].split(",")
        if len(parts) > max(put_idx, call_idx):
            try:
                p = int(parts[put_idx].strip().replace(",", "").replace('"', ''))
                c = int(parts[call_idx].strip().replace(",", "").replace('"', ''))
                total_puts += p
                total_calls += c
            except (ValueError, TypeError):
                pass

    return total_puts, total_calls


def fetch_occ_day(date_str):
    """Fetch one day from OCC. date_str = YYYYMMDD."""
    url = f"https://marketdata.theocc.com/daily-volume-statistics?reportDate={date_str}&format=csv"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200 or len(r.text.strip()) < 50:
            return None
        puts, calls = parse_occ_daily(r.text)
        if calls > 0:
            return puts / calls
        return None
    except Exception:
        return None


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
    """Download OCC data for the needed date range, using cache for known days."""
    cached = load_cache()
    end = datetime.now()
    start = end - timedelta(days=int(years_needed * 365) + 60)

    # If cache fully covers the range, return immediately
    if not cached.empty:
        if cached.index.max() >= (end - timedelta(days=3)) and cached.index.min() <= start + timedelta(days=5):
            return cached

    # Figure out which days we still need
    all_days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            all_days.append(d)
        d += timedelta(days=1)

    cached_dates = set(cached.index.date) if not cached.empty else set()
    missing = [d for d in all_days if d.date() not in cached_dates]

    if not missing:
        return cached

    if status_text:
        status_text.text(f"Downloading {len(missing)} days from OCC...")

    new_rows = []
    consecutive_fails = 0

    for i, day in enumerate(missing):
        if progress_bar:
            progress_bar.progress((i + 1) / len(missing),
                                  text=f"OCC download: {day.strftime('%Y-%m-%d')} ({i+1}/{len(missing)})")

        ratio = fetch_occ_day(day.strftime("%Y%m%d"))
        if ratio is not None:
            new_rows.append({"Date": pd.Timestamp(day), "PC_Ratio": ratio})
            consecutive_fails = 0
        else:
            consecutive_fails += 1
            if consecutive_fails > 15:
                if status_text:
                    status_text.text("OCC not responding — using what we have...")
                break

        # Rate limit: ~3 req/sec
        if i % 3 == 0:
            time.sleep(0.35)

    if new_rows:
        new_df = pd.DataFrame(new_rows).set_index("Date")
        combined = pd.concat([cached, new_df])
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()
        save_cache(combined)
        return combined

    return cached


# ── Chart ────────────────────────────────────────────────────────────────────

def plot_cpc(df, years, ma_short, ma_long, show_raw):
    cutoff = df.index.max() - pd.DateOffset(years=years)
    df = df.loc[cutoff:].copy()
    df["MA_Short"] = df["PC_Ratio"].rolling(ma_short).mean()
    df["MA_Long"] = df["PC_Ratio"].rolling(ma_long).mean()

    fig = go.Figure()
    if show_raw:
        fig.add_trace(go.Scatter(
            x=df.index, y=df["PC_Ratio"], mode="lines", name="P/C Ratio",
            line=dict(color="#475569", width=1), opacity=0.4))
    fig.add_trace(go.Scatter(
        x=df.index, y=df["MA_Short"], mode="lines", name=f"{ma_short}-day MA",
        line=dict(color="#38bdf8", width=2)))
    fig.add_trace(go.Scatter(
        x=df.index, y=df["MA_Long"], mode="lines", name=f"{ma_long}-day MA",
        line=dict(color="#f59e0b", width=1.5, dash="dot")))

    fig.add_hline(y=1.0, line_dash="dash", line_color="#334155", line_width=1,
                  annotation_text="1.0 (neutral)", annotation_font_color="#64748b")
    fig.add_hline(y=0.8, line_dash="dot", line_color="#22c55e", line_width=1,
                  annotation_text="0.8 (bullish extreme)", annotation_font_color="#22c55e",
                  annotation_position="bottom left")
    fig.add_hline(y=1.2, line_dash="dot", line_color="#ef4444", line_width=1,
                  annotation_text="1.2 (bearish extreme)", annotation_font_color="#ef4444",
                  annotation_position="top left")

    fig.update_layout(**PLOT_LAYOUT,
                      title=dict(text="CBOE Total Put/Call Ratio ($CPC)",
                                 font=dict(size=16, color="#e2e8f0")),
                      yaxis_title="Put/Call Ratio", height=500)
    return fig, df


# ── Sidebar ──────────────────────────────────────────────────────────────────

st.sidebar.markdown("## ⚙️ Controls")
cpc_years = st.sidebar.selectbox("Lookback", [1, 2, 3, 5], index=1, key="yr")
cpc_ma_short = st.sidebar.slider("Short MA (days)", 5, 30, 10, key="ma_s")
cpc_ma_long = st.sidebar.slider("Long MA (days)", 10, 60, 20, key="ma_l")
cpc_show_raw = st.sidebar.checkbox("Show raw daily ratio", True, key="raw")
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Force refresh"):
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
    st.rerun()


# ── Main ─────────────────────────────────────────────────────────────────────

st.markdown("# 📊 CBOE Total Put/Call Ratio ($CPC)")

cached = load_cache()
needs_download = cached.empty or (datetime.now() - cached.index.max()).days > 3

if needs_download:
    progress = st.progress(0)
    status = st.empty()
    data = build_dataset(cpc_years + 0.5, progress_bar=progress, status_text=status)
    progress.empty()
    status.empty()
else:
    data = cached

if data is not None and not data.empty:
    data = data[(data["PC_Ratio"] > 0.3) & (data["PC_Ratio"] < 3.0)]

    fig, plot_data = plot_cpc(data, cpc_years, cpc_ma_short, cpc_ma_long, cpc_show_raw)

    latest = plot_data["PC_Ratio"].iloc[-1]
    ma_s = plot_data["MA_Short"].iloc[-1]
    ma_l = plot_data["MA_Long"].iloc[-1]
    pct = (plot_data["PC_Ratio"].dropna() <= latest).mean() * 100

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(metric_card("Latest P/C", f"{latest:.3f}",
                            "metric-red" if latest > 1.0 else "metric-green"), unsafe_allow_html=True)
    c2.markdown(metric_card(f"{cpc_ma_short}d MA", f"{ma_s:.3f}"), unsafe_allow_html=True)
    c3.markdown(metric_card(f"{cpc_ma_long}d MA", f"{ma_l:.3f}"), unsafe_allow_html=True)
    c4.markdown(metric_card(f"Pctile ({cpc_years}Y)", f"{pct:.0f}th"), unsafe_allow_html=True)

    st.plotly_chart(fig, use_container_width=True)

    st.markdown(
        f'<div class="source-note">'
        f'Source: OCC daily volume stats · Latest: {data.index.max().strftime("%Y-%m-%d")} · '
        f'{len(plot_data)} trading days shown · '
        f'&gt;1.0 = bearish sentiment · &lt;0.8 = complacency</div>',
        unsafe_allow_html=True)
else:
    st.error("No data available. Check internet connection or click Force refresh.")
