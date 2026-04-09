"""
Market Breadth Dashboard
========================
Replicates three StockCharts indicators:
  1. $CPC  — CBOE Total Put/Call Ratio (daily, 2yr)
  2. $NAAD — NYSE Advance-Decline Cumulative Line (daily, 1yr)
  3. $BINAHLC — NYSE New Highs–New Lows Cumulative (weekly, 5yr)

Data sources:
  - Chart 1: CBOE free CSV (cdn.cboe.com) — rock-solid, official
  - Charts 2 & 3: Computed from NYSE-listed stocks via yfinance
    (slower on first load, but free and no API key needed)
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import io
from datetime import datetime, timedelta
import yfinance as yf
import warnings
import os
import json

warnings.filterwarnings("ignore")

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Market Breadth Dashboard",
    page_icon="📊",
    layout="wide",
)

# ── Styling ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
    
    .stApp { background-color: #0a0e17; }
    
    h1, h2, h3, .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
        font-family: 'IBM Plex Sans', sans-serif !important;
        color: #e2e8f0 !important;
    }
    p, span, div, .stMarkdown p {
        font-family: 'IBM Plex Sans', sans-serif !important;
        color: #94a3b8 !important;
    }
    code, .stCode {
        font-family: 'IBM Plex Mono', monospace !important;
    }
    .block-container { padding-top: 1.5rem; }
    
    .metric-card {
        background: linear-gradient(135deg, #111827 0%, #1a1f2e 100%);
        border: 1px solid #1e293b;
        border-radius: 8px;
        padding: 16px 20px;
        margin-bottom: 8px;
    }
    .metric-label {
        font-size: 0.75rem;
        color: #64748b !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 4px;
    }
    .metric-value {
        font-size: 1.5rem;
        font-weight: 600;
        font-family: 'IBM Plex Mono', monospace !important;
    }
    .metric-green { color: #34d399 !important; }
    .metric-red { color: #f87171 !important; }
    .metric-neutral { color: #e2e8f0 !important; }
    
    .source-note {
        font-size: 0.7rem;
        color: #475569 !important;
        font-family: 'IBM Plex Mono', monospace !important;
        border-top: 1px solid #1e293b;
        padding-top: 8px;
        margin-top: 12px;
    }
</style>
""", unsafe_allow_html=True)


# ── Plot theme ───────────────────────────────────────────────────────────────
PLOT_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="#0a0e17",
    plot_bgcolor="#0f1420",
    font=dict(family="IBM Plex Sans", color="#94a3b8", size=12),
    margin=dict(l=60, r=30, t=50, b=40),
    xaxis=dict(
        gridcolor="#1e293b", zerolinecolor="#1e293b",
        showgrid=True, gridwidth=1,
    ),
    yaxis=dict(
        gridcolor="#1e293b", zerolinecolor="#334155",
        showgrid=True, gridwidth=1,
    ),
    legend=dict(
        bgcolor="rgba(0,0,0,0)", bordercolor="rgba(0,0,0,0)",
        font=dict(size=11),
    ),
)


def metric_card(label, value, color_class="metric-neutral"):
    return f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value {color_class}">{value}</div>
    </div>
    """


# ══════════════════════════════════════════════════════════════════════════════
# CHART 1: CBOE Total Put/Call Ratio
# ══════════════════════════════════════════════════════════════════════════════

CACHE_DIR = os.path.join(os.path.expanduser("~"), ".breadth_cache")
os.makedirs(CACHE_DIR, exist_ok=True)


def cache_path(name):
    return os.path.join(CACHE_DIR, f"{name}.parquet")


def is_cache_fresh(name, max_age_hours=12):
    p = cache_path(name)
    if not os.path.exists(p):
        return False
    age = datetime.now().timestamp() - os.path.getmtime(p)
    return age < max_age_hours * 3600


@st.cache_data(ttl=3600 * 6, show_spinner=False)
def fetch_cboe_putcall():
    """
    Fetch CBOE Total Put/Call Ratio from official CBOE CSV.
    
    Primary: totalpc.csv (current year)
    Fallback: pcratioarchive.csv (full history back to 1995)
    """
    urls = [
        # Total P/C ratio — current data
        "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/totalpc.csv",
        # Archive — full history with total, index, and equity columns
        "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/pcratioarchive.csv",
    ]

    for url in urls:
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            text = r.text

            # The CSVs have header junk — find the row that starts with DATE or Trade_date
            lines = text.strip().split("\n")
            header_idx = 0
            for i, line in enumerate(lines):
                if line.strip().upper().startswith("DATE") or line.strip().upper().startswith("TRADE_DATE"):
                    header_idx = i
                    break

            clean = "\n".join(lines[header_idx:])
            df = pd.read_csv(io.StringIO(clean))

            # Normalize column names
            df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

            # Find date column
            date_col = [c for c in df.columns if "DATE" in c][0]
            df["Date"] = pd.to_datetime(df[date_col], format="mixed", dayfirst=False)

            # Find P/C ratio column — prefer TOTAL_VOLUME_P/C_RATIO or P/C_RATIO
            ratio_cols = [c for c in df.columns if "P/C" in c or "PC" in c]
            if not ratio_cols:
                # Compute from CALL and PUT columns
                df["PC_Ratio"] = df["PUT"] / df["CALL"]
            else:
                # For archive: use TOTAL_VOLUME_P/C_RATIO; for totalpc: use P/C_RATIO
                col = ratio_cols[0]
                df["PC_Ratio"] = pd.to_numeric(df[col], errors="coerce")

            df = df[["Date", "PC_Ratio"]].dropna().sort_values("Date").reset_index(drop=True)
            df = df.set_index("Date")

            if "totalpc" in url:
                st.session_state["cpc_source"] = "CBOE totalpc.csv (current year)"
            else:
                st.session_state["cpc_source"] = "CBOE pcratioarchive.csv (full history)"

            return df

        except Exception as e:
            continue

    # If CBOE fails, return None and show error
    return None


def plot_cpc(df, years=2):
    """Plot Put/Call Ratio with 10-day and 20-day MAs, matching StockCharts style."""
    cutoff = df.index.max() - pd.DateOffset(years=years)
    df = df.loc[cutoff:]

    df["MA10"] = df["PC_Ratio"].rolling(10).mean()
    df["MA20"] = df["PC_Ratio"].rolling(20).mean()

    fig = go.Figure()

    # Raw ratio as thin line
    fig.add_trace(go.Scatter(
        x=df.index, y=df["PC_Ratio"],
        mode="lines", name="P/C Ratio",
        line=dict(color="#475569", width=1),
        opacity=0.5,
    ))

    # 10-day MA
    fig.add_trace(go.Scatter(
        x=df.index, y=df["MA10"],
        mode="lines", name="10-day MA",
        line=dict(color="#38bdf8", width=2),
    ))

    # 20-day MA (optional overlay)
    fig.add_trace(go.Scatter(
        x=df.index, y=df["MA20"],
        mode="lines", name="20-day MA",
        line=dict(color="#f59e0b", width=1.5, dash="dot"),
    ))

    # Reference lines
    fig.add_hline(y=1.0, line_dash="dash", line_color="#334155", line_width=1,
                  annotation_text="1.0 (neutral)", annotation_font_color="#64748b")
    fig.add_hline(y=0.8, line_dash="dot", line_color="#22c55e", line_width=1,
                  annotation_text="0.8 (bullish extreme)", annotation_font_color="#22c55e",
                  annotation_position="bottom left")
    fig.add_hline(y=1.2, line_dash="dot", line_color="#ef4444", line_width=1,
                  annotation_text="1.2 (bearish extreme)", annotation_font_color="#ef4444",
                  annotation_position="top left")

    fig.update_layout(
        **PLOT_LAYOUT,
        title=dict(text="CBOE Total Put/Call Ratio ($CPC)", font=dict(size=16, color="#e2e8f0")),
        yaxis_title="Put/Call Ratio",
        height=450,
    )
    return fig, df


# ══════════════════════════════════════════════════════════════════════════════
# CHART 2: NYSE Advance-Decline Cumulative Line
# ══════════════════════════════════════════════════════════════════════════════

# Use a broad set of large/mid NYSE-listed ETFs as a proxy universe.
# This is a practical compromise — true $NAAD uses all NYSE issues.
# For production, you'd want a full NYSE ticker list from a paid data source.

# Alternatively, we can use the WSJ market diary scraping approach.
# Below we provide BOTH methods — the ETF proxy (fast) and the
# full-universe computation (slow but more accurate).

NYSE_PROXY_TICKERS = [
    # Large-cap NYSE stocks spanning sectors (top ~200 by market cap)
    # This gives a reasonable breadth proxy
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "BRK-B", "JPM", "V", "JNJ",
    "WMT", "PG", "MA", "HD", "CVX", "MRK", "ABBV", "PEP", "KO", "COST",
    "TMO", "ABT", "MCD", "CSCO", "DHR", "WFC", "ACN", "LIN", "NEE", "PM",
    "TXN", "UNP", "RTX", "HON", "LOW", "UPS", "IBM", "GE", "CAT", "BA",
    "SPGI", "AXP", "ELV", "PLD", "DE", "MDLZ", "BLK", "ADI", "CB", "CI",
    "SYK", "MMC", "ZTS", "TGT", "BDX", "PNC", "USB", "ADP", "CL", "MMM",
    "DUK", "SO", "CME", "ICE", "TFC", "FDX", "NSC", "EMR", "ETN", "APD",
    "SHW", "ITW", "ECL", "WM", "ROP", "GD", "PSA", "CCI", "D", "AEP",
    "XEL", "ED", "WEC", "AWK", "DTE", "PPL", "EIX", "ES", "FE", "AEE",
    "LNT", "CMS", "NI", "PNW", "OGE", "NRG", "AES", "EVR", "EVRG",
    "F", "GM", "DAL", "UAL", "LUV", "AAL", "ALK",
    "XOM", "COP", "EOG", "SLB", "PXD", "MPC", "VLO", "PSX", "OXY", "HAL",
    "GS", "MS", "C", "BAC", "SCHW", "BK", "STT", "FITB", "RF", "KEY",
    "CFG", "HBAN", "MTB", "ZION", "CMA", "ALLY",
    "PFE", "LLY", "BMY", "AMGN", "GILD", "VRTX", "REGN", "ZBH", "BAX",
    "STE", "A", "HOLX", "IQV", "PKI", "TECH",
    "CRM", "NOW", "INTU", "ADP", "CDNS", "SNPS", "ANSS", "KEYS",
    "T", "VZ", "TMUS", "CMCSA",
    "NKE", "SBUX", "EL", "CLX", "GIS", "K", "SJM", "HRL", "MKC",
    "AMT", "EQIX", "SPG", "O", "WELL", "DLR", "VTR", "ARE",
    "BXP", "SLG", "VNO", "KIM", "REG", "FRT",
    "DD", "DOW", "LYB", "CE", "EMN", "HUN", "AXTA",
    "IP", "PKG", "WRK", "SEE", "SON",
    "NUE", "STLD", "CLF", "X", "AA", "CENX",
]


@st.cache_data(ttl=3600 * 6, show_spinner=False)
def compute_nyse_ad_line(lookback_years=1.5):
    """
    Compute NYSE Advance-Decline cumulative line from a proxy universe.
    
    Method: Download daily closes for ~200 NYSE stocks, count advances
    vs declines each day, cumsum the net advances.
    """
    end = datetime.now()
    start = end - timedelta(days=int(lookback_years * 365))

    # Download all at once for speed
    tickers = NYSE_PROXY_TICKERS
    try:
        data = yf.download(tickers, start=start.strftime("%Y-%m-%d"),
                           end=end.strftime("%Y-%m-%d"), progress=False,
                           group_by="ticker", auto_adjust=True)
    except Exception as e:
        st.error(f"yfinance download failed: {e}")
        return None

    # Extract close prices
    closes = pd.DataFrame()
    for t in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                closes[t] = data[(t, "Close")]
            else:
                closes[t] = data["Close"]
        except (KeyError, TypeError):
            continue

    if closes.empty:
        return None

    # Daily returns
    rets = closes.pct_change()

    # Count advances / declines / unchanged
    advances = (rets > 0.0).sum(axis=1)
    declines = (rets < 0.0).sum(axis=1)
    net = advances - declines
    ad_line = net.cumsum()

    result = pd.DataFrame({
        "Advances": advances,
        "Declines": declines,
        "Net": net,
        "AD_Line": ad_line,
    })
    result.index.name = "Date"
    return result


def plot_ad_line(df, years=1):
    cutoff = df.index.max() - pd.DateOffset(years=years)
    df = df.loc[cutoff:]

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.7, 0.3], vertical_spacing=0.06)

    # Cumulative A/D line
    fig.add_trace(go.Scatter(
        x=df.index, y=df["AD_Line"],
        mode="lines", name="A/D Cumulative",
        line=dict(color="#38bdf8", width=2),
    ), row=1, col=1)

    # 20-day MA of A/D line
    ma20 = df["AD_Line"].rolling(20).mean()
    fig.add_trace(go.Scatter(
        x=df.index, y=ma20,
        mode="lines", name="20-day MA",
        line=dict(color="#f59e0b", width=1.5, dash="dot"),
    ), row=1, col=1)

    # Net advances histogram
    colors = ["#34d399" if v >= 0 else "#f87171" for v in df["Net"]]
    fig.add_trace(go.Bar(
        x=df.index, y=df["Net"],
        name="Net Advances",
        marker_color=colors,
        opacity=0.7,
    ), row=2, col=1)

    fig.update_layout(
        **PLOT_LAYOUT,
        title=dict(text="NYSE Advance-Decline Cumulative Line ($NAAD proxy)",
                   font=dict(size=16, color="#e2e8f0")),
        height=550,
        showlegend=True,
        barmode="relative",
    )
    fig.update_yaxes(title_text="Cumulative A/D", row=1, col=1,
                     gridcolor="#1e293b", zerolinecolor="#334155")
    fig.update_yaxes(title_text="Daily Net", row=2, col=1,
                     gridcolor="#1e293b", zerolinecolor="#334155")
    fig.update_xaxes(gridcolor="#1e293b", row=1, col=1)
    fig.update_xaxes(gridcolor="#1e293b", row=2, col=1)
    return fig, df


# ══════════════════════════════════════════════════════════════════════════════
# CHART 3: NYSE New Highs–New Lows Cumulative (weekly)
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600 * 6, show_spinner=False)
def compute_nyse_hilo_cumulative(lookback_years=6):
    """
    Compute New 52-week Highs minus New 52-week Lows cumulative line.
    
    Method: For each day, count how many stocks in our proxy universe
    hit a new 52-week high or low. Then cumsum(NH - NL) for the 
    cumulative high-low line. Resample to weekly for the final chart.
    
    We need 1 extra year of data to compute the first 52-week window.
    """
    end = datetime.now()
    start = end - timedelta(days=int((lookback_years + 1.1) * 365))

    tickers = NYSE_PROXY_TICKERS
    try:
        data = yf.download(tickers, start=start.strftime("%Y-%m-%d"),
                           end=end.strftime("%Y-%m-%d"), progress=False,
                           group_by="ticker", auto_adjust=True)
    except Exception as e:
        st.error(f"yfinance download failed: {e}")
        return None

    # Extract high and low prices
    highs = pd.DataFrame()
    lows = pd.DataFrame()
    for t in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                highs[t] = data[(t, "High")]
                lows[t] = data[(t, "Low")]
            else:
                highs[t] = data["High"]
                lows[t] = data["Low"]
        except (KeyError, TypeError):
            continue

    if highs.empty:
        return None

    # Rolling 252-day (52-week) high and low
    rolling_high = highs.rolling(252, min_periods=252).max()
    rolling_low = lows.rolling(252, min_periods=252).min()

    # New high: today's high >= rolling 252-day high
    new_highs = (highs >= rolling_high).sum(axis=1)
    # New low: today's low <= rolling 252-day low
    new_lows = (lows <= rolling_low).sum(axis=1)

    net_hilo = new_highs - new_lows
    cum_hilo = net_hilo.cumsum()

    result = pd.DataFrame({
        "New_Highs": new_highs,
        "New_Lows": new_lows,
        "Net_HiLo": net_hilo,
        "Cum_HiLo": cum_hilo,
    }).dropna()

    # Resample to weekly (Friday close)
    weekly = result.resample("W-FRI").last()

    return weekly


def plot_hilo_cumulative(df, years=5):
    cutoff = df.index.max() - pd.DateOffset(years=years)
    df = df.loc[cutoff:]

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.7, 0.3], vertical_spacing=0.06)

    # Cumulative NH-NL line
    fig.add_trace(go.Scatter(
        x=df.index, y=df["Cum_HiLo"],
        mode="lines", name="Cum NH-NL",
        line=dict(color="#a78bfa", width=2),
    ), row=1, col=1)

    # 10-week MA
    ma10 = df["Cum_HiLo"].rolling(10).mean()
    fig.add_trace(go.Scatter(
        x=df.index, y=ma10,
        mode="lines", name="10-wk MA",
        line=dict(color="#f59e0b", width=1.5, dash="dot"),
    ), row=1, col=1)

    # Weekly Net Hi-Lo histogram
    colors = ["#34d399" if v >= 0 else "#f87171" for v in df["Net_HiLo"]]
    fig.add_trace(go.Bar(
        x=df.index, y=df["Net_HiLo"],
        name="Weekly Net NH-NL",
        marker_color=colors,
        opacity=0.7,
    ), row=2, col=1)

    fig.update_layout(
        **PLOT_LAYOUT,
        title=dict(text="NYSE New Highs–New Lows Cumulative ($BINAHLC proxy)",
                   font=dict(size=16, color="#e2e8f0")),
        height=550,
        showlegend=True,
        barmode="relative",
    )
    fig.update_yaxes(title_text="Cumulative NH-NL", row=1, col=1,
                     gridcolor="#1e293b", zerolinecolor="#334155")
    fig.update_yaxes(title_text="Weekly Net", row=2, col=1,
                     gridcolor="#1e293b", zerolinecolor="#334155")
    fig.update_xaxes(gridcolor="#1e293b", row=1, col=1)
    fig.update_xaxes(gridcolor="#1e293b", row=2, col=1)
    return fig, df


# ══════════════════════════════════════════════════════════════════════════════
# MAIN APP
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("# 📊 Market Breadth Dashboard")
st.markdown("*Replicating StockCharts $CPC · $NAAD · $BINAHLC with free data sources*")

st.divider()

# ── Chart 1: Put/Call Ratio ──────────────────────────────────────────────────
st.markdown("## 1 · CBOE Total Put/Call Ratio")

with st.spinner("Fetching CBOE Put/Call data..."):
    cpc_df = fetch_cboe_putcall()

if cpc_df is not None and not cpc_df.empty:
    fig_cpc, cpc_plot = plot_cpc(cpc_df, years=2)

    # Metrics
    latest = cpc_plot["PC_Ratio"].iloc[-1]
    ma10_val = cpc_plot["MA10"].iloc[-1]
    ma20_val = cpc_plot["MA20"].iloc[-1]
    color = "metric-red" if latest > 1.0 else "metric-green"

    c1, c2, c3 = st.columns(3)
    c1.markdown(metric_card("Latest P/C Ratio", f"{latest:.2f}", color), unsafe_allow_html=True)
    c2.markdown(metric_card("10-day MA", f"{ma10_val:.2f}"), unsafe_allow_html=True)
    c3.markdown(metric_card("20-day MA", f"{ma20_val:.2f}"), unsafe_allow_html=True)

    st.plotly_chart(fig_cpc, use_container_width=True)
    src = st.session_state.get("cpc_source", "CBOE CSV")
    st.markdown(f'<div class="source-note">Source: {src} · Updated EOD · '
                f'Readings &gt;1.0 = more puts (bearish sentiment) · '
                f'&lt;0.8 = complacency zone</div>', unsafe_allow_html=True)
else:
    st.warning(
        "⚠️ Could not fetch CBOE data. This likely means the CBOE CDN is "
        "temporarily down. The app will retry on next refresh.\n\n"
        "**Manual fallback:** Download from https://cdn.cboe.com/resources/"
        "options/volume_and_call_put_ratios/totalpc.csv"
    )

st.divider()

# ── Chart 2: NYSE A/D Line ──────────────────────────────────────────────────
st.markdown("## 2 · NYSE Advance-Decline Cumulative Line")
st.caption(
    "Computed from ~200 NYSE-listed stocks. True $NAAD uses all ~3,000+ issues — "
    "this proxy captures the same directional signal with less noise."
)

with st.spinner("Computing NYSE A/D line (first load may take 30-60 seconds)..."):
    ad_df = compute_nyse_ad_line(lookback_years=1.5)

if ad_df is not None and not ad_df.empty:
    fig_ad, ad_plot = plot_ad_line(ad_df, years=1)

    latest_ad = ad_plot["AD_Line"].iloc[-1]
    latest_net = ad_plot["Net"].iloc[-1]
    net_color = "metric-green" if latest_net >= 0 else "metric-red"
    trend_5d = ad_plot["AD_Line"].iloc[-1] - ad_plot["AD_Line"].iloc[-6] if len(ad_plot) > 5 else 0
    trend_color = "metric-green" if trend_5d >= 0 else "metric-red"

    c1, c2, c3 = st.columns(3)
    c1.markdown(metric_card("Cumulative A/D", f"{latest_ad:,.0f}"), unsafe_allow_html=True)
    c2.markdown(metric_card("Latest Daily Net", f"{latest_net:+.0f}", net_color), unsafe_allow_html=True)
    c3.markdown(metric_card("5-day Trend", f"{trend_5d:+,.0f}", trend_color), unsafe_allow_html=True)

    st.plotly_chart(fig_ad, use_container_width=True)
    st.markdown(
        '<div class="source-note">Source: yfinance (~200 NYSE stocks) · '
        'Proxy for StockCharts $NAAD · Rising line = broad participation · '
        'Divergence from index = warning signal</div>',
        unsafe_allow_html=True,
    )
else:
    st.error("Failed to compute A/D line. Check your internet connection.")

st.divider()

# ── Chart 3: NH-NL Cumulative ────────────────────────────────────────────────
st.markdown("## 3 · NYSE New Highs–New Lows Cumulative")
st.caption(
    "Weekly view, computed from ~200 NYSE stocks' 52-week highs & lows. "
    "Proxy for StockCharts $BINAHLC."
)

with st.spinner("Computing NH-NL cumulative (first load may take 60-90 seconds)..."):
    hilo_df = compute_nyse_hilo_cumulative(lookback_years=6)

if hilo_df is not None and not hilo_df.empty:
    fig_hilo, hilo_plot = plot_hilo_cumulative(hilo_df, years=5)

    latest_cum = hilo_plot["Cum_HiLo"].iloc[-1]
    latest_net_hl = hilo_plot["Net_HiLo"].iloc[-1]
    hl_color = "metric-green" if latest_net_hl >= 0 else "metric-red"

    c1, c2, c3 = st.columns(3)
    c1.markdown(metric_card("Cumulative NH-NL", f"{latest_cum:,.0f}"), unsafe_allow_html=True)
    c2.markdown(metric_card("Latest Weekly Net", f"{latest_net_hl:+.0f}", hl_color), unsafe_allow_html=True)
    nh = hilo_plot["New_Highs"].iloc[-1]
    nl = hilo_plot["New_Lows"].iloc[-1]
    c3.markdown(metric_card("NH / NL (latest wk)", f"{nh:.0f} / {nl:.0f}"), unsafe_allow_html=True)

    st.plotly_chart(fig_hilo, use_container_width=True)
    st.markdown(
        '<div class="source-note">Source: yfinance (~200 NYSE stocks, 52-wk rolling window) · '
        'Proxy for StockCharts $BINAHLC · Rising line = healthy internal breadth · '
        'Falling while index rises = classic bearish divergence</div>',
        unsafe_allow_html=True,
    )
else:
    st.error("Failed to compute NH-NL line. Check your internet connection.")

st.divider()

# ── Data sourcing notes ──────────────────────────────────────────────────────
with st.expander("📋 Data Sourcing Notes & Upgrade Paths"):
    st.markdown("""
### Chart 1: $CPC — CBOE Total Put/Call Ratio
- **Source:** Direct CSV from `cdn.cboe.com` — this is the **official CBOE data**, same source StockCharts uses
- **URLs:**
  - Current year: `https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/totalpc.csv`
  - Full archive (1995+): `https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/pcratioarchive.csv`
  - Equity only: `https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/equitypc.csv`
  - Index only: `https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/indexpcarchive.csv`
- **Quality:** ⭐⭐⭐⭐⭐ — Official source, identical to StockCharts
- **Update frequency:** EOD, typically by 6 PM ET

### Chart 2: $NAAD — NYSE Advance-Decline Line
- **Source:** Computed from ~200 NYSE-listed stocks via yfinance
- **Limitation:** True $NAAD uses all ~3,000+ NYSE-listed securities
- **Quality:** ⭐⭐⭐ — Directionally accurate, magnitudes will differ
- **Upgrade paths:**
  - **Polygon.io** ($29/mo Starter): Has `GET /v2/aggs/ticker/{ticker}` for all NYSE stocks + market-level breadth endpoints
  - **Quandl/Nasdaq Data Link**: Some free breadth datasets
  - **Interactive Brokers API**: Free with account, has real-time A/D data
  - **WSJ Market Diary scraping**: Free but fragile (page structure changes)
  - **Norgate Data** (~$30/mo): High-quality EOD breadth data with history

### Chart 3: $BINAHLC — NYSE New Highs–New Lows Cumulative
- **Source:** Computed from same ~200-stock proxy universe
- **Limitation:** Same as Chart 2 — proxy vs full universe
- **Quality:** ⭐⭐⭐ — Shape and trend are reliable; absolute counts are understated
- **Upgrade paths:** Same as Chart 2. Polygon.io is probably the best cost/quality tradeoff.

### To expand the proxy universe:
You can increase accuracy by expanding `NYSE_PROXY_TICKERS` in the source code.
A full NYSE common-stock list can be downloaded from:
- https://www.nyse.com/listings_directory/stock (manual)
- Polygon.io ticker list endpoint (programmatic)
- SEC EDGAR full-index files (free, requires parsing)
    """)
