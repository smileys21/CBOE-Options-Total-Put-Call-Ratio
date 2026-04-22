"""
Market Breadth Dashboard
========================
Replicates three StockCharts indicators:
  1. $CPC  — CBOE Total Put/Call Ratio (daily)
  2. $NAAD — NYSE Advance-Decline Cumulative Line (daily)
  3. $BINAHLC — NYSE New Highs–New Lows Cumulative (weekly)

Data sources:
  - Chart 1: CBOE free CSV (cdn.cboe.com) — official source
  - Charts 2 & 3: Computed from NYSE-listed stocks via yfinance
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

warnings.filterwarnings("ignore")

st.set_page_config(page_title="Market Breadth Dashboard", page_icon="📊", layout="wide")

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
    code, .stCode { font-family: 'IBM Plex Mono', monospace !important; }
    .block-container { padding-top: 1.5rem; }
    .metric-card {
        background: linear-gradient(135deg, #111827 0%, #1a1f2e 100%);
        border: 1px solid #1e293b; border-radius: 8px; padding: 16px 20px; margin-bottom: 8px;
    }
    .metric-label { font-size: 0.75rem; color: #64748b !important; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 4px; }
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


def metric_card(label, value, color_class="metric-neutral"):
    return f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value {color_class}">{value}</div></div>'


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR CONTROLS
# ══════════════════════════════════════════════════════════════════════════════

st.sidebar.markdown("## ⚙️ Dashboard Controls")
st.sidebar.markdown("---")

st.sidebar.markdown("### Chart 1: $CPC")
cpc_years = st.sidebar.selectbox("Lookback (years)", [1, 2, 3, 5], index=1, key="cpc_yr")
cpc_ma_short = st.sidebar.slider("Short MA (days)", 5, 30, 10, key="cpc_ma_s")
cpc_ma_long = st.sidebar.slider("Long MA (days)", 10, 60, 20, key="cpc_ma_l")
cpc_show_raw = st.sidebar.checkbox("Show raw daily ratio", value=True, key="cpc_raw")

st.sidebar.markdown("---")
st.sidebar.markdown("### Chart 2: $NAAD")
ad_years = st.sidebar.selectbox("Lookback (years) ", [0.5, 1, 2, 3], index=1, key="ad_yr")
ad_ma = st.sidebar.slider("MA period (days)", 10, 50, 20, key="ad_ma")

st.sidebar.markdown("---")
st.sidebar.markdown("### Chart 3: $BINAHLC")
hl_years = st.sidebar.selectbox("Lookback (years)  ", [2, 3, 5, 7], index=2, key="hl_yr")
hl_ma = st.sidebar.slider("MA period (weeks)", 5, 20, 10, key="hl_ma")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 1: CBOE Total Put/Call Ratio
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600 * 6, show_spinner=False)
def fetch_cboe_putcall():
    """
    Fetch CBOE Total Put/Call Ratio from BOTH CSV sources.
    Pick whichever has the most recent data point.
    """
    results = []

    # Source 1: Archive — most reliable, full history 1995-present
    try:
        r = requests.get(
            "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/pcratioarchive.csv",
            timeout=30,
        )
        r.raise_for_status()
        lines = r.text.strip().split("\n")
        header_idx = next(i for i, l in enumerate(lines) if l.strip().upper().startswith("DATE"))
        clean = "\n".join(lines[header_idx:])
        df = pd.read_csv(io.StringIO(clean))
        df.columns = [c.strip() for c in df.columns]

        date_col = [c for c in df.columns if "DATE" in c.upper()][0]
        # Look for total P/C ratio column
        ratio_col = [c for c in df.columns if "TOTAL" in c.upper() and ("P/C" in c or "RATIO" in c.upper())]
        if not ratio_col:
            ratio_col = [c for c in df.columns if "P/C" in c and "INDEX" not in c.upper() and "EQUITY" not in c.upper()]
        if not ratio_col:
            ratio_col = [c for c in df.columns if "P/C" in c]

        if ratio_col:
            df["Date"] = pd.to_datetime(df[date_col], format="mixed", dayfirst=False, errors="coerce")
            df["PC_Ratio"] = pd.to_numeric(df[ratio_col[0]], errors="coerce")
            df = df[["Date", "PC_Ratio"]].dropna().sort_values("Date").set_index("Date")
            if len(df) > 100:
                results.append(("pcratioarchive.csv", df))
    except Exception:
        pass

    # Source 2: Total PC — current data, 2006-present
    try:
        r = requests.get(
            "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/totalpc.csv",
            timeout=30,
        )
        r.raise_for_status()
        lines = r.text.strip().split("\n")
        header_idx = next(i for i, l in enumerate(lines) if l.strip().upper().startswith("DATE"))
        clean = "\n".join(lines[header_idx:])
        df = pd.read_csv(io.StringIO(clean))
        df.columns = [c.strip() for c in df.columns]

        date_col = [c for c in df.columns if "DATE" in c.upper()][0]
        ratio_col = [c for c in df.columns if "P/C" in c]

        if ratio_col:
            df["Date"] = pd.to_datetime(df[date_col], format="mixed", dayfirst=False, errors="coerce")
            df["PC_Ratio"] = pd.to_numeric(df[ratio_col[0]], errors="coerce")
            df = df[["Date", "PC_Ratio"]].dropna().sort_values("Date").set_index("Date")
            if len(df) > 100:
                results.append(("totalpc.csv", df))
    except Exception:
        pass

    if not results:
        return None, "No data fetched"

    # Pick whichever has the MOST RECENT data point
    results.sort(key=lambda x: x[1].index.max(), reverse=True)
    best_name, best_df = results[0]
    return best_df, best_name


def plot_cpc(df, years=2, ma_short=10, ma_long=20, show_raw=True):
    cutoff = df.index.max() - pd.DateOffset(years=years)
    df = df.loc[cutoff:].copy()
    df["MA_Short"] = df["PC_Ratio"].rolling(ma_short).mean()
    df["MA_Long"] = df["PC_Ratio"].rolling(ma_long).mean()

    fig = go.Figure()
    if show_raw:
        fig.add_trace(go.Scatter(
            x=df.index, y=df["PC_Ratio"], mode="lines", name="P/C Ratio",
            line=dict(color="#475569", width=1), opacity=0.4,
        ))
    fig.add_trace(go.Scatter(
        x=df.index, y=df["MA_Short"], mode="lines", name=f"{ma_short}-day MA",
        line=dict(color="#38bdf8", width=2),
    ))
    fig.add_trace(go.Scatter(
        x=df.index, y=df["MA_Long"], mode="lines", name=f"{ma_long}-day MA",
        line=dict(color="#f59e0b", width=1.5, dash="dot"),
    ))
    fig.add_hline(y=1.0, line_dash="dash", line_color="#334155", line_width=1,
                  annotation_text="1.0 (neutral)", annotation_font_color="#64748b")
    fig.add_hline(y=0.8, line_dash="dot", line_color="#22c55e", line_width=1,
                  annotation_text="0.8 (bullish extreme)", annotation_font_color="#22c55e",
                  annotation_position="bottom left")
    fig.add_hline(y=1.2, line_dash="dot", line_color="#ef4444", line_width=1,
                  annotation_text="1.2 (bearish extreme)", annotation_font_color="#ef4444",
                  annotation_position="top left")
    fig.update_layout(**PLOT_LAYOUT, title=dict(text="CBOE Total Put/Call Ratio ($CPC)",
                      font=dict(size=16, color="#e2e8f0")), yaxis_title="Put/Call Ratio", height=450)
    return fig, df


# ══════════════════════════════════════════════════════════════════════════════
# CHARTS 2 & 3: NYSE breadth from yfinance proxy
# ══════════════════════════════════════════════════════════════════════════════

NYSE_PROXY_TICKERS = [
    "AAPL","MSFT","GOOGL","AMZN","META","BRK-B","JPM","V","JNJ","WMT","PG","MA","HD","CVX",
    "MRK","ABBV","PEP","KO","COST","TMO","ABT","MCD","CSCO","DHR","WFC","ACN","LIN","NEE",
    "PM","TXN","UNP","RTX","HON","LOW","UPS","IBM","GE","CAT","BA","SPGI","AXP","ELV","PLD",
    "DE","MDLZ","BLK","ADI","CB","CI","SYK","MMC","ZTS","TGT","BDX","PNC","USB","ADP","CL",
    "MMM","DUK","SO","CME","ICE","TFC","FDX","NSC","EMR","ETN","APD","SHW","ITW","ECL","WM",
    "ROP","GD","PSA","CCI","D","AEP","XEL","ED","WEC","AWK","DTE","PPL","EIX","ES","FE",
    "AEE","LNT","CMS","NI","PNW","OGE","NRG","AES","EVR","EVRG",
    "F","GM","DAL","UAL","LUV","AAL","ALK",
    "XOM","COP","EOG","SLB","MPC","VLO","PSX","OXY","HAL",
    "GS","MS","C","BAC","SCHW","BK","STT","FITB","RF","KEY","CFG","HBAN","MTB","ZION","CMA","ALLY",
    "PFE","LLY","BMY","AMGN","GILD","VRTX","REGN","ZBH","BAX","STE","A","HOLX","IQV",
    "CRM","NOW","INTU","CDNS","SNPS","ANSS","KEYS",
    "T","VZ","TMUS","CMCSA",
    "NKE","SBUX","EL","CLX","GIS","K","SJM","HRL","MKC",
    "AMT","EQIX","SPG","O","WELL","DLR","VTR","ARE","BXP","SLG","VNO","KIM","REG","FRT",
    "DD","DOW","LYB","CE","EMN","IP","PKG","WRK","SEE","SON",
    "NUE","STLD","CLF","X","AA",
]

@st.cache_data(ttl=3600 * 6, show_spinner=False)
def fetch_proxy_data(lookback_years=7):
    end = datetime.now()
    start = end - timedelta(days=int(lookback_years * 365))
    try:
        return yf.download(NYSE_PROXY_TICKERS, start=start.strftime("%Y-%m-%d"),
                           end=end.strftime("%Y-%m-%d"), progress=False,
                           group_by="ticker", auto_adjust=True)
    except Exception as e:
        st.error(f"yfinance download failed: {e}")
        return None


def compute_ad_line(data):
    closes = pd.DataFrame()
    for t in NYSE_PROXY_TICKERS:
        try:
            closes[t] = data[(t, "Close")] if isinstance(data.columns, pd.MultiIndex) else data["Close"]
        except (KeyError, TypeError):
            continue
    if closes.empty:
        return None
    rets = closes.pct_change()
    advances = (rets > 0.0).sum(axis=1)
    declines = (rets < 0.0).sum(axis=1)
    net = advances - declines
    return pd.DataFrame({"Advances": advances, "Declines": declines, "Net": net, "AD_Line": net.cumsum()})


def compute_hilo_cumulative(data):
    highs, lows = pd.DataFrame(), pd.DataFrame()
    for t in NYSE_PROXY_TICKERS:
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
    rh = highs.rolling(252, min_periods=252).max()
    rl = lows.rolling(252, min_periods=252).min()
    nh = (highs >= rh).sum(axis=1)
    nl = (lows <= rl).sum(axis=1)
    net = nh - nl
    result = pd.DataFrame({"New_Highs": nh, "New_Lows": nl, "Net_HiLo": net, "Cum_HiLo": net.cumsum()}).dropna()
    return result.resample("W-FRI").last()


def plot_ad_line(df, years=1, ma_period=20):
    cutoff = df.index.max() - pd.DateOffset(years=years)
    df = df.loc[cutoff:].copy()
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.06)
    fig.add_trace(go.Scatter(x=df.index, y=df["AD_Line"], mode="lines", name="A/D Cumulative",
                             line=dict(color="#38bdf8", width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["AD_Line"].rolling(ma_period).mean(), mode="lines",
                             name=f"{ma_period}-day MA", line=dict(color="#f59e0b", width=1.5, dash="dot")), row=1, col=1)
    colors = ["#34d399" if v >= 0 else "#f87171" for v in df["Net"]]
    fig.add_trace(go.Bar(x=df.index, y=df["Net"], name="Net Advances", marker_color=colors, opacity=0.7), row=2, col=1)
    fig.update_layout(**PLOT_LAYOUT, title=dict(text="NYSE Advance-Decline Cumulative ($NAAD proxy)",
                      font=dict(size=16, color="#e2e8f0")), height=550, showlegend=True, barmode="relative")
    fig.update_yaxes(title_text="Cumulative A/D", row=1, col=1, gridcolor="#1e293b", zerolinecolor="#334155")
    fig.update_yaxes(title_text="Daily Net", row=2, col=1, gridcolor="#1e293b", zerolinecolor="#334155")
    fig.update_xaxes(gridcolor="#1e293b", row=1, col=1)
    fig.update_xaxes(gridcolor="#1e293b", row=2, col=1)
    return fig, df


def plot_hilo_cumulative(df, years=5, ma_period=10):
    cutoff = df.index.max() - pd.DateOffset(years=years)
    df = df.loc[cutoff:].copy()
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.06)
    fig.add_trace(go.Scatter(x=df.index, y=df["Cum_HiLo"], mode="lines", name="Cum NH-NL",
                             line=dict(color="#a78bfa", width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["Cum_HiLo"].rolling(ma_period).mean(), mode="lines",
                             name=f"{ma_period}-wk MA", line=dict(color="#f59e0b", width=1.5, dash="dot")), row=1, col=1)
    colors = ["#34d399" if v >= 0 else "#f87171" for v in df["Net_HiLo"]]
    fig.add_trace(go.Bar(x=df.index, y=df["Net_HiLo"], name="Weekly Net NH-NL", marker_color=colors, opacity=0.7), row=2, col=1)
    fig.update_layout(**PLOT_LAYOUT, title=dict(text="NYSE New Highs–New Lows Cumulative ($BINAHLC proxy)",
                      font=dict(size=16, color="#e2e8f0")), height=550, showlegend=True, barmode="relative")
    fig.update_yaxes(title_text="Cumulative NH-NL", row=1, col=1, gridcolor="#1e293b", zerolinecolor="#334155")
    fig.update_yaxes(title_text="Weekly Net", row=2, col=1, gridcolor="#1e293b", zerolinecolor="#334155")
    fig.update_xaxes(gridcolor="#1e293b", row=1, col=1)
    fig.update_xaxes(gridcolor="#1e293b", row=2, col=1)
    return fig, df


# ══════════════════════════════════════════════════════════════════════════════
# MAIN APP
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("# 📊 Market Breadth Dashboard")
st.markdown("*Replicating StockCharts $CPC · $NAAD · $BINAHLC with free data sources*")
st.divider()

# ── Chart 1 ──────────────────────────────────────────────────────────────────
st.markdown("## 1 · CBOE Total Put/Call Ratio")

with st.spinner("Fetching CBOE Put/Call data..."):
    cpc_df, cpc_source = fetch_cboe_putcall()

if cpc_df is not None and not cpc_df.empty:
    latest_date = cpc_df.index.max().strftime("%Y-%m-%d")
    st.caption(f"Source: **{cpc_source}** · Latest data: **{latest_date}**")

    fig_cpc, cpc_plot = plot_cpc(cpc_df, years=cpc_years, ma_short=cpc_ma_short,
                                  ma_long=cpc_ma_long, show_raw=cpc_show_raw)
    latest = cpc_plot["PC_Ratio"].iloc[-1]
    ma_s = cpc_plot["MA_Short"].iloc[-1]
    ma_l = cpc_plot["MA_Long"].iloc[-1]
    color = "metric-red" if latest > 1.0 else "metric-green"

    c1, c2, c3 = st.columns(3)
    c1.markdown(metric_card("Latest P/C Ratio", f"{latest:.2f}", color), unsafe_allow_html=True)
    c2.markdown(metric_card(f"{cpc_ma_short}-day MA", f"{ma_s:.2f}"), unsafe_allow_html=True)
    c3.markdown(metric_card(f"{cpc_ma_long}-day MA", f"{ma_l:.2f}"), unsafe_allow_html=True)

    st.plotly_chart(fig_cpc, use_container_width=True)
    st.markdown(f'<div class="source-note">Source: CBOE {cpc_source} · Updated EOD · '
                f'Readings &gt;1.0 = more puts (bearish sentiment) · &lt;0.8 = complacency zone</div>',
                unsafe_allow_html=True)
else:
    st.error("Could not fetch CBOE data. Check internet connection.")

st.divider()

# ── Chart 2 ──────────────────────────────────────────────────────────────────
st.markdown("## 2 · NYSE Advance-Decline Cumulative Line")
st.caption("Computed from ~200 NYSE-listed stocks. True $NAAD uses all ~3,000+ issues — "
           "this proxy captures the same directional trend with smaller magnitudes.")

with st.spinner("Downloading NYSE proxy data (first load may take 30-60 seconds)..."):
    proxy_data = fetch_proxy_data(lookback_years=max(float(hl_years) + 1.5, float(ad_years) + 1))

if proxy_data is not None:
    ad_df = compute_ad_line(proxy_data)
    if ad_df is not None and not ad_df.empty:
        fig_ad, ad_plot = plot_ad_line(ad_df, years=ad_years, ma_period=ad_ma)
        latest_ad = ad_plot["AD_Line"].iloc[-1]
        latest_net = ad_plot["Net"].iloc[-1]
        net_color = "metric-green" if latest_net >= 0 else "metric-red"
        trend_5d = (ad_plot["AD_Line"].iloc[-1] - ad_plot["AD_Line"].iloc[-6]) if len(ad_plot) > 5 else 0
        trend_color = "metric-green" if trend_5d >= 0 else "metric-red"

        c1, c2, c3 = st.columns(3)
        c1.markdown(metric_card("Cumulative A/D", f"{latest_ad:,.0f}"), unsafe_allow_html=True)
        c2.markdown(metric_card("Latest Daily Net", f"{latest_net:+.0f}", net_color), unsafe_allow_html=True)
        c3.markdown(metric_card("5-day Trend", f"{trend_5d:+,.0f}", trend_color), unsafe_allow_html=True)
        st.plotly_chart(fig_ad, use_container_width=True)
        st.markdown('<div class="source-note">Source: yfinance (~200 NYSE stocks) · Proxy for $NAAD · '
                    'Rising line = broad participation · Divergence from index = warning signal</div>',
                    unsafe_allow_html=True)

st.divider()

# ── Chart 3 ──────────────────────────────────────────────────────────────────
st.markdown("## 3 · NYSE New Highs–New Lows Cumulative")
st.caption("Weekly view, computed from ~200 NYSE stocks' 52-week highs & lows. Proxy for $BINAHLC.")

if proxy_data is not None:
    hilo_df = compute_hilo_cumulative(proxy_data)
    if hilo_df is not None and not hilo_df.empty:
        fig_hilo, hilo_plot = plot_hilo_cumulative(hilo_df, years=hl_years, ma_period=hl_ma)
        latest_cum = hilo_plot["Cum_HiLo"].iloc[-1]
        latest_net_hl = hilo_plot["Net_HiLo"].iloc[-1]
        hl_color = "metric-green" if latest_net_hl >= 0 else "metric-red"
        nh = hilo_plot["New_Highs"].iloc[-1]
        nl = hilo_plot["New_Lows"].iloc[-1]

        c1, c2, c3 = st.columns(3)
        c1.markdown(metric_card("Cumulative NH-NL", f"{latest_cum:,.0f}"), unsafe_allow_html=True)
        c2.markdown(metric_card("Latest Weekly Net", f"{latest_net_hl:+.0f}", hl_color), unsafe_allow_html=True)
        c3.markdown(metric_card("NH / NL (latest wk)", f"{nh:.0f} / {nl:.0f}"), unsafe_allow_html=True)
        st.plotly_chart(fig_hilo, use_container_width=True)
        st.markdown('<div class="source-note">Source: yfinance (~200 NYSE stocks, 52-wk rolling) · '
                    'Proxy for $BINAHLC · Rising = healthy breadth · Falling while index rises = bearish divergence</div>',
                    unsafe_allow_html=True)

st.divider()
with st.expander("📋 Data Sourcing Notes & Upgrade Paths"):
    st.markdown("""
### Chart 1: $CPC — CBOE Total Put/Call Ratio
- **Source:** Direct CSV from `cdn.cboe.com` — official CBOE data
- **Logic:** Fetches BOTH `pcratioarchive.csv` and `totalpc.csv`, uses whichever has the most recent date
- **Quality:** ⭐⭐⭐⭐⭐ — Same upstream source as StockCharts

### Charts 2 & 3: $NAAD / $BINAHLC
- **Source:** Computed from ~200 NYSE-listed stocks via yfinance
- **Limitation:** True indicators use all ~3,000+ NYSE securities — magnitudes will be smaller
- **Quality:** ⭐⭐⭐ — Directional trend is accurate
- **Upgrade paths:** Polygon.io ($29/mo), Interactive Brokers API (free w/ account), Norgate Data (~$30/mo)
""")
