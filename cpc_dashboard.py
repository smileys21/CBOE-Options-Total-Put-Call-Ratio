"""
CBOE Put/Call Ratio ($CPC) — Debug Build
========================================
Shows step-by-step what OCC returns so we can fix parsing issues.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import requests
import re
import os
import time
from datetime import datetime, timedelta

st.set_page_config(page_title="$CPC Put/Call Ratio", page_icon="📊", layout="wide")
st.title("📊 CBOE Put/Call Ratio ($CPC)")

# ── Step 1: Test a single OCC fetch ──────────────────────────────────────────

st.subheader("Step 1: Test OCC fetch")

# Find most recent Friday
today = datetime.now()
days_since_friday = (today.weekday() - 4) % 7
last_friday = today - timedelta(days=days_since_friday)
if days_since_friday == 0 and today.hour < 18:
    last_friday = today - timedelta(days=7)

date_str = last_friday.strftime("%Y%m%d")
url = f"https://marketdata.theocc.com/weekly-volume-reports?reportDate={date_str}&reportType=options&reportClass=equity&format=csv"

st.write(f"Testing Friday: **{last_friday.strftime('%Y-%m-%d')}**")
st.code(url, language=None)

try:
    r = requests.get(url, timeout=20)
    st.write(f"Status: **{r.status_code}** | Size: **{len(r.content)} bytes**")

    if r.status_code == 200 and len(r.content) > 100:
        text = r.content.decode("utf-8", errors="replace")
        lines = text.strip().split("\n")
        st.write(f"Total lines: **{len(lines)}**")

        # Show all lines containing "TOTAL"
        st.write("**Lines containing 'TOTAL':**")
        total_calls = None
        total_puts = None
        for i, line in enumerate(lines):
            if "TOTAL" in line.upper():
                st.text(f"  [{i}] {line[:200]}")

                # Try to parse
                parts = line.split(",")
                label = parts[0].strip().upper() if parts else ""

                if "TOTAL CALLS" in label or "TOTAL CALL" in label:
                    try:
                        total_calls = int(parts[1].strip().strip('"').replace(",", ""))
                        st.write(f"  → Parsed TOTAL CALLS = **{total_calls:,}**")
                    except:
                        st.write(f"  → Failed to parse calls from: `{parts[1] if len(parts)>1 else 'N/A'}`")

                if "TOTAL PUTS" in label or "TOTAL PUT" in label:
                    try:
                        total_puts = int(parts[1].strip().strip('"').replace(",", ""))
                        st.write(f"  → Parsed TOTAL PUTS = **{total_puts:,}**")
                    except:
                        st.write(f"  → Failed to parse puts from: `{parts[1] if len(parts)>1 else 'N/A'}`")

        if total_calls and total_puts:
            ratio = total_puts / total_calls
            st.success(f"✅ P/C Ratio = {total_puts:,} / {total_calls:,} = **{ratio:.4f}**")
        else:
            st.error(f"❌ Could not find both totals. Calls={total_calls}, Puts={total_puts}")
            st.write("**Full response for debugging:**")
            st.code(text[:5000], language=None)
    else:
        st.error(f"Bad response. Trying previous Friday...")
        # Try the Friday before
        prev_friday = last_friday - timedelta(days=7)
        date_str2 = prev_friday.strftime("%Y%m%d")
        url2 = f"https://marketdata.theocc.com/weekly-volume-reports?reportDate={date_str2}&reportType=options&reportClass=equity&format=csv"
        r2 = requests.get(url2, timeout=20)
        st.write(f"Previous Friday {prev_friday.strftime('%Y-%m-%d')}: Status={r2.status_code}, Size={len(r2.content)}")
        if len(r2.content) > 100:
            text2 = r2.content.decode("utf-8", errors="replace")
            st.code(text2[:3000], language=None)

except Exception as e:
    st.error(f"Request failed: {type(e).__name__}: {e}")

st.divider()

# ── Step 2: If parsing works, build full dataset ─────────────────────────────

st.subheader("Step 2: Build full dataset (click when Step 1 looks good)")

CACHE_DIR = os.path.join(os.path.expanduser("~"), ".cpc_cache")
os.makedirs(CACHE_DIR, exist_ok=True)
CACHE_FILE = os.path.join(CACHE_DIR, "cpc_weekly.csv")

years = st.selectbox("Lookback", [1, 2, 3, 5], index=1)


def parse_weekly(text):
    total_calls = total_puts = None
    for line in text.split("\n"):
        parts = line.split(",")
        label = parts[0].strip().upper() if parts else ""
        if len(parts) >= 2:
            val_str = parts[1].strip().strip('"').replace(",", "")
            if "TOTAL CALLS" in label or "TOTAL CALL" in label:
                try:
                    total_calls = int(val_str)
                except:
                    pass
            if "TOTAL PUTS" in label or "TOTAL PUT" in label:
                try:
                    total_puts = int(val_str)
                except:
                    pass
    return total_calls, total_puts


def get_week_date(text):
    m = re.search(r"Week Ending\s+(\d{2}/\d{2}/\d{4})", text)
    return datetime.strptime(m.group(1), "%m/%d/%Y") if m else None


if st.button("🚀 Build Dataset"):
    end = datetime.now()
    start = end - timedelta(days=int(years * 365) + 30)

    # Generate Fridays
    d = start
    while d.weekday() != 4:
        d += timedelta(days=1)
    fridays = []
    while d <= end:
        fridays.append(d)
        d += timedelta(days=7)

    st.write(f"Fetching {len(fridays)} weeks...")
    progress = st.progress(0)
    rows = []
    fails = 0
    errors_log = []

    for i, friday in enumerate(fridays):
        progress.progress((i + 1) / len(fridays),
                          text=f"{friday.strftime('%Y-%m-%d')} ({i+1}/{len(fridays)})")

        ds = friday.strftime("%Y%m%d")
        url = f"https://marketdata.theocc.com/weekly-volume-reports?reportDate={ds}&reportType=options&reportClass=equity&format=csv"

        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 200 and len(r.content) > 100:
                text = r.content.decode("utf-8", errors="replace")
                calls, puts = parse_weekly(text)
                week_date = get_week_date(text) or friday

                if calls and puts and calls > 0:
                    rows.append({"Date": pd.Timestamp(week_date),
                                 "PC_Ratio": puts / calls,
                                 "Puts": puts, "Calls": calls})
                    fails = 0
                else:
                    fails += 1
                    errors_log.append(f"{friday.strftime('%Y-%m-%d')}: parsed calls={calls}, puts={puts}")
            else:
                fails += 1
                errors_log.append(f"{friday.strftime('%Y-%m-%d')}: status={r.status_code}, size={len(r.content)}")
        except Exception as e:
            fails += 1
            errors_log.append(f"{friday.strftime('%Y-%m-%d')}: {type(e).__name__}")

        if fails > 12:
            st.warning("Too many consecutive failures, stopping early.")
            break

        if i % 3 == 0:
            time.sleep(0.3)

    progress.empty()

    if rows:
        df = pd.DataFrame(rows).set_index("Date").sort_index()
        df = df[(df["PC_Ratio"] > 0.2) & (df["PC_Ratio"] < 3.0)]
        df.to_csv(CACHE_FILE)

        st.success(f"✅ Got {len(df)} weeks of data ({df.index.min().strftime('%Y-%m-%d')} to {df.index.max().strftime('%Y-%m-%d')})")

        # Plot
        ma4 = df["PC_Ratio"].rolling(4).mean()
        ma10 = df["PC_Ratio"].rolling(10).mean()

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df.index, y=df["PC_Ratio"], mode="lines",
                                 name="Weekly P/C", line=dict(color="#475569", width=1), opacity=0.5))
        fig.add_trace(go.Scatter(x=df.index, y=ma4, mode="lines",
                                 name="4-wk MA", line=dict(color="#38bdf8", width=2)))
        fig.add_trace(go.Scatter(x=df.index, y=ma10, mode="lines",
                                 name="10-wk MA", line=dict(color="#f59e0b", width=1.5, dash="dot")))
        fig.add_hline(y=1.0, line_dash="dash", line_color="#334155")
        fig.add_hline(y=0.8, line_dash="dot", line_color="#22c55e", line_width=1)
        fig.add_hline(y=1.2, line_dash="dot", line_color="#ef4444", line_width=1)
        fig.update_layout(template="plotly_dark", paper_bgcolor="#0a0e17", plot_bgcolor="#0f1420",
                          height=500, title="CBOE Total Put/Call Ratio ($CPC · weekly from OCC)")
        st.plotly_chart(fig, use_container_width=True)

        # Show last 10 data points
        st.write("**Last 10 data points:**")
        st.dataframe(df.tail(10)[["PC_Ratio", "Puts", "Calls"]].style.format({
            "PC_Ratio": "{:.4f}", "Puts": "{:,.0f}", "Calls": "{:,.0f}"}))
    else:
        st.error("No data parsed successfully.")

    if errors_log:
        with st.expander(f"⚠️ {len(errors_log)} errors"):
            for e in errors_log[:30]:
                st.text(e)
