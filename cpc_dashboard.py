"""
OCC Data Diagnostic v2
======================
Tests multiple OCC endpoints to find one with put/call breakdown.
"""

import streamlit as st
import requests
from datetime import datetime

st.set_page_config(page_title="OCC Diagnostic v2", layout="wide")
st.title("🔍 OCC Endpoint Scanner")

test_date = st.date_input("Test date", value=datetime(2026, 4, 17))
d_slash = test_date.strftime("%m/%d/%Y")    # 04/17/2026
d_compact = test_date.strftime("%Y%m%d")    # 20260417

# All OCC endpoints that might have put/call data
endpoints = {
    "1. Exchange Volume (txt)": f"https://marketdata.theocc.com/exchange-volume?reportView=regular&reportType=D&reportDate={d_compact}&instrumentType=options&format=txt",
    "2. Exchange Volume (csv)": f"https://marketdata.theocc.com/exchange-volume?reportView=regular&reportType=D&reportDate={d_compact}&instrumentType=options&format=csv",
    "3. Daily Volume Stats": f"https://marketdata.theocc.com/daily-volume-statistics?reportDate={d_compact}&format=csv",
    "4. Historical Volume Stats": f"https://marketdata.theocc.com/historical-volume-statistics?reportDate={d_compact}&format=csv",
    "5. Monthly Volume (equity options)": f"https://marketdata.theocc.com/monthly-volume-reports?reportDate={d_compact}&reportType=options&reportClass=equity&format=csv",
    "6. Weekly Volume (equity options)": f"https://marketdata.theocc.com/weekly-volume-reports?reportDate={d_compact}&reportType=options&reportClass=equity&format=csv",
    "7. Volume by Account Type": f"https://marketdata.theocc.com/volume-by-account-type?reportDate={d_compact}&format=csv",
}

if st.button("🚀 Test All Endpoints"):
    for name, url in endpoints.items():
        st.subheader(name)
        st.code(url, language=None)
        try:
            r = requests.get(url, timeout=20)
            st.write(f"Status: **{r.status_code}** | Content-Type: `{r.headers.get('content-type', '?')}` | Size: {len(r.content)} bytes")
            
            if r.status_code == 200 and len(r.content) > 50:
                # Try to decode
                try:
                    text = r.content.decode("utf-8", errors="replace")
                except:
                    text = r.content.decode("latin-1", errors="replace")
                
                has_pc = "put" in text.lower() or "call" in text.lower()
                st.write(f"Contains 'put' or 'call': **{'✅ YES' if has_pc else '❌ No'}**")
                
                st.text("First 1500 chars:")
                st.code(text[:1500], language=None)
            else:
                st.warning(f"Empty or error response")
        except Exception as e:
            st.error(f"{type(e).__name__}: {e}")
        st.divider()
