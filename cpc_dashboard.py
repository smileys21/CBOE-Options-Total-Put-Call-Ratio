"""
OCC Data Diagnostic
===================
Fetches a single day from OCC and shows raw response so we can debug the parser.
"""

import streamlit as st
import requests
import gzip
import io
from datetime import datetime, timedelta

st.set_page_config(page_title="OCC Diagnostic", layout="wide")
st.title("🔍 OCC Data Diagnostic")
st.markdown("Fetching a single day from OCC to inspect the raw response format.")

test_date = st.date_input("Test date", value=datetime(2026, 4, 17))
date_str = test_date.strftime("%Y%m%d")

url = f"https://marketdata.theocc.com/daily-volume-statistics?reportDate={date_str}&format=csv"
st.code(url, language=None)

if st.button("Fetch & Inspect"):
    with st.spinner("Fetching..."):
        try:
            r = requests.get(url, timeout=30)

            st.subheader("Response Metadata")
            st.write(f"**Status code:** {r.status_code}")
            st.write(f"**Content-Type:** {r.headers.get('content-type', 'not set')}")
            st.write(f"**Content-Encoding:** {r.headers.get('content-encoding', 'not set')}")
            st.write(f"**Content-Length:** {r.headers.get('content-length', 'not set')}")
            st.write(f"**Raw bytes length:** {len(r.content)}")

            # Show first 20 raw bytes (hex) to detect gzip/binary
            raw_hex = r.content[:20].hex()
            st.write(f"**First 20 bytes (hex):** `{raw_hex}`")

            is_gzip = r.content[:2] == b'\x1f\x8b'
            st.write(f"**Looks like gzip?** {'✅ Yes' if is_gzip else '❌ No'}")

            # Try to decode
            text = None

            if is_gzip:
                st.subheader("Decompressing gzip...")
                try:
                    text = gzip.decompress(r.content).decode("utf-8", errors="replace")
                    st.success(f"Decompressed OK — {len(text)} characters")
                except Exception as e:
                    st.error(f"Gzip decompression failed: {e}")

            if text is None:
                st.subheader("Trying as plain text...")
                try:
                    text = r.content.decode("utf-8", errors="replace")
                    st.success(f"Decoded as UTF-8 — {len(text)} characters")
                except Exception as e:
                    st.error(f"UTF-8 decode failed: {e}")
                    try:
                        text = r.content.decode("latin-1", errors="replace")
                        st.success(f"Decoded as Latin-1 — {len(text)} characters")
                    except Exception as e2:
                        st.error(f"Latin-1 decode also failed: {e2}")

            if text:
                st.subheader("First 3000 characters of response")
                st.code(text[:3000], language=None)

                st.subheader("Line count & structure")
                lines = text.strip().split("\n")
                st.write(f"**Total lines:** {len(lines)}")
                st.write("**First 30 lines:**")
                for i, line in enumerate(lines[:30]):
                    st.text(f"  [{i}] {line[:200]}")

                # Try to find put/call columns
                st.subheader("Searching for put/call keywords...")
                for i, line in enumerate(lines):
                    if "put" in line.lower() or "call" in line.lower():
                        st.text(f"  Line {i}: {line[:200]}")
                        if i < len(lines) - 1:
                            st.text(f"  Line {i+1}: {lines[i+1][:200]}")
                        break
            else:
                st.error("Could not decode response at all.")

        except requests.exceptions.ConnectionError as e:
            st.error(f"Connection failed: {e}")
        except requests.exceptions.Timeout:
            st.error("Request timed out after 30 seconds")
        except Exception as e:
            st.error(f"Unexpected error: {type(e).__name__}: {e}")
