# It handles the UI for the deterministic scorecard analysis.
import streamlit as st
import pandas as pd
import plotly.express as px
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.database import get_all_fund_data, get_nav_history_by_code
from core.analysis_engine import generate_fund_scorecard

st.set_page_config(layout="wide", page_title="Fund Screener & Scorecard")
st.title("📊 Fund Screener & Scorecard")
st.markdown("Select a fund from the dropdown below to see its detailed analytical scorecard and performance history.")

@st.cache_data(ttl="1d")
def load_data():
    return get_all_fund_data()

funds_df = load_data()

if funds_df.empty:
    st.error("Could not load fund data. Please run the `scripts/build_database.py` script first.")
else:
    # Code for the selectbox and displaying the scorecard and chart...
    fund_options = funds_df.set_index('scheme_code')['scheme_name']
    selected_fund = st.selectbox("Select a Mutual Fund:", options=fund_options, index=None, placeholder="Search for a fund...")
    if selected_fund:
        # ... (rest of the code is identical)
        pass