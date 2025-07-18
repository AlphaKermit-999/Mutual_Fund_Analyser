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
    # fund_options = funds_df.set_index('scheme_code')['scheme_name']
    fund_options = funds_df.set_index('scheme_name')['scheme_code']
    print("fund_options", fund_options)
    selected_fund = st.selectbox("Select a Mutual Fund:", options=fund_options.index, index=None, placeholder="Search for a fund...")
    scheme_code = fund_options[selected_fund]
    if selected_fund:
        scheme_code = fund_options[fund_options == selected_fund].index[0]
        
        with st.spinner(f"Analyzing {selected_fund}..."):
            scorecard = generate_fund_scorecard(scheme_code)
            nav_history = get_nav_history_by_code(scheme_code)

        if "error" in scorecard:
            st.error(scorecard["error"])
        else:
            final_score = scorecard['scores']['final_score']
            st.subheader(f"Analysis for: {selected_fund}")
            
            col1, col2 = st.columns([1, 2])
            with col1:
                st.metric(label="Overall Fund Score", value=f"{final_score}/100")
                st.progress(final_score / 100)
                
                st.markdown("##### Key Metrics")
                metrics = scorecard['metrics']
                for metric, value in metrics.items():
                    if isinstance(value, float) and not pd.isna(value):
                        val_str = f"{value*100:.2f}%" if 'Year' in metric or 'Month' in metric or 'volatility' in metric else f"{value:.2f}"
                        st.text(f"{metric}: {val_str}")
            
            with col2:
                fig = px.line(nav_history, x=nav_history.index, y='nav', title='Historical NAV Performance', labels={'nav': 'Net Asset Value (INR)', 'date': 'Date'})
                st.plotly_chart(fig, use_container_width=True)