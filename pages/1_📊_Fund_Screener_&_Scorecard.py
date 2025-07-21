# It handles the UI for the deterministic scorecard analysis.
import streamlit as st
import pandas as pd
import plotly.express as px
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.database import get_all_fund_data, get_nav_history_by_code
from core.analysis_engine import generate_fund_scorecard

st.set_page_config(
    layout="wide", 
    page_title="Fund Screener & Scorecard")
st.title("📊 Fund Screener & Scorecard")
st.markdown("Select a fund from the dropdown below to see its detailed analytical scorecard and performance history.")

# --- Data Loading with Caching ---
@st.cache_data(ttl="1d")
def load_data():
    """
    Loads all fund data into a DataFrame with caching for 1 day.
    
    Returns:
        pd.DataFrame: A DataFrame with 'scheme_code' and 'scheme_name' columns.
    """
    return get_all_fund_data()

funds_df = load_data()

if funds_df.empty:
    st.error("Could not load fund data. Please run the `scripts/build_database.py` script first.")
else:
    # Code for the selectbox and displaying the scorecard and chart...
    fund_name_options = funds_df['scheme_name'].sort_values().tolist()
    selected_fund_name = st.selectbox(
        "Select a Mutual Fund:", 
        options=fund_name_options, 
        index=None, 
        placeholder="Search for a fund..."
        )
    
    # This block of code only runs after the user has made a selection.
    if selected_fund_name:
        # Retrieve the unique scheme_code for the selected fund name
        scheme_code_from_df = funds_df[funds_df['scheme_name'] == selected_fund_name]['scheme_code'].iloc[0]
        scheme_code = int(scheme_code_from_df)
        print("scheme_code", scheme_code)
        
        # Show a spinner while performing the analysis for a better user experience
        with st.spinner(f"Crunching the numbers for {selected_fund_name}..."):
            scorecard = generate_fund_scorecard(scheme_code)
            nav_history = get_nav_history_by_code(scheme_code)

        # --- Display Results ---
        if "error" in scorecard:
            st.error(scorecard["error"])
        else:
            final_score = scorecard['scores']['final_score']
            st.subheader(f"Analysis for: {selected_fund_name}")
            
            # Use columns for a clean, dashboard-like layout
            col1, col2 = st.columns([1, 2])
            with col1:
                # Display the primary score prominently
                st.metric(label="Overall Fund Score", value=f"{final_score}/100")
                st.progress(final_score / 100)
                
                st.markdown("---")
                st.markdown("##### Key Metrics")
                
                metrics = scorecard['metrics']
                for metric, value in metrics.items():
                    if isinstance(value, float) and not pd.isna(value):
                        # Format percentages and ratios correctly for readability
                        val_str = f"{value*100:.2f}%" if 'Year' in metric or 'Month' in metric or 'volatility' in metric else f"{value:.2f}"
                        st.text(f"{metric}: {val_str}")
            
            with col2:
                # Create and display an interactive plot of the fund's history
                fig = px.line(
                    nav_history, 
                    x=nav_history.index, 
                    y='nav', 
                    title='Historical NAV Performance', 
                    labels={'nav': 'Net Asset Value (INR)', 'date': 'Date'})
                st.plotly_chart(fig, use_container_width=True)