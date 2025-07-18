import streamlit as st

# Configure the page at the very top, only once per app run
st.set_page_config(
    page_title="AI Mutual Fund Analyzer",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("📈 AI Mutual Fund Analyzer")
st.markdown("""
Welcome to the AI Mutual Fund Analyzer, a tool designed to combine traditional financial analysis with the power of Generative AI.

**This application has two main features:**

1.  **📊 Fund Screener & Scorecard:**
    A classic analytical tool. Select a mutual fund to view its detailed performance metrics, historical data, and a quantitative "Fund Scorecard".

2.  **🤖 AI Analyst Chat:**
    A conversational Generative AI assistant. Ask questions in natural language about any Indian mutual fund using a Retrieval-Augmented Generation (RAG) system.

**👈 Select a feature from the sidebar to get started!**

---
*Disclaimer: This is an educational AI project and does not constitute financial advice.*
""")