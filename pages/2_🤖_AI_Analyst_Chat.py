import streamlit as st
import sys, os
import time

# Add project root to path for correct imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Import our new, powerful LangChain-based response function
from core.genai_engine import get_rag_response

st.set_page_config(layout="centered", page_title="AI Analyst Chat")
st.title("🤖 AI Analyst Chat")
st.caption("Powered by Google Gemini & LangChain")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "assistant", "content": "Hi! I'm FinBot. How can I help you analyze a mutual fund today?"}]

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# React to user input
if prompt := st.chat_input("e.g., How did the Quant Small Cap fund do last year?"):
    st.chat_message("user").markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("assistant"):
        with st.spinner("FinBot is thinking..."):
            # Call our new, robust backend function
            response = get_rag_response(prompt)
            st.markdown(response)
    
    st.session_state.messages.append({"role": "assistant", "content": response})