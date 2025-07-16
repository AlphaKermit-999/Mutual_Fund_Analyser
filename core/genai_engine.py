import os
import logging
from dotenv import load_dotenv
from fuzzywuzzy import process
import pandas as pd

# ---Local Imports---
from .database import get_all_fund_data
from .analysis_engine import generate_fund_scorecard

# ---LangChain Imports---
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

# Load API Key from .env file
load_dotenv()
if not os.getenv("GOOGLE_API_KEY"):
    raise ValueError("GOOGLE_API_KEY not found in .env file. Please add it.")

# --- The Custom Retriever ---
# This part of the RAG pattern is the custom logic. It retrieves the context.
def get_context_for_query(user_query: str) -> tuple[str, str | None]:
    """
    Retrieves the best-matching fund and its scorecard data.
    Returns a tuple of (matched_fund_name, context_string).
    Returns (None, error_message) if no suitable fund is found.
    """
    all_funds = get_all_fund_data()
    if all_funds.empty:
        return None, "The fund database is currently empty. Please run the data pipeline."

    best_match = process.extractOne(user_query, all_funds['scheme_name'].tolist())
    
    if not best_match or best_match[1] < 75:
        return None, "I couldn't find a specific fund matching your query. Please be more specific with the fund name."

    matched_fund_name = best_match[0]
    matched_scheme_code = all_funds[all_funds['scheme_name'] == matched_fund_name]['scheme_code'].iloc[0]

    scorecard = generate_fund_scorecard(matched_scheme_code)
    if 'error' in scorecard:
        return None, f"I found the fund '{matched_fund_name}', but an error occurred: {scorecard['error']}"

    # Format the context string for the LLM
    context_str = f"Fund Name: {matched_fund_name}\n"
    context_str += f"Overall Score: {scorecard['scores']['final_score']}/100\n"
    for metric, value in scorecard['metrics'].items():
        if isinstance(value, float) and not pd.isna(value):
            val_str = f"{value*100:.2f}%" if 'Year' in metric or 'Month' in metric or 'volatility' in metric else f"{value:.2f}"
            context_str += f"{metric}: {val_str}\n"
    
    return matched_fund_name, context_str

# --- The LangChain Orchestration ---
# Defining the prompt, model, and parser as modular components.

# The Prompt Template: A clear set of instructions for the LLM
prompt_template = ChatPromptTemplate.from_template(
    """
    You are an expert Indian Mutual Fund Analyst. Your name is "FinBot".
    Your task is to answer the user's question in a helpful and friendly tone based *only* on the factual data provided in the context below.
    Do not make up any information, predictions, or recommendations. If the context does not contain the answer, say "I'm sorry, but I don't have that specific information in my current data."

    **CONTEXT:**
    {context}

    **USER'S QUESTION:**
    {question}

    **YOUR RESPONSE:**
    """
)

# The Model: An instance of the Gemini Pro model, configured for our use case.
# Temperature controls creativity. Lower is better for factual tasks.
model = ChatGoogleGenerativeAI(model="gemini-pro", temperature=0.3)

# The Output Parser: Converts the LLM's complex output object into a simple string.
output_parser = StrOutputParser()

# The Chain: This is the core of LangChain. It links the components together
# using the LangChain Expression Language (LCEL). It's a "pipeline".
rag_chain = prompt_template | model | output_parser

# --- Step 3: The Public-Facing Function ---
# This is the single function our UI will call.
def get_rag_response(user_query: str) -> str:
    """
    Orchestrates the RAG process and returns a final response string.
    """
    matched_fund, context = get_context_for_query(user_query)
    
    if not context:
        # If get_context_for_query returned an error message
        return matched_fund if matched_fund is None else context

    try:
        # Invoke the chain with the retrieved context and the original question.
        response = rag_chain.invoke({"context": context, "question": user_query})
        return response
    except Exception as e:
        logging.error(f"An error occurred while invoking the RAG chain: {e}")
        return "I'm sorry, an unexpected error occurred while processing your request. Please try again later."