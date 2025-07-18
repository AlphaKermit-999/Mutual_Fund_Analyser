""" 
This script is the single source of truth for creating and populating the database.
It fetches, processes, and saves all necessary mutual fund data.
"""

import pandas as pd
import requests
from io import StringIO
import logging
from sqlalchemy import create_engine, text
import sys
import os

# This line allows the script to import from the parent directory (e.g., config.py)
# This is a standard pattern for making scripts runnable from the command line.

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import AMFI_NAV_URL, DB_PATH

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_amfi_data() -> str:
    """Extract: Fetches the raw text data from the AMFI URL."""
    try:
        logging.info(f"Fetching data from AMFI URL: {AMFI_NAV_URL}")
        response = requests.get(AMFI_NAV_URL, timeout=120)
        response.raise_for_status()
        logging.info("Successfully fetched data.")
        return response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"FATAL: HTTP Request failed: {e}")
        sys.exit(1)
        
        
def process_data(raw_data: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transform: Processes the raw, messy AMFI text data using a resilient line-by-line approach.
    """
    try:
        processed_records = []
        # Split the entire text block into individual lines for processing
        lines = raw_data.strip().split('\n')
        logging.info(f"Processing {len(lines)} raw lines from source file.")

        for line in lines:
            # A valid data line must contain a semicolon. This is the first filter
            # to discard non-data lines like fund house headers and blank lines.
            if ';' not in line:
                continue

            fields = line.strip().split(';')

            # --- The Core Data Validation Contract ---
            # Based on our analysis of the live data, a valid record has exactly 6 fields.
            # This check is crucial for data quality and pipeline stability.
            if len(fields) == 6:
                # Create a structured dictionary from the validated fields.
                # We explicitly use the correct index for each piece of data.
                processed_records.append({
                    'scheme_code': fields[0],      # Index 0 is the Scheme Code
                    'scheme_name': fields[3],      # Index 2 is the Scheme Name
                    'nav':         fields[4],      # Index 4 is the Net Asset Value
                    'date':        fields[5]       # Index 5 is the Date
                })

        if not processed_records:
            logging.error("FATAL: No valid records could be processed from the source data. The file might be empty or in an unexpected format.")
            sys.exit(1)

        # Create a Pandas DataFrame from our list of clean, validated dictionaries
        df = pd.DataFrame(processed_records)
        logging.info(f"Successfully parsed {len(df)} valid data records.")

        # --- Data Cleaning and Type Conversion ---
        # Convert columns to their proper data types for analysis, coercing errors to NaN.
        df['scheme_code'] = pd.to_numeric(df['scheme_code'], errors='coerce')
        df['nav'] = pd.to_numeric(df['nav'], errors='coerce')
        df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y', errors='coerce')
        # Drop any row where our essential data is missing after conversion
        df.dropna(subset=['scheme_code', 'nav', 'date', 'scheme_name'], inplace=True)
        # Convert scheme_code to a proper integer
        df['scheme_code'] = df['scheme_code'].astype(int)
        # Filter out any funds with a zero or negative NAV, which are invalid for calculation
        df = df[df['nav'] > 0]
        logging.info(f"Records remaining after cleaning and type conversion: {len(df)}")

        # --- Separate into Final Tables ---
        # Metadata Table: A unique list of all funds
        df_metadata = df[['scheme_code', 'scheme_name']].drop_duplicates(subset=['scheme_code']).set_index('scheme_code')

        # Time-Series Table: The historical NAV data for all funds
        df_nav_history = df[['scheme_code', 'date', 'nav']]

        return df_metadata, df_nav_history
    
    except Exception as e:
        logging.error(f"Error processing AMFI data: {e}")
        sys.exit(1)
    

def save_to_database(df_metadata: pd.DataFrame, df_nav_history: pd.DataFrame):
    """Load: Saves the processed DataFrames into an SQLite database."""
    logging.info(f"Connecting to database at: {DB_PATH}")
    engine = create_engine(f'sqlite:///{DB_PATH}')
    
    df_metadata.to_sql('funds_metadata', engine, if_exists='replace', index=True)
    logging.info(f"Saved {len(df_metadata)} funds to `funds_metadata` table.")
    
    df_nav_history.to_sql('nav_history', engine, if_exists='replace', index=False, chunksize=10000)
    
    with engine.connect() as connection:
        logging.info("Creating indexes on nav_history table...")
        connection.execute(text('CREATE INDEX IF NOT EXISTS idx_nav_scheme_code ON nav_history (scheme_code)'))
        connection.execute(text('CREATE INDEX IF NOT EXISTS idx_nav_date ON nav_history (date)'))
    logging.info(f"Saved {len(df_nav_history)} records to `nav_history` table and created indexes.")
    
    
if __name__ == '__main__':
    logging.info("--- Starting Database Build Process (Milestone 1) ---")
    raw_amfi_data = fetch_amfi_data()
    metadata, nav_history = process_data(raw_amfi_data)
    save_to_database(metadata, nav_history)
    logging.info("--- Database Build Process Complete ---")
    