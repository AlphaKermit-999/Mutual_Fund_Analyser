""" 
This script is the single source of truth for creating and populating the database.
It fetches, processes, and saves all necessary mutual fund data.
"""

import pandas as pd
import requests
from io import StringIO
import logging
from sqlalchemy import create_engine
import sys
import os

# This line allows the script to import from the parent directory (e.g., config.py)
# This is a standard pattern for making scripts runnable from the command line.

sys.path.append(os.path.abspath(os.path.join(os.path_dirname(__file__), '..')))
from config import AMFI_NAV_URL, DB_PATH

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_amfi_data() -> str:
    """Extract: Fetches the raw text data from the AMFI URL."""
    try:
        logging.info(f"Fetching data from AMFI URL: {AMFI_NAV_URL}")
        response = requests.get(AMFI_NAV_URL, timeout=60)
        response.raise_for_status()
        logging.info("Successfully fetched data.")
        return response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"FATAL: HTTP Request failed: {e}")
        sys.exit(1)

def process_data(raw_data: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Transform: Processes the raw AMFI text data into structured DataFrames."""
    try:
        data_start_pos = raw_data.find("Scheme Code;")
        if data_start_pos == -1:
            logging.error("Fatal: 'Scheme Code;' header not found in the data.")
            sys.exit()
            
        
        data_io = StringIO(raw_data[data_start_pos:])
        
        
        df = pd.read_csv(data_io, sep=';', header=0)
        df.columns = ['scheme_code', 'isin_payput', 'isin_reinvestment', 'scheme_name', 'nav', 'repurchase_price', 'sale_price', 'date']
        
        df['scheme_code'] = pd.to_numeric(df['scheme_code'], errors='coerce') 
        df['nav'] = pd.to_numeric(df['nav'], errors='coerce')
        df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y', errors='coerce')
        df.dropna(subset=['scheme_code', 'nav', 'date', 'scheme_name'], inplace=True)
        df['scheme_code'] = df['scheme_code'].astype(int)
        df = df[df['nav'] > 0]
        
        df_metadata = df[['scheme_code', 'scheme_name']].drop_duplicates(subset=['scheme_code']).set_index('scheme_code')
        df_nav_history = df[['scheme_code', 'date', 'nav']]
        
        return df_metadata, df_nav_history
    

def save_to_database(df_metadata: pd.DataFrame, df_nav_history: pd.DataFrame):
    """Load: Saves the processed DataFrames into an SQLite database."""
    logging.info(f"Connecting to database at: {DB_PATH}")
    engine = create_engine(f'sqlite:///{DB_PATH}')
    
    df_metadata.to_sql('funds_metadata', engine, if_exists='replace', index=True)
    logging.info(f"Saved {len(df_metadata)} funds to `funds_metadata` table.")
    
    df_nav_history.to_sql('nav_history', engine, if_exists='replace', index=False, chunksize=10000)
    
    with engine.connect() as connection:
        connection.execute('CREATE INDEX IF NOT EXISTS idx_nav_scheme_code ON nav_history (scheme_code)')
        connection.execute('CREATE INDEX IF NOT EXISTS idx_nav_date ON nav_history (date)')
    logging.info(f"Saved {len(df_nav_history)} records to `nav_history` table and created indexes.")
    
    
if __name__ == '__main__':
    logging.info("--- Starting Database Build Process (Milestone 1) ---")
    raw_amfi_data = fetch_amfi_data()
    metadata, nav_history = process_data(raw_amfi_data)
    save_to_database(metadata, nav_history)
    logging.info("--- Database Build Process Complete ---")
    