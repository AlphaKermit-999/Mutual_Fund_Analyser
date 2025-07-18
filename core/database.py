import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import logging
from config import DB_PATH

# Set up logging for this module
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_engine() -> Engine | None:
    """
    Creates and returns a SQLAlchemy engine.
    This is a helper function to ensure a valid engine is created.
    """
    try:
        # The 'text' function isn't needed here, but it's good practice for other queries
        engine = create_engine(f'sqlite:///{DB_PATH}')
        return engine
    except ImportError:
        logging.error("SQLAlchemy library not found. Please install it with 'pip install SQLAlchemy'.")
        return None
    except Exception as e:
        logging.error(f"Failed to create database engine for path {DB_PATH}: {e}")
        return None

engine = get_db_engine()

def get_all_fund_data() -> pd.DataFrame:
    """
    Retrieves a DataFrame of all fund names and their unique scheme codes.
    This is used to populate search boxes and for fuzzy matching.

    Returns:
        pd.DataFrame: A DataFrame with 'scheme_code' and 'scheme_name' columns, or an empty DataFrame on error.
    """
    if not isinstance(engine, Engine):
        logging.error("Database engine is not available. Cannot fetch fund data.")
        return pd.DataFrame()
    
    try:
        query = "SELECT scheme_code, scheme_name FROM funds_metadata"
        return pd.read_sql(query, engine)
    except SQLAlchemyError as e:
        logging.error(f"A database error occurred while fetching all fund data: {e}")
        return pd.DataFrame()

def get_nav_history_by_code(scheme_code: int) -> pd.DataFrame:
    """
    Retrieves the full NAV history for a single fund, indexed by date.

    Args:
        scheme_code (int): The unique scheme code of the fund.

    Returns:
        pd.DataFrame: A time-series DataFrame with 'nav' values, or an empty DataFrame on error.
    """
    if not isinstance(engine, Engine):
        logging.error("Database engine is not available. Cannot fetch NAV history.")
        return pd.DataFrame()

    try:
        # --- REFINEMENT 1: Parameterized Query for Security ---
        # We use a placeholder (`:code`) in the query and pass the actual value
        # via the `params` argument. This prevents SQL injection.
        query = text("SELECT date, nav FROM nav_history WHERE scheme_code = :code")
        
        df = pd.read_sql(
            query, 
            engine, 
            params={"code": scheme_code}, 
            parse_dates=['date'], 
            index_col='date'
        )
        return df.sort_index()
    except SQLAlchemyError as e:
        # --- REFINEMENT 2: Specific Exception Handling & Logging ---
        # We catch a specific database error and log it for easier debugging.
        logging.error(f"A database error occurred for scheme_code {scheme_code}: {e}")
        return pd.DataFrame()