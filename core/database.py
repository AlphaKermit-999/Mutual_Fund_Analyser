import pandas as pd
from sqlalchemy import create_engine
import logging
from config import DB_PATH

try:
    engine = create_engine(f'sqlite:///{DB_PATH}')
except Exception as e:
    logging.error(f"Failed to create database engine: {e}")
    engine = None

def get_all_fund_data() -> pd.DataFrame:
    """
    Retrieves a DataFrame with all known mutual fund data from the database.

    The DataFrame will contain two columns: 'scheme_code' and 'scheme_name'.

    :return: A DataFrame with all known mutual fund data.
    """
    if not engine: return pd.DataFrame()
    try:
        return pd.read_sql("SELECT scheme_code, scheme_name FROM funds_metadata", engine)
    except Exception: return pd.DataFrame()

def get_nav_history_by_code(scheme_code: int) -> pd.DataFrame:
    """
    Retrieves a DataFrame with the NAV history of a mutual fund from the database.

    The DataFrame will contain two columns: 'date' and 'nav'.

    :param scheme_code: The scheme code of the mutual fund.
    :return: A DataFrame with the NAV history of the mutual fund.
    """
    if not engine: return pd.DataFrame()
    try:
        return pd.read_sql(f"SELECT date, nav FROM nav_history WHERE scheme_code = {scheme_code}", engine, parse_dates=['date'], index_col='date').sort_index()
    except Exception: return pd.DataFrame()