import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import logging
import boto3
import json
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv


# --- Configuration ---
# Configure logging for better debugging

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read configuration from the loaded environment variables
SECRET_NAME = os.environ.get("SECRET_NAME")
AWS_REGION = os.environ.get("AWS_REGION")

# --- Caching ---
# This simple in-memory cache prevents creating a new database connection for every single query.
_engine = None

def get_secret():
    """Retrieves database credentials from AWS Secrets Manager."""
    if not SECRET_NAME or not AWS_REGION:
        raise ValueError("Configuration error: SECRET_NAME and AWS_REGION must be set in your .env file.")

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=AWS_REGION)
    try:
        logging.info(f"Retrieving secret '{SECRET_NAME}' from AWS Secrets Manager in region {AWS_REGION}.")
        get_secret_value_response = client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(get_secret_value_response['SecretString'])
        logging.info("Successfully retrieved secret.")
        return secret
    except ClientError as e:
        logging.error(f"Failed to retrieve secret '{SECRET_NAME}': {e}")
        raise e

def get_db_engine():
    """
    Creates and returns a SQLAlchemy engine for the RDS PostgreSQL database.
    Uses a cached engine to avoid reconnecting on every call.
    """
    global _engine
    if _engine is not None:
        return _engine

    try:
        db_creds = get_secret()
        # Construct the database URL for PostgreSQL using the fetched credentials
        db_url = (
            f"postgresql+psycopg2://{db_creds['username']}:{db_creds['password']}"
            f"@{db_creds['host']}:{db_creds['port']}/{db_creds['dbname']}"
        )
        _engine = create_engine(db_url)
        logging.info(f"Successfully created database engine for host: {db_creds['host']}")
        return _engine
    except Exception as e:
        logging.error(f"Failed to create database engine: {e}")
        return None


def get_all_fund_data() -> pd.DataFrame:
    """Retrieves a DataFrame of all fund names and their unique scheme codes from RDS."""
    engine = get_db_engine()
    if engine is None:
        return pd.DataFrame()
    
    try:
        query = "SELECT scheme_code, scheme_name FROM funds_metadata"
        return pd.read_sql(query, engine)
    except SQLAlchemyError as e:
        logging.error(f"Database error fetching all fund data: {e}")
        return pd.DataFrame()
    

def get_nav_history_by_code(scheme_code: int) -> pd.DataFrame:
    """Retrieves the full NAV history for a single fund from RDS, indexed by date."""
    engine = get_db_engine()
    if engine is None:
        return pd.DataFrame()

    try:
        # Use a parameterized query for security
        query = "SELECT date, nav FROM nav_history WHERE scheme_code = %(code)s"
        
        df = pd.read_sql(
            query, 
            engine, 
            params={"code": scheme_code}, 
            parse_dates=['date'], 
            index_col='date'
        )
        return df.sort_index()
    except SQLAlchemyError as e:
        logging.error(f"Database error for scheme_code {scheme_code}: {e}")
        return pd.DataFrame()