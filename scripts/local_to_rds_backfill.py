import pandas as pd
from sqlalchemy import create_engine, text
import os
import boto3
import json
import logging
from botocore.exceptions import ClientError
import sys

# Add project root to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import DB_PATH as LOCAL_SQLITE_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
SECRET_NAME = "prod/MutualFundsDB/Credentials"

def get_secret(secret_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret
    except ClientError as e:
        logging.error(f"Failed to retrieve secret '{secret_name}': {e}")
        raise e

def get_rds_engine(db_credentials):
    try:
        db_url = f"postgresql+psycopg2://{db_credentials['username']}:{db_credentials['password']}@{db_credentials['host']}:{db_credentials['port']}/{db_credentials['dbname']}"
        return create_engine(db_url)
    except Exception as e:
        logging.error(f"Failed to create RDS database engine: {e}")
        raise e

if __name__ == '__main__':
    logging.info("--- Starting COMPLETE one-time backfill from local SQLite to AWS RDS ---")

    # --- Step 1: Read BOTH tables from the local SQLite database ---
    try:
        local_engine = create_engine(f'sqlite:///{LOCAL_SQLITE_PATH}')
        logging.info(f"Reading data from local database: {LOCAL_SQLITE_PATH}...")
        
        # Read the NAV history
        historical_df = pd.read_sql_table('nav_history', local_engine)
        if historical_df.empty:
            raise ValueError("Local `nav_history` table is empty. Nothing to backfill.")
        logging.info(f"Successfully loaded {len(historical_df)} records from `nav_history`.")
        
        # --- THE FIX: Read the metadata table ---
        metadata_df = pd.read_sql_table('funds_metadata', local_engine)
        if metadata_df.empty:
            raise ValueError("Local `funds_metadata` table is empty. Nothing to backfill.")
        logging.info(f"Successfully loaded {len(metadata_df)} records from `funds_metadata`.")

    except Exception as e:
        logging.error(f"FATAL: Could not read from local SQLite database. Error: {e}")
        sys.exit(1)

    # --- Step 2: Connect to the remote AWS RDS PostgreSQL database ---
    logging.info("Connecting to AWS RDS...")
    try:
        db_creds = get_secret(SECRET_NAME)
        rds_engine = get_rds_engine(db_creds)
        logging.info(f"Successfully connected to RDS instance at {db_creds['host']}.")
    except Exception as e:
        logging.error(f"FATAL: Could not connect to RDS. Ensure your IP is allow-listed. Error: {e}")
        sys.exit(1)

    # --- Step 3: Write BOTH tables to the RDS database ---
    try:
        # Write the metadata table first
        logging.info(f"Writing {len(metadata_df)} metadata records to RDS...")
        metadata_df.to_sql('funds_metadata', rds_engine, if_exists='replace', index=False)
        # Set the primary key in PostgreSQL for the metadata table
        with rds_engine.connect() as connection:
            connection.execute(text('ALTER TABLE funds_metadata ADD PRIMARY KEY (scheme_code);'))
            connection.commit()
        logging.info("Successfully wrote `funds_metadata` table and set primary key.")

        # Write the historical NAV data
        logging.info(f"Writing {len(historical_df)} NAV records to RDS. This may take a few minutes...")
        historical_df.to_sql('nav_history', rds_engine, if_exists='replace', index=False, chunksize=1000)
        # Create indexes for performance on the history table
        with rds_engine.connect() as connection:
            connection.execute(text('CREATE INDEX IF NOT EXISTS idx_nav_scheme_code ON nav_history (scheme_code);'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS idx_nav_date ON nav_history (date);'))
            connection.commit()
        logging.info("Successfully wrote `nav_history` table and created indexes.")
        
        logging.info("--- Backfill complete! Your RDS database is now fully populated. ---")
    except Exception as e:
        logging.error(f"FATAL: Failed to write data to RDS. Error: {e}")