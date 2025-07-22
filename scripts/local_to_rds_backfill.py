import pandas as pd
from sqlalchemy import create_engine
import os
import boto3
import json
import logging
from botocore.exceptions import ClientError
import sys

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
    logging.info("--- Starting one-time backfill from local SQLite to AWS RDS ---")

    try:
        local_engine = create_engine(f'sqlite:///{LOCAL_SQLITE_PATH}')
        logging.info(f"Reading historical data from {LOCAL_SQLITE_PATH}...")
        historical_df = pd.read_sql_table('nav_history', local_engine)
        if historical_df.empty:
            raise ValueError("Local database is empty. Nothing to backfill. Run the local build_database.py first.")
        logging.info(f"Successfully loaded {len(historical_df)} records from local database.")
    except Exception as e:
        logging.error(f"FATAL: Could not read from local SQLite database. Error: {e}")
        sys.exit(1)

    try:
        db_creds = get_secret(SECRET_NAME)
        rds_engine = get_rds_engine(db_creds)
        logging.info(f"Successfully connected to RDS instance at {db_creds['host']}.")
    except Exception as e:
        logging.error(f"FATAL: Could not connect to RDS. Ensure your IP is allow-listed in the security group. Error: {e}")
        sys.exit(1)

    logging.info(f"Writing {len(historical_df)} records to the 'nav_history' table in RDS. This may take a few minutes...")
    try:
        historical_df.to_sql('nav_history', rds_engine, if_exists='replace', index=False, chunksize=1000)
        logging.info("--- Backfill complete! Your RDS database is now populated with historical data. ---")
    except Exception as e:
        logging.error(f"FATAL: Failed to write data to RDS. Error: {e}")