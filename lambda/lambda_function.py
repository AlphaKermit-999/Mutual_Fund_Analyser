import pandas as pd
import requests
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

from config import AMFI_NAV_URL

# --- Configuration ---
# Configure logging to work with AWS CloudWatch Logs
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# These environment variables will be configured in the Lambda settings in Milestone 3
SECRET_NAME = os.environ.get("SECRET_NAME")
S3_LANDING_BUCKET = os.environ.get("S3_LANDING_BUCKET")
S3_RAW_BUCKET = os.environ.get("S3_RAW_BUCKET")

# --- Functions ---

def get_secret(secret_name):
    """Retrieves database credentials from AWS Secrets Manager."""
    if not secret_name:
        raise ValueError("SECRET_NAME environment variable not set.")
    
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')
    logger.info(f"Retrieving secret '{secret_name}' from Secrets Manager.")
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'])
        logger.info("Successfully retrieved secret.")
        return secret
    except ClientError as e:
        logger.error(f"Failed to retrieve secret '{secret_name}': {e}")
        raise e

def get_db_engine(db_credentials):
    """Creates a SQLAlchemy engine for our RDS PostgreSQL database."""
    try:
        db_url = f"postgresql+psycopg2://{db_credentials['username']}:{db_credentials['password']}@{db_credentials['host']}:{db_credentials['port']}/{db_credentials['dbname']}"
        engine = create_engine(db_url)
        logger.info("Database engine created successfully.")
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise e

def process_daily_data(raw_data: str) -> pd.DataFrame:
    """Processes the raw AMFI text data into a clean DataFrame."""
    logger.info("Processing raw data...")
    processed_records = []
    lines = raw_data.strip().split('\n')
    for line in lines:
        if ';' not in line: continue
        fields = line.strip().split(';')
        if len(fields) == 6:
            processed_records.append({
                'scheme_code': fields[0], 'scheme_name': fields[3],
                'nav': fields[4], 'date': fields[5]
            })
    
    if not processed_records: return pd.DataFrame()

    df = pd.DataFrame(processed_records)
    df['scheme_code'] = pd.to_numeric(df['scheme_code'], errors='coerce')
    df['nav'] = pd.to_numeric(df['nav'], errors='coerce')
    df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y', errors='coerce')
    df.dropna(inplace=True)
    df['scheme_code'] = df['scheme_code'].astype(int)
    df = df[df['nav'] > 0]
    return df

def validate_raw_data(raw_data: str, sample_size=100, conformity_threshold=0.9) -> bool:
    """
    Performs a sampling validation on the raw data to check its structure.
    Checks the first `sample_size` lines and ensures that at least `conformity_threshold`
    of the data lines have the expected 6 columns.
    """
    logger.info(f"Performing structural validation on raw data...")
    lines = raw_data.strip().split('\n')
    sample_lines = lines[:sample_size]
    
    data_lines_checked = 0
    valid_lines_found = 0

    if not sample_lines:
        logger.error("Validation failed: The data file is empty.")
        return False

    for line in sample_lines:
        if ';' in line:
            data_lines_checked += 1
            fields = line.strip().split(';')
            if len(fields) == 6:
                valid_lines_found += 1

    if data_lines_checked == 0:
        logger.error("Validation failed: No data lines (containing ';') found in the sample.")
        return False
        
    conformity_score = valid_lines_found / data_lines_checked
    logger.info(f"Validation check: {valid_lines_found}/{data_lines_checked} sample lines are valid. Conformity: {conformity_score:.2%}")

    if conformity_score < conformity_threshold:
        logger.error(f"Validation failed: Data conformity ({conformity_score:.2%}) is below the threshold of {conformity_threshold:.2%}.")
        return False
        
    logger.info("Validation successful: Data structure conforms to expectations.")
    return True

def lambda_handler(event, context):
    """
    The main handler function that AWS Lambda will execute.
    This orchestrates the entire daily ETL process.
    """
    try:
        # --- EXTRACT ---
        logger.info(f"Step 1: Fetching data from {AMFI_NAV_URL}...")
        response = requests.get(AMFI_NAV_URL, timeout=60)
        response.raise_for_status()
        raw_data = response.text
        
        # --- ARCHIVE TO LANDING ZONE ---
        s3_client = boto3.client('s3')
        today_str = datetime.now().strftime('%Y-%m-%d')
        s3_key = f"amfi_nav_all_{today_str}.txt"
        s3_client.put_object(Bucket=S3_LANDING_BUCKET, Key=s3_key, Body=raw_data)
        logger.info(f"Successfully placed raw data in Landing Zone: s3://{S3_LANDING_BUCKET}/{s3_key}")
        
        # --- THE NEW VALIDATION STEP ---
        if not validate_raw_data(raw_data):
            # If validation fails, we stop here. The bad file remains in the landing zone
            # for manual inspection, and the pipeline does not proceed.
            raise ValueError("Structural validation of the raw data file failed.")
        
        # --- MOVE TO RAW ZONE ---
        copy_source = {'Bucket': S3_LANDING_BUCKET, 'Key': s3_key}
        s3_client.copy_object(CopySource=copy_source, Bucket=S3_RAW_BUCKET, Key=s3_key)
        s3_client.delete_object(Bucket=S3_LANDING_BUCKET, Key=s3_key)
        logger.info(f"Validated and moved raw data to Raw Zone: s3://{S3_RAW_BUCKET}/{s3_key}")
        
        # --- TRANSFORM ---
        logger.info("Step 2: Processing daily data...")
        daily_df = process_daily_data(raw_data)
        if daily_df.empty:
            logger.warning("No data processed today. Exiting successfully.")
            return {'statusCode': 200, 'body': json.dumps('No data processed.')}
        
        # --- LOAD ---
        logger.info("Step 3: Loading data into RDS Data Warehouse...")
        db_creds = get_secret(SECRET_NAME)
        engine = get_db_engine(db_creds)
        
        metadata_table = 'funds_metadata'
        nav_history_table = 'nav_history'

        # 3a. Update Metadata: Overwrite to catch name changes
        metadata_df = daily_df[['scheme_code', 'scheme_name']].drop_duplicates('scheme_code').set_index('scheme_code')
        metadata_df.to_sql(metadata_table, engine, if_exists='replace', index=True)
        logger.info(f"Upserted {len(metadata_df)} records into '{metadata_table}'.")

        # 3b. Accumulate Historical NAVs
        with engine.connect() as conn:
            conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {nav_history_table} (
                scheme_code INTEGER,
                date DATE,
                nav FLOAT,
                PRIMARY KEY (scheme_code, date)
            );
            """))
            daily_df.to_sql('temp_nav', conn, if_exists='replace', index=False)
            conn.execute(text(f"""
            INSERT INTO {nav_history_table} (scheme_code, date, nav)
            SELECT scheme_code, date, nav FROM temp_nav
            ON CONFLICT (scheme_code, date) DO UPDATE SET nav = EXCLUDED.nav;
            """))
            conn.commit()
        
        logger.info(f"Successfully upserted {len(daily_df)} NAV records into '{nav_history_table}'.")

        return {'statusCode': 200, 'body': json.dumps('Data pipeline executed successfully!')}

    except Exception as e:
        logger.error(f"A critical error occurred during pipeline execution: {e}", exc_info=True)
        return {'statusCode': 500, 'body': json.dumps(f"An error occurred: {str(e)}")}