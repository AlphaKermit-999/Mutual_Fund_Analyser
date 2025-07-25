import pandas as pd
import logging
from sqlalchemy import create_engine, text
import os
import json
import boto3
import urllib.parse
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SECRET_NAME = os.environ.get("SECRET_NAME")
#Helper function to get DB credentials from Secret Manager
def get_secret(secret_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret
    except ClientError as e:
        logger.error(f"Failed to retrieve secret '{secret_name}': {e}")
        raise e
    
#Helper function to create a database connection engine
def get_db_engine(db_credentials):
    try:
        db_url = f"postgresql+psycopg2://{db_credentials['username']}:{db_credentials['password']}@{db_credentials['host']}:{db_credentials['port']}/{db_credentials['dbname']}"
        return create_engine(db_url)
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise e
    
#Helper function to process the raw data text
def process_data(raw_data: str) -> pd.DataFrame:
    try:
        processed_records = []
        lines = raw_data.strip().split('\n')
        for line in lines:
            if ';' not in line:
                continue
            fields = line.strip().split(';')
            if len(fields) == 6:
                processed_records.append({
                    'scheme_code': fields[0],
                    'scheme_name': fields[3],
                    'nav': fields[4],
                    'date': fields[5]
                })
                
        if not processed_records:
            return pd.DataFrame()
        
        df = pd.DataFrame(processed_records)
        df['scheme_code'] = pd.to_numeric(df['scheme_code'], errors='coerce')
        df['nav'] = pd.to_numeric(df['nav'], errors='coerce')
        df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y', errors='coerce')
        df.dropna(inplace=True)
        df['scheme_code'] = df['scheme_code'].astype(int)
        df = df[df['nav'] > 0]
        return df
    except Exception as e:
        logger.error(f"Failed to process data: {e}")
        raise e
    

def lambda_handler(event, context):
    logger.info(f"Processor function triggered by event.")
    try:
        # 1. Get the bucket and key from the S3 trigger event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        logger.info(f"Processing file s3://{bucket}/{key}")
        
        # 2. EXTRACT the file from S3
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        raw_data = response['Body'].read().decode('utf-8')

        # 3. TRANSFORM
        daily_df = process_data(raw_data)
        if daily_df.empty:
            logger.warning("No data processed from the file. Exiting.")
            return {'statusCode': 200, 'body': 'No data processed.'}

        # 4. LOAD
        db_creds = get_secret(SECRET_NAME)
        engine = get_db_engine(db_creds)
        
        metadata_table = 'funds_metadata'
        nav_history_table = 'nav_history'

        with engine.connect() as conn:
            # Create tables if they don't exist
            conn.execute(text(f"CREATE TABLE IF NOT EXISTS {metadata_table} (scheme_code INTEGER PRIMARY KEY, scheme_name VARCHAR(255));"))
            conn.execute(text(f"CREATE TABLE IF NOT EXISTS {nav_history_table} (scheme_code INTEGER, date DATE, nav FLOAT, PRIMARY KEY (scheme_code, date));"))
            
            # Upsert metadata
            metadata_df = daily_df[['scheme_code', 'scheme_name']].drop_duplicates('scheme_code').set_index('scheme_code')
            metadata_df.to_sql('temp_metadata', conn, if_exists='replace', index=True)
            conn.execute(text(f"INSERT INTO {metadata_table} SELECT * FROM temp_metadata ON CONFLICT (scheme_code) DO UPDATE SET scheme_name = EXCLUDED.scheme_name;"))

            # Upsert NAV history
            daily_df[['scheme_code', 'date', 'nav']].to_sql('temp_nav', conn, if_exists='replace', index=False)
            conn.execute(text(f"INSERT INTO {nav_history_table} SELECT * FROM temp_nav ON CONFLICT (scheme_code, date) DO UPDATE SET nav = EXCLUDED.nav;"))
            conn.commit()
        
        logger.info(f"Successfully upserted {len(daily_df)} records.")
        return {'statusCode': 200, 'body': 'Processor executed successfully.'}
    except Exception as e:
        logger.error(f"An error occurred in the processor: {e}", exc_info=True)
        raise e