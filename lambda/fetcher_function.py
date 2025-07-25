import requests
import logging
import os
import boto3
from datetime import datetime

# Configure logging for AWS CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# These environment variables will be configured in the AWS Lambda console
S3_LANDING_BUCKET = os.environ.get("S3_LANDING_BUCKET")
AMFI_NAV_URL = os.environ.get("AMFI_NAV_URL")

def lambda_handler(event, context):
    """
    This public-facing Lambda's only job is to fetch the AMFI data
    and drop it into the S3 Landing Zone.
    """
    logger.info("Fetcher function triggered.")
    if not S3_LANDING_BUCKET or not AMFI_NAV_URL:
        logger.error("FATAL: Environment variables S3_LANDING_BUCKET or AMFI_NAV_URL are not set.")
        raise ValueError("Missing required environment variables.")
    
    try:
        logger.info(f"Fetching data from: {AMFI_NAV_URL}")
        response = requests.get(AMFI_NAV_URL, timeout=60)
        response.raise_for_status()
        raw_data = response.text
        
        s3_client = boto3.client('s3')
        # Use a timestamp for the S3 key to ensure uniqueness
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        s3_key = f"amfi_nav_all_{timestamp}.txt"
        
        s3_client.put_object(Bucket=S3_LANDING_BUCKET, Key=s3_key, Body=raw_data)
        logger.info(f"Successfully placed raw data in Landing Zone: s3://{S3_LANDING_BUCKET}/{s3_key}")
        
        return {'statusCode': 200, 'body': f'File {s3_key} placed in landing zone.'}
    except Exception as e:
        logger.error(f"An error occurred in the fetcher: {e}", exc_info=True)
        raise e # Reraise the exception to mark the Lambda execution as failed