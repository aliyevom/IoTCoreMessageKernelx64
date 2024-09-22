import boto3
import json
from botocore.exceptions import ClientError
from device_sdk.core_data_extraction import DeviceCoreSDK
from config.s3_config import BUCKET_NAME, S3_KEY
from utils.logger import setup_logger

logger = setup_logger("CoreExtractAndStore")

def store_data_in_s3():

    sdk = DeviceCoreSDK(base_url="http://device-data-api.io/v1/resources")
    data = sdk.fetch_data()

    if data:
    
        s3_client = boto3.client('s3')
        try:
            logger.info(f"Uploading data to S3 bucket {BsUCKET_NAME}...")
            response = s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=S3_KEY,
                Body=json.dumps(data)
            )
            logger.info(f"Data successfully uploaded to S3: {BUCKET_NAME}/{S3_KEY}")
        except ClientError as e:
            logger.error(f"Error uploading data to S3: {e}")
    else:
        logger.error("No data fetched from SDK. Skipping S3 upload.")

# Configuration example
BUCKET_NAME = "sdk-signalIoT"
S3_KEY = "device_data.json"
