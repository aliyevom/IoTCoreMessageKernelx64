import json
import boto3
import zlib
from botocore.exceptions import ClientError
from utils.logger import setup_logger

logger = setup_logger("KinesisConsumer")

# Initialize S3 client
s3_client = boto3.client('s3')

# Constants
BUCKET_NAME = 'sdk-signalIoT'
MAX_RETRIES = 3  # Maximum number of retries for storing data in S3
COMPRESSION_ENABLED = True  # Set to True if you want to compress the payload

def lambda_handler(event, context):
    """
    Lambda function to process IoT telemetry data from Kinesis stream and store it in S3.
    """
    # Loop over each Kinesis record
    for record in event['Records']:
        try:
            # Extract the payload from Kinesis and decode the data
            payload = json.loads(record['kinesis']['data'])
            logger.info(f"Processing record: {payload}")
            
            # Validate the payload
            if not validate_payload(payload):
                logger.warning(f"Invalid payload detected: {payload}")
                continue
            
            # Store the data in S3
            store_data_in_s3(payload, context)

        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error parsing Kinesis record: {e}")
        except Exception as e:
            logger.error(f"Unexpected error processing Kinesis record: {e}")

def store_data_in_s3(payload, context):
    """
    Stores the processed payload in S3, implementing retries for resiliency.
    """
    device_id = payload.get('device_id', 'unknown_device')
    s3_key = f"real-time/{device_id}/{context.aws_request_id}.json"
    
    # Optionally compress the payload before storing in S3
    payload_data = json.dumps(payload)
    if COMPRESSION_ENABLED:
        payload_data = zlib.compress(payload_data.encode('utf-8'))
        s3_key = s3_key.replace(".json", ".json.gz")

    # Attempt to store the object in S3 with retries
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=payload_data,
                ContentType="application/json",
                ContentEncoding="gzip" if COMPRESSION_ENABLED else None
            )
            logger.info(f"Successfully stored data in S3: {s3_key}")
            break
        except ClientError as e:
            attempts += 1
            logger.error(f"Error storing data in S3 (attempt {attempts}): {e}")
            if attempts >= MAX_RETRIES:
                logger.error(f"Failed to store data in S3 after {MAX_RETRIES} attempts: {s3_key}")

def validate_payload(payload):
    """
    Validates the incoming payload to ensure it has the required fields.
    Returns True if valid, False otherwise.
    """
    required_fields = ['device_id', 'timestamp', 'sensor_data']
    if all(field in payload for field in required_fields):
        return True
    return False
