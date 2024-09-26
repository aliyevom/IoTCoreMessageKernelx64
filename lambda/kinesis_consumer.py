import json
import boto3
from utils.logger import setup_logger

logger = setup_logger("KinesisConsumer")

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Lambda function to process IoT telemetry data from Kinesis stream.
    """
    bucket_name = 'sdk-signalIoT'
    
    # Loop over each Kinesis record
    for record in event['Records']:
        payload = json.loads(record['kinesis']['data'])
        logger.info(f"Processing record: {payload}")
        
        # Example: Store data in S3
        device_id = payload.get('device_id', 'unknown_device')
        s3_key = f"real-time/{device_id}/{context.aws_request_id}.json"
        
        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=json.dumps(payload)
            )
            logger.info(f"Successfully stored data in S3: {s3_key}")
        except Exception as e:
            logger.error(f"Error storing data in S3: {e}")
