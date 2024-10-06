import boto3
import json
import time
import random
from device_sdk.core_data_extraction import DeviceCoreSDK
from utils.logger import setup_logger
from botocore.exceptions import ClientError

logger = setup_logger("KinesisProducer")

# Initialize AWS SDK clients
kinesis_client = boto3.client('kinesis', region_name='us-east-1')

# Constants
STREAM_NAME = "IoTDataStream"
MAX_RETRIES = 5  # Max retries for sending data
DELAY_BETWEEN_RETRIES = 2  # Initial delay in seconds for retry, will use exponential backoff
BATCH_SIZE = 10  # Number of records to batch for Kinesis
DATA_SEND_DELAY = 5  # Delay between sending each batch in seconds

def send_data_to_kinesis():
    """
    Fetch data from the IoT Device SDK and send it to the Kinesis Data Stream in batches.
    Implements batch processing and error handling with retries.
    """
    # Initialize Device SDK
    sdk = DeviceCoreSDK(base_url="http://device-data-api.io/v1/resources")
    
    while True:
        try:
            # Fetch data from the IoT device in batches
            batch_data = []
            for _ in range(BATCH_SIZE):
                data = sdk.fetch_data()
                
                if not validate_data(data):
                    logger.warning("Invalid data received. Skipping...")
                    continue

                # Prepare data for Kinesis
                partition_key = determine_partition_key(data)
                record = {
                    'Data': json.dumps(data),
                    'PartitionKey': partition_key
                }
                batch_data.append(record)

            if batch_data:
                # Send the batch to Kinesis
                send_batch_to_kinesis(batch_data)

            # Wait before fetching the next batch
            time.sleep(DATA_SEND_DELAY)

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(DATA_SEND_DELAY)

def send_batch_to_kinesis(batch_data):
    """
    Sends a batch of data to Kinesis, implementing retries with exponential backoff.
    """
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            # Send batch data to Kinesis
            response = kinesis_client.put_records(
                Records=batch_data,
                StreamName=STREAM_NAME
            )
            failed_records = response.get('FailedRecordCount', 0)
            if failed_records == 0:
                logger.info(f"Successfully sent {len(batch_data)} records to Kinesis.")
                break
            else:
                logger.warning(f"{failed_records} records failed to send. Retrying...")
                handle_failed_records(response, batch_data)

        except ClientError as e:
            attempts += 1
            delay = DELAY_BETWEEN_RETRIES * (2 ** attempts) + random.uniform(0, 1)
            logger.error(f"Client error: {e}. Retrying in {delay:.2f} seconds...")
            time.sleep(delay)
        except Exception as e:
            logger.error(f"Error sending batch to Kinesis: {e}")
            break

def handle_failed_records(response, batch_data):
    """
    Extract and handle failed records from the Kinesis response.
    Re-attempt sending only the failed records.
    """
    failed_records = []
    for idx, record in enumerate(response['Records']):
        if 'ErrorCode' in record:
            logger.error(f"Record {idx} failed: {record['ErrorCode']} - {record['ErrorMessage']}")
            failed_records.append(batch_data[idx])

    if failed_records:
        logger.info(f"Retrying {len(failed_records)} failed records...")
        send_batch_to_kinesis(failed_records)

def determine_partition_key(data):
    """
    Determine the partition key based on device data.
    Use device_id or a timestamp to ensure even distribution across shards.
    """
    device_id = data.get('device_id')
    if device_id:
        return device_id
    # Fallback to using timestamp if device_id is not present
    return str(int(time.time()))

def validate_data(data):
    """
    Basic validation for the fetched IoT data.
    Ensures data has the necessary fields and structure before sending to Kinesis.
    """
    if not isinstance(data, dict):
        return False
    required_fields = ['device_id', 'timestamp', 'sensor_readings']
    return all(field in data for field in required_fields)

if __name__ == "__main__":
    send_data_to_kinesis()
