import boto3
import json
import time
from device_sdk.core_data_extraction import DeviceCoreSDK
from utils.logger import setup_logger

logger = setup_logger("KinesisProducer")

def send_data_to_kinesis():
    """
    Fetch data from the IoT Device SDK and send it to the Kinesis Data Stream.
    """
    # Initialize Device SDK
    sdk = DeviceCoreSDK(base_url="http://device-data-api.io/v1/resources")

    # Initialize Kinesis client
    kinesis_client = boto3.client('kinesis', region_name='us-east-1')
    
    # Stream name
    stream_name = "IoTDataStream"
    
    while True:
        # Fetch data from the IoT Device
        data = sdk.fetch_data()
        
        if data:
            try:
                # Send data to Kinesis stream
                response = kinesis_client.put_record(
                    StreamName=stream_name,
                    Data=json.dumps(data),
                    PartitionKey="device_id"  # Could be device ID or timestamp
                )
                logger.info(f"Data sent to Kinesis Stream: {response['ShardId']}")
            except Exception as e:
                logger.error(f"Error sending data to Kinesis: {e}")
        
        # Wait for 5 seconds before sending the next batch of data
        time.sleep(5)
