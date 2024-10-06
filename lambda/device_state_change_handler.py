import boto3
import json
from utils.logger import setup_logger

logger = setup_logger("DeviceStateChangeLambda")

def lambda_handler(event, context):
  
    # Log the incoming event
    logger.info(f"Received event: {json.dumps(event)}")

    # Example logic: send a notification if temperature > 30°C
    state = event.get('detail', {}).get('state', {})
    temperature = state.get('reported', {}).get('temperature', 0)
    
    if temperature > 30:
        logger.warning(f"Temperature exceeds threshold: {temperature}°C")
        send_notification(f"High temperature alert: {temperature}°C")

def send_notification(message):
  
    sns_client = boto3.client('sns')
    sns_client.publish(
        TopicArn='arn:aws:sns:us-east-1:211551251112:IoTAlerts',
        Message=message,
        Subject="IoT Device Alert"
    )
    logger.info("Notification sent.")
