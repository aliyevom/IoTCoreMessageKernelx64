import boto3
from utils.logger import setup_logger

logger = setup_logger("AthenaQuery")

def run_athena_query():
    """
    Run an Athena query on the transformed data stored in S3.
    """
    athena_client = boto3.client('athena')
    
    query = """
    SELECT * FROM iot_data_db.transformed_iot_data
    WHERE temperature > 25
    """
    
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'iot_data_db'
        },
        ResultConfiguration={
            'OutputLocation': 's3://sdk-signalIoT/athena-results/'
        }
    )
    
    logger.info(f"Started Athena query with execution ID: {response['QueryExecutionId']}")
    return response['QueryExecutionId']
