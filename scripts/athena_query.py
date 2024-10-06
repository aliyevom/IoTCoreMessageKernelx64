import time
import boto3
from botocore.exceptions import ClientError
from utils.logger import setup_logger

# Initialize logger
logger = setup_logger("AthenaQuery")

# Constants
DATABASE = 'iot_data_db'
OUTPUT_BUCKET = 's3://sdk-signalIoT/athena-results/'
MAX_RETRIES = 5
SLEEP_TIME = 5  # Wait time in seconds between query status checks
QUERY_TIMEOUT = 300  # Timeout for query execution in seconds

def run_athena_query():
    """
    Run an Athena query on the transformed IoT data stored in S3.
    Waits for query completion and handles errors and retries.
    """
    athena_client = boto3.client('athena')

    query = """
    SELECT * FROM iot_data_db.transformed_iot_data
    WHERE temperature > 25
    """

    try:
        # Start the Athena query execution
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': DATABASE
            },
            ResultConfiguration={
                'OutputLocation': OUTPUT_BUCKET
            }
        )

        # Extract query execution ID
        query_execution_id = response['QueryExecutionId']
        logger.info(f"Started Athena query with execution ID: {query_execution_id}")

        # Wait for the query to complete and monitor its status
        return wait_for_query_to_complete(athena_client, query_execution_id)

    except ClientError as e:
        logger.error(f"Error starting Athena query: {e}")
        raise

def wait_for_query_to_complete(athena_client, query_execution_id):
    """
    Waits for the Athena query to complete by checking its status periodically.
    Handles query retries and timeouts.
    """
    elapsed_time = 0
    for attempt in range(MAX_RETRIES):
        try:
            while elapsed_time < QUERY_TIMEOUT:
                # Get the query execution status
                response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = response['QueryExecution']['Status']['State']
                logger.info(f"Query execution status: {status}")

                # Check for terminal states (SUCCEEDED, FAILED, CANCELLED)
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break

                # Wait before checking again
                time.sleep(SLEEP_TIME)
                elapsed_time += SLEEP_TIME

            if status == 'SUCCEEDED':
                logger.info(f"Query completed successfully. Execution ID: {query_execution_id}")
                return query_execution_id
            elif status == 'FAILED':
                reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown reason')
                logger.error(f"Query failed: {reason}")
                return None
            elif status == 'CANCELLED':
                logger.warning(f"Query was cancelled. Execution ID: {query_execution_id}")
                return None
            else:
                logger.error(f"Query did not complete in time. Execution ID: {query_execution_id}")
                return None

        except ClientError as e:
            logger.error(f"Error fetching query execution status: {e}")
            if attempt < MAX_RETRIES - 1:
                logger.info(f"Retrying... (Attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(SLEEP_TIME)
            else:
                logger.error(f"Exceeded maximum retries for query execution status check.")
                raise

    logger.error(f"Query timed out after {QUERY_TIMEOUT} seconds.")
    return None
