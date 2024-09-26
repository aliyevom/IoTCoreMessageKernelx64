from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from scripts.core_extract_and_store import store_data_in_s3

# Default arguments for the Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['infodev@qbee.io'], # Group Partner lifecycle opt in 
}

# DAG definition for IoT Core Data Pipeline
with DAG('iot_core_data_pipeline','iot_core_pipeline_with_kinesis',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    extract_and_store_task = PythonOperator(
        task_id='extract_and_store_data',
        python_callable=store_data_in_s3,
        retries=2,
        retry_delay=timedelta(minutes=3)
    )

    athena_query_task = PythonOperator(
        task_id='run_athena_query',
        python_callable=run_athena_query,
        retries=2,
        retry_delay=timedelta(minutes=3)
    )
    kinesis_producer_task = PythonOperator(
        task_id='send_data_to_kinesis',
        python_callable=send_data_to_kinesis,
        retries=2,
        retry_delay=timedelta(minutes=3)
    )
    
