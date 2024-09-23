from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from scripts.state_change_handler import handle_device_state_change

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['admin@example.com'],
}


with DAG('device_state_management_pipeline',
         default_args=default_args,
         schedule_interval='@hourly',
         catchup=False) as dag:

    state_change_task = PythonOperator(
        task_id='update_and_get_device_state',
        python_callable=handle_device_state_change,
        retries=2,
        retry_delay=timedelta(minutes=3)
    )

    state_change_task
