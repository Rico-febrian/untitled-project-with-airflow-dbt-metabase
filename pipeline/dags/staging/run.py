import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from staging.tasks.extract import extract_to_minio

default_args = {
    'owner' : 'Rcof',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}

local_tz = pendulum.timezone("Asia/Jakarta")

with DAG(
    dag_id = 'staging',
    default_args = default_args,
    start_date = datetime(2025, 12, 27, tzinfo=local_tz),
    schedule_interval = '@hourly',
    catchup = False,
    tags = ['staging', 'open-weather-pipeline']
) as dag:
    
    extract_task = PythonOperator(
        task_id = 'extract_to_minio',
        python_callable = extract_to_minio,
        provide_context = True
    )