from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig

from extract import extract_to_minio
from load import load_to_staging

default_args = {
    "owner": "rico",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# dbt config for Cosmos
profile_config = ProfileConfig(
    profile_name="exchange_rate_dbt",
    target_name="dev",
    profiles_yml_filepath="/opt/airflow/dbt/profiles.yml",
)

with DAG(
    dag_id="exchange_rate_pipeline",
    default_args=default_args,
    description="Daily ELT pipeline: Frankfurter API → MinIO → PostgreSQL → dbt",
    schedule="0 16 * * 1-5",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["exchange-rate", "elt"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_to_minio",
        python_callable=extract_to_minio,
    )

    load_task = PythonOperator(
        task_id="load_to_staging",
        python_callable=load_to_staging,
    )

    # Cosmos auto-generates tasks from dbt models
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(
            dbt_project_path="/opt/airflow/dbt",
        ),
        profile_config=profile_config,
        render_config=RenderConfig(
            test_behavior="after_all",
        ),
    )

    extract_task >> load_task >> dbt_transform
