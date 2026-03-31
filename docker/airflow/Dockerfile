FROM apache/airflow:2.10.4

# Official constraint file locks all dependency versions tested with Airflow 2.10.4 + Python 3.12
ARG CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.12.txt"

# Airflow providers + pipeline dependencies (versions resolved by constraint file)
RUN pip install --no-cache-dir \
    "apache-airflow-providers-postgres" \
    "apache-airflow-providers-amazon" \
    "apache-airflow-providers-slack" \
    "apache-airflow-providers-http" \
    pandas \
    minio \
    requests \
    --constraint "${CONSTRAINTS}"

# dbt + Cosmos installed separately (not in Airflow constraints)
# Pin Airflow to prevent upgrade to 3.x
RUN pip install --no-cache-dir \
    "apache-airflow==2.10.4" \
    "astronomer-cosmos==1.8.0" \
    "dbt-core==1.8.7" \
    "dbt-postgres==1.8.2"
