import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


def load_to_staging(**context):
    """
    Read raw JSON from MinIO and upsert into staging.stg_exchange_rates.
    Gets the MinIO file path from the extract task via XCom.
    """

    # Get the s3 key from extract task via XCom
    s3_key = context["ti"].xcom_pull(task_ids="extract_to_minio")
    bucket = Variable.get("exchange_rate_bucket")

    # Read JSON from MinIO
    s3_hook = S3Hook(aws_conn_id="minio_conn")
    file_content = s3_hook.read_key(key=s3_key, bucket_name=bucket)
    data = json.loads(file_content)

    print(f"Loaded data for date: {data['date']}, base: {data['base']}")

    # Parse rates into rows
    base_currency = data["base"]
    rate_date = data["date"]
    raw_json = json.dumps(data)

    rows = []
    for currency, rate in data["rates"].items():
        rows.append((base_currency, currency, rate, rate_date, raw_json))

    # Upsert into staging table
    pg_hook = PostgresHook(postgres_conn_id="warehouse_conn")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO staging.stg_exchange_rates
            (base_currency, target_currency, rate, rate_date, raw_json)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (base_currency, target_currency, rate_date)
        DO UPDATE SET
            rate = EXCLUDED.rate,
            raw_json = EXCLUDED.raw_json,
            loaded_at = CURRENT_TIMESTAMP;
    """

    for row in rows:
        cursor.execute(insert_sql, row)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Upserted {len(rows)} rows into staging.stg_exchange_rates")
