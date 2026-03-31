import json
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable


def extract_to_minio(**context):
    """
    Extract exchange rates from Frankfurter API and save raw JSON to MinIO.
    Uses Airflow execution date so backfills and retries fetch the correct date.
    """

    # Get config from Airflow variables
    base_currency = Variable.get("exchange_rate_base_currency")
    target_currencies = Variable.get("exchange_rate_target_currencies")
    bucket = Variable.get("exchange_rate_bucket")

    # Get execution date from Airflow context (not datetime.now)
    execution_date = context["logical_date"]
    date_str = execution_date.strftime("%Y-%m-%d")

    # Setup hooks (credentials come from Airflow connections)
    http_hook = HttpHook(http_conn_id="frankfurter_api", method="GET")
    s3_hook = S3Hook(aws_conn_id="minio_conn")

    # Call Frankfurter API
    endpoint = f"/v1/{date_str}?base={base_currency}&symbols={target_currencies}"
    print(f"Fetching exchange rates for {date_str}...")

    response = http_hook.run(endpoint=endpoint)
    data = response.json()

    print(f"Rates received: {data['rates']}")

    # Build MinIO path with date partitioning
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day = execution_date.strftime("%d")
    s3_key = f"year={year}/month={month}/day={day}/rates.json"

    # Upload raw JSON to MinIO
    s3_hook.load_string(
        string_data=json.dumps(data),
        key=s3_key,
        bucket_name=bucket,
        replace=True,
    )

    print(f"Saved to s3://{bucket}/{s3_key}")

    # Pass the s3 key to the next task via XCom
    return s3_key
