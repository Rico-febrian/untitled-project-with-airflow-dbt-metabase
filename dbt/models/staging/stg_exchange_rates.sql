{{ config(materialized='view') }}

select
    id,
    base_currency,
    target_currency,
    rate,
    rate_date,
    loaded_at
from {{ source('staging', 'stg_exchange_rates') }}
