-- Staging model: clean and rename columns from raw source
-- Materialized as view because it's just a passthrough layer

{{ config(materialized='view', schema='staging') }}

select
    id,
    base_currency,
    target_currency,
    rate,
    rate_date,
    loaded_at
from {{ source('staging', 'stg_exchange_rates') }}
