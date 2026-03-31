-- Date dimension: generated from the date range in staging data
-- Materialized as table because it's queried frequently by fact joins

{{ config(materialized='table') }}

with date_spine as (
    select
        generate_series(
            (select min(rate_date) from {{ ref('stg_exchange_rates') }}),
            (select max(rate_date) from {{ ref('stg_exchange_rates') }}),
            interval '1 day'
        )::date as date_id
)

select
    date_id,
    extract(year from date_id)::int as year,
    extract(month from date_id)::int as month,
    extract(day from date_id)::int as day,
    extract(dow from date_id)::int as day_of_week,
    to_char(date_id, 'Day') as day_name,
    to_char(date_id, 'Month') as month_name,
    extract(week from date_id)::int as week_of_year,
    case when extract(dow from date_id) in (0, 6) then true else false end as is_weekend
from date_spine
