-- Fact table: exchange rates with daily change calculations
-- Grain: 1 row per currency per business day

{{ config(materialized='table') }}

with rates as (
    select
        s.rate_date,
        s.target_currency,
        c.currency_id,
        s.rate,
        lag(s.rate) over (
            partition by s.target_currency
            order by s.rate_date
        ) as prev_day_rate
    from {{ ref('stg_exchange_rates') }} s
    inner join warehouse.dim_currencies c
        on s.target_currency = c.currency_code
)

select
    r.currency_id,
    r.rate_date as date_id,
    r.rate,
    r.prev_day_rate,
    r.rate - r.prev_day_rate as daily_change,
    case
        when r.prev_day_rate is not null and r.prev_day_rate != 0
        then round(((r.rate - r.prev_day_rate) / r.prev_day_rate * 100)::numeric, 4)
        else null
    end as daily_change_pct
from rates r
