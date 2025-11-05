{{ config(materialized='view') }}

with waits as (
  select
    f.region_key,
    f.date_key,
    f.gender_key,
    f.indicator_key,
    d.source_system,
    d.source_indicator_code,
    d.indicator_name,
    d.unit,
    f.value,
    f.numerator,
    f.denominator
  from {{ ref('fct_performance_monthly') }} f
  join {{ ref('dim_indicator') }} d
    on d.indicator_key = f.indicator_key
  where coalesce(d.topic,'') = 'WAITING_TIME'
)

select * from waits