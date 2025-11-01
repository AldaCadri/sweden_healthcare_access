{{ config(materialized='table') }}

with src as (
  select *
  from {{ ref('stg_overcrowding') }}
)

select
  r.region_key,
  to_number(to_char(date_trunc('month', s.period_month),'YYYYMMDD')) as date_key,
  i.indicator_key,
  s.value::float as value
from src s
left join {{ ref('dim_region') }}    r on r.region_name = s.region_name
left join {{ ref('dim_indicator') }} i
  on i.source_system = s.source_system
 and i.source_indicator_code = s.source_indicator_code