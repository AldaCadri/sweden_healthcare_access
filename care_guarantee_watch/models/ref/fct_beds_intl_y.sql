{{ config(materialized='table') }}

with src as (
  select *
  from {{ ref('stg_beds_intl') }}
)

select
  s.country_name,
  s.year::int as year,
  i.indicator_key,
  s.value::float as value,
  s.unit as unit
from src s
left join {{ ref('dim_indicator') }} i
  on i.source_system = s.source_system
 and i.source_indicator_code = s.source_indicator_code