{{ config(materialized='table') }}

with src as (
  select *
  from {{ ref('stg_kolada') }}
)

select
  r.region_key,
  s.year::int as year,
  g.gender_key,
  i.indicator_key,
  s.value::float as value,
  null::varchar as unit
from src s
left join {{ ref('dim_region') }}    r on r.region_name = s.region_name    -- switch to code if available
left join {{ ref('dim_gender') }}    g on g.gender_code = s.gender
left join {{ ref('dim_indicator') }} i
  on i.source_system = s.source_system
 and i.source_indicator_code = s.source_indicator_code