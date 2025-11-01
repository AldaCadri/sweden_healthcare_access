{{ config(materialized='table') }}

with src as (
  select *
  from {{ ref('stg_tid_konalder') }}
)

select
  r.region_key,
  to_number(to_char(date_trunc('month', s.period_date),'YYYYMMDD')) as date_key,
  g.gender_key,
  a.age_key,
  i.indicator_key,
  s.value::float as value
from src s
left join {{ ref('dim_region') }}       r on r.region_name     = s.region_name
left join {{ ref('dim_gender') }}       g on g.gender_code         = s.gender
left join {{ ref('dim_age_group') }}    a on a.age_group_label     = s.age_group
left join {{ ref('dim_indicator') }}    i
  on i.source_system = s.source_system
 and i.source_indicator_code = s.source_indicator_code