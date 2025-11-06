{{ config(materialized='view') }}

select
  f.region_key,
  f.year,
  d.source_system,
  d.source_indicator_code,
  d.indicator_name,
  coalesce(d.unit,'minuter') as unit,
  'MINUTES'::varchar as metric_kind,
  f.value
from {{ ref('fct_performance_yearly') }} f
join {{ ref('dim_indicator') }} d
  on d.indicator_key = f.indicator_key
where lower(d.indicator_name) like '%responstid för ambulans%'
   or lower(d.source_indicator_code) like '%ambulans%'