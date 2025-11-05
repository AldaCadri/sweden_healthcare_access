-- models/marts/pressure/mart_pressure_monthly.sql
{{ config(materialized='view') }}

with press_dim as (
  select indicator_key, source_system, source_indicator_code, indicator_name, polarity
  from {{ ref('dim_indicator') }}
  where upper(coalesce(topic,'')) = 'PRESSURE'
),

overc as (
  select f.region_key, f.date_key, f.indicator_key, f.value
  from {{ ref('fct_overcrowding_monthly') }} f
),

doctime as (
  
  select f.region_key, f.date_key, f.indicator_key, f.value
  from {{ ref('fct_visits_by_gender_age_m') }} f
),

perf as (
  -- any PRESSURE metrics that live in performance_monthly (e.g., ambulance response time)
  select f.region_key, f.date_key, f.indicator_key, f.value
  from {{ ref('fct_performance_monthly') }} f
),

unioned as (
  select * from overc
  union all select * from doctime
  union all select * from perf
)

select
  u.region_key,
  u.date_key,
  d.source_system,
  d.source_indicator_code as metric_code,
  d.indicator_name,
  d.polarity,
  u.value
from unioned u
join press_dim d
  on d.indicator_key = u.indicator_key