{{ config(materialized='view') }}

with
cal as (
  -- full monthly calendar from the dim
  select date_key
  from {{ ref('dim_date') }}
),

pairs as (
  -- all region × indicator pairs for WAITING_TIME metrics seen in the data
  select distinct
    f.region_key,
    f.indicator_key
  from {{ ref('fct_performance_monthly') }} f
  join {{ ref('dim_indicator') }} d
    on d.indicator_key = f.indicator_key
  where coalesce(d.topic,'') = 'WAITING_TIME'
),

grid as (
  -- dense grid for all months (you can limit the calendar if you want)
  select
    p.region_key,
    c.date_key,
    p.indicator_key
  from pairs p
  cross join cal c
),

vals as (
  -- observed monthly values
  select
    region_key,
    date_key,
    indicator_key,
    value
  from {{ ref('fct_performance_monthly') }}
)

select
  g.region_key,
  g.date_key,
  g.indicator_key,
  v.value
from grid g
left join vals v
  on v.region_key     = g.region_key
 and v.date_key       = g.date_key
 and v.indicator_key  = g.indicator_key