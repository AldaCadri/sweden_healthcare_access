{{ config(materialized='view') }}

with
-- Only the columns needed from the indicator dim
dim_i as (
  select
    indicator_key,
    upper(trim(topic))                 as topic,
    upper(trim(polarity))              as polarity,
    source_system,
    source_indicator_code,
    indicator_name
  from {{ ref('dim_indicator') }}
),

/* =======================
   MONTHLY: WAITING_TIME from fct_performance_monthly
   ======================= */
m_wait as (
  select
    f.region_key,
    f.date_key,
    cast(null as int)                  as year,
    'MONTH'                            as time_grain,
    d.topic                            as theme,
    d.polarity,
    d.source_indicator_code            as metric_code,
    d.indicator_name,
    f.value
  from {{ ref('fct_performance_monthly') }} f
  join dim_i d on d.indicator_key = f.indicator_key
  where d.topic = 'WAITING_TIME'
),

/* =======================
   MONTHLY: PRESSURE from fct_overcrowding_monthly
   ======================= */
m_pressure_overc as (
  select
    f.region_key,
    f.date_key,
    cast(null as int)                  as year,
    'MONTH'                            as time_grain,
    d.topic                            as theme,
    d.polarity,
    d.source_indicator_code            as metric_code,
    d.indicator_name,
    f.value
  from {{ ref('fct_overcrowding_monthly') }} f
  join dim_i d on d.indicator_key = f.indicator_key
  where d.topic = 'PRESSURE'
),

/* =======================
   MONTHLY: DEMAND (visits) from fct_visits_by_gender_age_m
   ======================= */
m_demand_visits as (
  select
    f.region_key,
    f.date_key,
    cast(null as int)                  as year,
    'MONTH'                            as time_grain,
    'DEMAND'                           as theme,
    'NEUTRAL'                          as polarity,
    coalesce(d.source_indicator_code, 'VISITS_TOTAL') as metric_code,
    coalesce(d.indicator_name, 'ED visits (total)')   as indicator_name,
    sum(f.value)                       as value
  from {{ ref('fct_visits_by_gender_age_m') }} f
  left join dim_i d on d.indicator_key = f.indicator_key
                     and d.topic = 'DEMAND'
  group by 1,2,3,4,5,6,7,8
),

/* =======================
   MONTHLY: PRESSURE (median time to doctor) in the same fact
   ======================= */
m_pressure_doctime as (
  select
    f.region_key,
    f.date_key,
    cast(null as int)                  as year,
    'MONTH'                            as time_grain,
    'PRESSURE'                         as theme,
    'NEG'                              as polarity,
    d.source_indicator_code            as metric_code,
    d.indicator_name,
    avg(f.value)                       as value
  from {{ ref('fct_visits_by_gender_age_m') }} f
  join dim_i d on d.indicator_key = f.indicator_key
  where d.topic = 'PRESSURE'          
  group by 1,2,3,4,5,6,7,8
),

/* =======================
   YEARLY: CAPACITY / COST (and any yearly WAITING_TIME/QUALITY) from capacity + yearly performance
   ======================= */
y_capacity_cost as (
  select
    c.region_key,
    cast(null as number)               as date_key,
    c.year,
    'YEAR'                             as time_grain,
    d.topic                            as theme,
    d.polarity,
    d.source_indicator_code            as metric_code,
    d.indicator_name,
    c.value
  from {{ ref('fct_capacity_yearly') }} c
  join dim_i d on d.indicator_key = c.indicator_key
  where d.topic in ('CAPACITY','COST','WAITING_TIME','DEMAND')
),

y_perf as (
  select
    f.region_key,
    cast(null as number)               as date_key,
    f.year,
    'YEAR'                             as time_grain,
    d.topic                            as theme,
    d.polarity,
    d.source_indicator_code            as metric_code,
    d.indicator_name,
    f.value
  from {{ ref('fct_performance_yearly') }} f
  join dim_i d on d.indicator_key = f.indicator_key
  where d.topic in ('CAPACITY','COST','WAITING_TIME','DEMAND')
),

y_kolada as (
  select
    f.region_key,
    cast(null as number)               as date_key,
    f.year,
    'YEAR'                             as time_grain,
    d.topic                            as theme,
    d.polarity,
    d.source_indicator_code            as metric_code,
    d.indicator_name,
    f.value
  from {{ ref('fct_kolada_y') }} f
  join dim_i d on d.indicator_key = f.indicator_key
  where d.topic in ('CAPACITY','COST','WAITING_TIME','DEMAND')
)

select * from m_wait
union all
select * from m_pressure_overc
union all
select * from m_demand_visits
union all
select * from m_pressure_doctime
union all
select * from y_capacity_cost
union all
select * from y_perf
union all
select * from y_kolada
