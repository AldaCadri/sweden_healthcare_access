{{ config(materialized='view') }}

-- PRESSURE indicators only
with press_dim as (
  select
    i.indicator_key,
    i.source_system,
    i.source_indicator_code,
    i.indicator_name,
    coalesce(i.unit,
      case
        when lower(i.source_indicator_code) like '%median_tid_till_läkare%' then 'minuter'
        when lower(i.source_indicator_code) like '%displaced%' then 'antal'
        when lower(i.source_indicator_code) like '%besök%' then 'antal'
      end
    ) as unit
  from {{ ref('dim_indicator') }} i
  where upper(coalesce(i.topic,'')) = 'PRESSURE'
  or (
      upper(i.source_system) in ('STAT_KON_ALDER','STAT_DIAGNOS')
      and regexp_like(lower(i.source_indicator_code||' '||i.indicator_name), 'antal.*bes(ö|o)k|bes_k')
    )
),

-- 1) Overcrowding (monthly count)
overc as (
  select
    f.region_key,                -- varchar
    f.date_key,                  -- varchar
    d.source_system,
    d.source_indicator_code,
    d.indicator_name,
    d.unit,
    'COUNT'::varchar  as metric_kind,
    'MONTHLY'::varchar as grain,
    null::varchar     as gender_key,
    null::varchar     as age_key,
    null::varchar     as diagnosis_group,
    f.value::float    as value
  from {{ ref('fct_overcrowding_monthly') }} f
  join press_dim d on d.indicator_key = f.indicator_key
  where lower(d.source_indicator_code) like '%displaced%'
),

-- 2) Median time to doctor (minutes) by age x gender 
doctime_detail as (
  select
    f.region_key,
    f.date_key,
    d.source_system,
    d.source_indicator_code,
    d.indicator_name,
    d.unit,
    'MINUTES'::varchar as metric_kind,
    'MONTHLY'::varchar as grain,
    f.gender_key,               
    f.age_key,                  
    null::varchar  as diagnosis_group,
    f.value::float as value
  from {{ ref('fct_visits_by_gender_age_m') }} f
  join press_dim d on d.indicator_key = f.indicator_key
  where lower(d.source_indicator_code) like '%median_tid_till_läkare%'
),

-- 3) Doctor visits (count) by age x gender 
visits_detail as (
  select
    f.region_key,
    f.date_key,
    d.source_system,
    d.source_indicator_code,
    d.indicator_name,
    coalesce(d.unit,'antal') as unit,
    'VISITS'::varchar  as metric_kind,
    'MONTHLY'::varchar as grain,
    f.gender_key,
    f.age_key,
    null::varchar  as diagnosis_group,
    f.value::float as value
  from {{ ref('fct_visits_by_gender_age_m') }} f
  join press_dim d on d.indicator_key = f.indicator_key
  where regexp_like(lower(d.source_indicator_code||' '||d.indicator_name), 'antal.*bes(ö|o)k|bes_k')
),

-- 4) Median time to doctor by diagnosis (minutes)
doctime_diag as (
  select
    f.region_key,
    f.date_key,
    d.source_system,
    d.source_indicator_code,
    d.indicator_name,
    coalesce(d.unit,'minuter') as unit,
    'MINUTES'::varchar  as metric_kind,
    'MONTHLY'::varchar  as grain,
    null::varchar       as gender_key,
    null::varchar       as age_key,
    f.diagnosgrupp::varchar as diagnosis_group,
    f.value::float  as value
  from {{ ref('fct_ed_time_by_diagnosis_m') }} f
  join press_dim d on d.indicator_key = f.indicator_key
)

select * from overc
union all
select * from doctime_detail
union all
select * from visits_detail
union all
select * from doctime_diag
