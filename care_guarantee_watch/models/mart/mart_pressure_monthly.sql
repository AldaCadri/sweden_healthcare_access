{{ config(materialized='view') }}

-- Pressure indicators we actually want on this page:
--   1) displaced_patients       (count per month)
--   2) median_tid_till_läkare_i_minuter  (minutes per month; comes from visits-by-gender/age fact)
--
-- We carry unit + a metric_kind so PBI can separate minutes vs counts.

with press_dim as (
  select
    indicator_key,
    source_system,
    source_indicator_code,
    indicator_name,
    coalesce(unit, case
                     when lower(source_indicator_code) like '%median_tid_till_läkare%' then 'minuter'
                     when lower(source_indicator_code) like '%displaced%' then 'antal'
                   end)                                     as unit
  from {{ ref('dim_indicator') }}
  where upper(coalesce(topic,'')) = 'PRESSURE'
),

-- 1) Overcrowding (displaced patients) – monthly counts
overc_raw as (
  select
    f.region_key,
    f.date_key,
    f.indicator_key,
    f.value
  from {{ ref('fct_overcrowding_monthly') }} f
),
overc as (
  select
    o.region_key,
    o.date_key,
    d.source_system,
    d.source_indicator_code,
    d.indicator_name,
    d.unit,
    'COUNT'::varchar as metric_kind,
    o.value
  from overc_raw o
  join press_dim d on d.indicator_key = o.indicator_key
  where lower(d.source_indicator_code) like '%displaced%'   -- replace with exact code if you have it
),

-- 2) Median time to doctor (minutes) – monthly; source rows are by gender/age,
--    so we aggregate to region x month to avoid duplicate points in KPIs.
doctime_raw as (
  select
    f.region_key,
    f.date_key,
    f.indicator_key,
    f.value
  from {{ ref('fct_visits_by_gender_age_m') }} f
),
doctime as (
  select
    dct.region_key,
    dct.date_key,
    dim.source_system,
    dim.source_indicator_code,
    dim.indicator_name,
    dim.unit,
    'MINUTES'::varchar as metric_kind,
    avg(dct.value) as value                               -- avg across gender/age rows
  from doctime_raw dct
  join press_dim dim on dim.indicator_key = dct.indicator_key
  where lower(dim.source_indicator_code) like '%median_tid_till_läkare%'
  group by 1,2,3,4,5,6
)

select * from overc
union all
select * from doctime