{{ config(materialized='view') }}

with waits as (
  select
    f.region_key,
    f.year,
    cast(year * 10000 + 101 as number(8,0)) as date_key,
    f.gender_key,
    f.indicator_key,
    d.source_system,
    d.source_indicator_code,
    d.indicator_name,
    d.unit,
    f.value,
  from {{ ref('fct_kolada_y') }} f
  join {{ ref('dim_indicator') }} d
    on d.indicator_key = f.indicator_key
  where coalesce(d.topic,'') = 'WAITING_TIME'
  and d.source_indicator_code in ('n79241','n79240','n79242','n79243')  -- care guarantee indicators(median days)
)

select * from waits