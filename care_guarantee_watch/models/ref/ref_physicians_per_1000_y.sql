{{ config(materialized='view') }}


with s as (
  select
      region_name,
      year,
      value::float as value,
      'OECD_HEALTH'                  as source_system,
      'PHYSICIANS_PER_1000'          as source_indicator_code,
      'Doctors per 1000 inhabitants' as indicator_name,
      'Per 1000 inhabitants'         as unit
  from {{ ref('stg_physicians_per1000') }}
),

-- map region + date_key
mapped as (
  select
      r.region_key,
      to_number(concat(s.year, '0101'))           as date_key,   -- YYYY0101
      s.value,
      s.year,
      s.source_system,
      s.source_indicator_code,
      s.indicator_name,
      s.unit
  from s
  left join {{ ref('dim_region') }} r
    on upper(r.region_name) = upper(s.region_name)
 
   where r.region_key is not null
),

with_indicator as (
  select
      m.region_key,
      m.date_key,
      m.year,
      i.indicator_key,
      m.value,
      m.source_system,
      m.source_indicator_code,
      m.indicator_name,
      m.unit
  from mapped m
  left join {{ ref('dim_indicator') }} i
    on i.source_system = m.source_system
   and i.source_indicator_code = m.source_indicator_code
)

select * from with_indicator