{{ config(materialized='table', schema='REF') }}

with s as (
  select * from {{ ref('stg_health_exp_gdp_intl') }}
),
keys as (
  select
    s.country_name,
    to_number(concat(s.year,'0101'))                             as date_key,   -- YYYY0101
    i.indicator_key,
    s.value
  from s
  left join {{ ref('dim_indicator') }} i
    on i.source_system = s.source_system
   and i.source_indicator_code = s.source_indicator_code
)

select
  country_name,
  date_key,
  indicator_key,     
  value
from keys
where date_key is not null