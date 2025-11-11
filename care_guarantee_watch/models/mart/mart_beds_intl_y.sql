{{ config(materialized='view') }}

-- Normalize keys from fact (supports either date_key or year)
with f as (
  select
      country_name,
      to_number(concat(year,'0101'))                             as date_key,
      year,
      indicator_key,
      value
  from {{ ref('fct_beds_intl_y') }}
),

joined as (
  select
      f.country_name,
      f.date_key,
      f.year,
      f.indicator_key,
      coalesce(i.indicator_name, 'Hospital beds per 1,000 inhabitants') as indicator_name,
      coalesce(i.topic, 'CAPACITY')                                      as topic,
      coalesce(i.source_system, 'OECD_HEALTH')                           as source_system,
      f.value,                                                           -- numeric
      'Per 1 000 inhabitants'                                            as unit
  from f
  left join {{ ref('dim_date') }}      d on d.date_key = f.date_key
  left join {{ ref('dim_indicator') }} i on i.indicator_key = f.indicator_key
)

select * from joined