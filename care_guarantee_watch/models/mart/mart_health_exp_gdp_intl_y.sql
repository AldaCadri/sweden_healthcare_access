{{ config(materialized='view', schema='MART') }}

with f as (
  select country_name, date_key, indicator_key, value
  from {{ ref('fct_health_exp_gdp_intl_y') }}
),

joined as (
  select
    f.country_name,
    f.date_key,
    d.year,
    f.indicator_key,
    coalesce(i.indicator_name, 'Health expenditure (% of GDP)') as indicator_name,
    coalesce(i.topic, 'COST')                                    as topic,
    coalesce(i.source_system, 'OECD_HEALTH')                     as source_system,
    f.value,
    '% of GDP'                                                   as unit
  from f
  left join {{ ref('dim_date') }}      d on d.date_key = f.date_key
  left join {{ ref('dim_indicator') }} i on i.indicator_key = f.indicator_key
)

select * from joined