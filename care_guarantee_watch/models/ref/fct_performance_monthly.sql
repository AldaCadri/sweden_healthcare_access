{{ config(materialized='table') }}

with src as (
  select *
  from {{ ref('stg_dataexport') }}
  where period_granularity = 'MONTH'
),

final as (
  select
    r.region_key,
    to_number(to_char(date_trunc('month', s.period_month),'YYYYMMDD')) as date_key,
    g.gender_key,                              -- may be null if STG has NULL gender
    i.indicator_key,
    s.value::float       as value,
    s.numerator::float   as numerator,
    s.denominator::float as denominator,
    s.unit               as unit
  from src s
  left join {{ ref('dim_region') }}    r on r.region_code_int = s.region_code
  left join {{ ref('dim_gender') }}    g on g.gender_code    = s.gender
  left join {{ ref('dim_indicator') }} i
    on i.source_system = s.source_system
   and i.source_indicator_code = s.source_indicator_code
)

select * from final