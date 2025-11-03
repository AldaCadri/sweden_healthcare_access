{{ config(materialized='table') }}

with src as (
  select *
  from {{ ref('stg_dataexport') }}
  where period_granularity = 'YEAR'
),

final as (
  select
    r.region_key,
    s.year::int          as year,
    g.gender_key,        -- may be null
    i.indicator_key,
    s.value::float       as value,
    s.numerator::float   as numerator,
    s.denominator::float as denominator,
    s.unit               as unit
  from src s
  left join {{ ref('dim_region') }}    r on r.region_code_int = s.region_code
  left join {{ ref('dim_gender') }}    g on g.gender_code    = s.gender
  left join {{ ref('dim_indicator') }} i
  on upper(trim(i.source_system)) = upper(trim(s.source_system))
 and upper(trim(i.source_indicator_code)) = upper(trim(s.source_indicator_code))
)

select * from final