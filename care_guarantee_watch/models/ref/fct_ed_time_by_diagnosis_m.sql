{{ config(materialized='table') }}

with src as (
  select *
  from {{ ref('stg_tid_diagnos') }}
),
dim_diag as (
  select distinct diagnosgrupp,
         {{ dbt_utils.generate_surrogate_key(['diagnosgrupp']) }} as diagnosis_key
  from {{ ref('stg_tid_diagnos') }}
  where diagnosgrupp is not null
)

select
  r.region_key,
  to_number(to_char(date_trunc('month', s.period_month),'YYYYMMDD')) as date_key,
  d.diagnosis_key,
  s.diagnosgrupp,
  i.indicator_key,
  s.value::float as value,
  s.unit as unit
from src s
left join {{ ref('dim_region') }}    r on r.region_name = s.region_name
left join dim_diag                   d on d.diagnosgrupp   = s.diagnosgrupp
left join {{ ref('dim_indicator') }} i
  on i.source_system = s.source_system
 and i.source_indicator_code = s.source_indicator_code