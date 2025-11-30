{{ config(materialized='table') }}

with

-- Beds (region–year) 
beds as (
  select
    r.region_key,
    s.year::int                               as year,
    'BEDS_PER_1000'                           as metric_type,
    'BEDS_REGION'                             as src_system,       
    s.indicator_code                          as src_code,          -- from STG_BEDS_REGION
    s.value::float                            as value,
    cast(null as varchar)                     as unit
  from {{ ref('stg_beds_region') }} s
  left join {{ ref('dim_region') }} r
    on r.region_code_int = s.region_code
),

-- Staffing (region–year)
staff as (
  select
    r.region_key,
    s.year::int                   as year,
    'STAFF_HEADCOUNT'             as metric_type,
    s.source_system               as src_system,
    s.source_indicator_code       as src_code,
    sum(s.value)::float           as value,
    cast(null as varchar)         as unit
  from {{ ref('stg_medical_personnel') }} s
  left join {{ ref('dim_region') }} r
    on r.region_name = s.region_name
  where s.value is not null
  group by r.region_key, s.year, s.source_system, s.source_indicator_code
),

-- Population (region–year) 
pop as (
  select
    r.region_key,
    s.year::int                               as year,
    'POPULATION'                              as metric_type,
    'SCB'                                     as src_system,        
    'POPULATION'                              as src_code,          
    s.population::float                       as value,
    cast(null as varchar)                     as unit
  from {{ ref('stg_population') }} s
  left join {{ ref('dim_region') }} r
    on r.region_code_int = s.region_code
),

-- Net cost (region–year)
cost as (
  select
    r.region_key,
    s.year::int                               as year,
    'NET_COST_SEK'                            as metric_type,
    s.source_system                           as src_system,
    s.source_indicator_code                   as src_code,
    s.value::float                            as value,
    s.unit                                    as unit
  from {{ ref('stg_net_cost') }} s
  left join {{ ref('dim_region') }} r
    on r.region_code_int = s.region_code
),

unioned as (
  select * from beds
  union all select * from staff
  union all select * from pop
  union all select * from cost
)

select
  u.region_key,
  u.year,
  u.metric_type,
  i.indicator_key,      
  u.value,
  u.unit
from unioned u
left join {{ ref('dim_indicator') }} i
  on i.source_system = u.src_system
 and i.source_indicator_code = u.src_code