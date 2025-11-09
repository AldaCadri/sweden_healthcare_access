{{ config(materialized='table') }}

-- Build a unified registry of indicators across all STG sources,
-- using the *actual* column names found in your CSVs.

with base as (

  /* ========== MONTHLY SOURCES ========== */

  -- STG_DATAEXPORT: has SOURCE_SYSTEM, SOURCE_INDICATOR_CODE, INDICATOR_NAME, UNIT
  select distinct
      SOURCE_SYSTEM                           as source_system,
      SOURCE_INDICATOR_CODE                   as source_indicator_code,
      INDICATOR_NAME                          as indicator_name,
      UNIT                                    as unit,
      'MONTH'                                 as default_granularity
  from {{ ref('stg_dataexport') }}
  where PERIOD_GRANULARITY = 'MONTH'

  union
  -- STG_OVERCROWDING: has SOURCE_SYSTEM, SOURCE_INDICATOR_CODE, INDICATOR_NAME, UNIT
  select distinct
      SOURCE_SYSTEM,
      SOURCE_INDICATOR_CODE,
      INDICATOR_NAME,
      cast(null as varchar)                   as unit,
      'MONTH' as default_granularity
  from {{ ref('stg_overcrowding') }}

  union
  -- STG_TID_KONALDER: has SOURCE_SYSTEM, SOURCE_INDICATOR_CODE, INDICATOR_NAME (no UNIT column)
  select distinct
      SOURCE_SYSTEM,
      SOURCE_INDICATOR_CODE,
      INDICATOR_NAME,
      cast(null as varchar)                   as unit,
      'MONTH'                                 as default_granularity
  from {{ ref('stg_tid_konalder') }}

  union
  -- STG_TID_DIAGNOS: has SOURCE_SYSTEM, SOURCE_INDICATOR_CODE, INDICATOR_NAME, UNIT
  select distinct
      SOURCE_SYSTEM,
      SOURCE_INDICATOR_CODE,
      INDICATOR_NAME,
      UNIT as unit,
      'MONTH' as default_granularity
  from {{ ref('stg_tid_diagnos') }}


  /* ========== YEARLY SOURCES ========== */

    union
    select distinct
        SOURCE_SYSTEM                               as source_system,
        SOURCE_INDICATOR_CODE                       as source_indicator_code,
        INDICATOR_NAME                              as indicator_name,
        UNIT                                        as unit,
        'YEAR'                                      as default_granularity
    from {{ ref('stg_dataexport') }}
    where PERIOD_GRANULARITY = 'YEAR'

  union
  -- STG_BEDS_REGION: NO SOURCE_SYSTEM column, and code is INDICATOR_CODE
  select distinct
      'BEDS_REGION'                           as source_system,
      INDICATOR_CODE                          as source_indicator_code,
      INDICATOR_NAME                          as indicator_name,
      cast(null as varchar)                   as unit,          
      'YEAR'                                  as default_granularity
  from {{ ref('stg_beds_region') }}

  union
  select distinct
      SOURCE_SYSTEM,
      SOURCE_INDICATOR_CODE,
      INDICATOR_NAME,
      UNIT,
      'YEAR' as default_granularity
  from {{ ref('stg_beds_intl') }}

  union
  -- STG_MEDICAL_PERSONNEL: has SOURCE_SYSTEM, SOURCE_INDICATOR_CODE, INDICATOR_NAME (unit may be absent)
  select distinct
      SOURCE_SYSTEM,
      SOURCE_INDICATOR_CODE,
      INDICATOR_NAME,
      cast(null as varchar)                   as unit,
      'YEAR'                                  as default_granularity
  from {{ ref('stg_medical_personnel') }}

  union
  -- STG_POPULATION: NO SOURCE_SYSTEM / CODE / NAME; create a stable synthetic indicator
  select distinct
      'SCB'                                   as source_system,
      'POPULATION'                            as source_indicator_code,
      'Population'                            as indicator_name,
      cast(null as varchar)                   as unit,
      'YEAR'                                  as default_granularity
  from {{ ref('stg_population') }}

  union
  -- STG_NET_COST: has SOURCE_SYSTEM, SOURCE_INDICATOR_CODE, INDICATOR_NAME, UNIT
  select distinct
      SOURCE_SYSTEM,
      SOURCE_INDICATOR_CODE,
      INDICATOR_NAME,
      UNIT as unit,
      'YEAR'                                  as default_granularity
  from {{ ref('stg_net_cost') }}

  union
  -- STG_KOLADA: has SOURCE_SYSTEM, SOURCE_INDICATOR_CODE, INDICATOR_NAME (unit not present)
  select distinct
      SOURCE_SYSTEM,
      SOURCE_INDICATOR_CODE,
      INDICATOR_NAME,
     cast(null as varchar)                   as unit,
      'YEAR'                                  as default_granularity
  from {{ ref('stg_kolada') }}

union
-- STG_PHYSICIANSPER1000 (separate indicator registry)
select distinct
    'OECD_HEALTH'                  as source_system,
    'PHYSICIANS_PER_1000'          as source_indicator_code,
    'Doctors per 1000 inhabitants' as indicator_name,
    'Per 1000 inhabitants'         as unit,
    'YEAR'                         as default_granularity
from {{ ref('stg_physicians_per1000') }}

  union
  -- STG_EXPENDITURE_GDP: has SOURCE_SYSTEM, SOURCE_INDICATOR_CODE, INDICATOR_NAME, UNIT
  select distinct
      SOURCE_SYSTEM,
      SOURCE_INDICATOR_CODE,
      INDICATOR_NAME,
      UNIT as unit,
      'YEAR'                                  as default_granularity
  from {{ ref('stg_expenditure_gdp') }}
),

-- Deduplicate by (source_system, source_indicator_code)
dedup as (
  select
    {{ dbt_utils.generate_surrogate_key(['source_system','source_indicator_code']) }} as indicator_key,
    source_system,
    source_indicator_code,
    indicator_name,
    unit,
    default_granularity
  from base
  qualify row_number() over (
    partition by source_system, source_indicator_code
    order by indicator_name
  ) = 1
),


map as (
  select
    upper(trim(source_system))       as source_system,
    upper(trim(source_indicator_code)) as source_indicator_code,
    topic,
    polarity
  from {{ ref('dim_indicator_taxonomy') }}
),

final as (
  select
    d.indicator_key,
    d.source_system,
    d.source_indicator_code,
    d.indicator_name,
    d.unit,
    d.default_granularity,
    m.topic,
    m.polarity
  from dedup d
  left join map m
    on upper(trim(m.source_system)) = upper(trim(d.source_system))
   and upper(trim(m.source_indicator_code)) = upper(trim(d.source_indicator_code))
)

select * from final
