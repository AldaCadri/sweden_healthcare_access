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
  -- STG_BEDS_REGION: NO SOURCE_SYSTEM column, and code is INDICATOR_CODE
  select distinct
      'BEDS_REGION'                           as source_system,
      INDICATOR_CODE                          as source_indicator_code,
      INDICATOR_NAME                          as indicator_name,
      cast(null as varchar)                   as unit,          
      'YEAR'                                  as default_granularity
  from {{ ref('stg_beds_region') }}

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

/* Optional: light taxonomy so you can filter in marts / Power BI without hardcoding there.
   Extend this VALUES block with real codes you encounter as you go. */
map as (
  select
    column1::varchar as source_system,
    column2::varchar as source_indicator_code,
    column3::varchar as topic,      -- e.g., WAITING_TIME, PRESSURE, CAPACITY, COST, QUALITY, UTILIZATION
    column4::varchar as polarity    -- POS (higher=better) | NEG (higher=worse) | NEUTRAL
  from values
    -- Waiting-time examples (VIS = Vården i siffror)
    ('VIS','E3_CONTACT','WAITING_TIME','POS'),
    ('VIS','E90_FIRST_VISIT','WAITING_TIME','POS'),
    ('VIS','E90_OPERATION','WAITING_TIME','POS'),
    -- Pressure
    ('VIS','ED_OVERCRWD','PRESSURE','NEG'),
    -- Capacity / population / cost
    ('BEDS_REGION','BEDS_PER_1000','CAPACITY','POS'),
    ('SCB','POPULATION','CAPACITY','NEUTRAL'),
    ('REGFIN','NET_COST','COST','NEG')  -- adjust if your NET_COST has a different source_system
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
    on m.source_system = d.source_system
   and m.source_indicator_code = d.source_indicator_code
)

select * from final
