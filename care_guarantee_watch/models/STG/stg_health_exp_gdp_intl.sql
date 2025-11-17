{{ config(materialized='view', schema='STG') }}

with src as (
  select
      trim(REFERENCE_AREA)                       as country_name,
      try_to_number(TIME_PERIOD)                      as year,
      -- convert "7,3" -> 7.3 and cast
      try_to_decimal(replace(OBS_VALUE, ',', '.'), 10, 2) as value,
      UNIT_OF_MEASURE               as unit
  from {{ source('RAW_DATA','HEALTH_EXPENDITURE_INTL_RAW') }}
  where country_name is not null
    and year is not null
)

select
    country_name,
    year,
    value::float                                           as value,
    'OECD_HEALTH'                                          as source_system,
    'HEALTH_EXPENDITURE_PCT_GDP'                           as source_indicator_code,
    'Health expenditure (% of GDP)'                        as indicator_name,
    unit
from src