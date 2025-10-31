{{ config(materialized='view') }}

WITH src AS (
  SELECT
    "REF_AREA"                    AS country_raw,
    "Reference area"              AS country_name_raw,
    "OBS_VALUE"                   AS obs_value_raw,
    "STRUCTURE_NAME"              AS structure_name_raw,
    "Unit of measure"             AS unit_measure_raw,
    "TIME_PERIOD"                 AS year_raw
  FROM {{ source('RAW_DATA','BEDS_INTL_RAW') }}
),

cleaned AS (
  SELECT
    TRIM(country_raw)                       AS country_code,
    TRIM(country_name_raw)                  AS country_name,
    year_raw                                AS year,
    obs_value_raw                           AS beds_per_1000,
    'beds_per_1000_population'              AS source_indicator_code,
    'Hospital beds per 1,000 inhabitants'   AS indicator_name,
    NULLIF(TRIM(unit_measure_raw), '')      AS unit,
    'OECD_HEALTH'                           AS source_system
  FROM src
  WHERE obs_value_raw IS NOT NULL
)

SELECT
  country_code,
  country_name,
  year,
  source_system,
  source_indicator_code,
  indicator_name,
  unit,
  beds_per_1000 AS value
FROM cleaned
WHERE year IS NOT NULL
ORDER BY country_name, year