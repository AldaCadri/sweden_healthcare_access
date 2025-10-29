-- stg_kolada.sql
{{ config(materialized='view') }}

WITH prepared AS (
  SELECT
    "Område"                AS region_name,
    "Kön"                   AS gender,
    "Nyckeltalsid"          AS indicator_code,
    "Nyckeltal"             AS indicator_name,
    "Nyckeltalsbeskrivning" AS indicator_description,
    CAST("2019" AS FLOAT) AS y2019,
    CAST("2020" AS FLOAT) AS y2020,
    CAST("2021" AS FLOAT) AS y2021,
    CAST("2022" AS FLOAT) AS y2022,
    CAST("2023" AS FLOAT) AS y2023,
    CAST("2024" AS FLOAT) AS y2024
  FROM {{ source('RAW_DATA','KOLADA_RAW') }}
),

unpivoted AS (
  SELECT
    region_name,
    gender,
    indicator_code,
    indicator_name,
    indicator_description,
    TO_NUMBER(REGEXP_REPLACE(year_col, '[^0-9]', '')) AS year,
    val::FLOAT AS value
  FROM prepared
  UNPIVOT EXCLUDE NULLS ( val FOR year_col IN (y2019, y2020, y2021, y2022, y2023, y2024) )
)

SELECT
  region_name,
  year,
  'KOLADA' AS source_system,
  LOWER(TRIM(indicator_code)) AS source_indicator_code,
  TRIM(indicator_name)        AS indicator_name,
  value,
  CASE
    WHEN LOWER(TRIM(gender)) IN ('k','kvinna','kvinnor') THEN 'F'
    WHEN LOWER(TRIM(gender)) IN ('m','man','män')        THEN 'M'
    WHEN LOWER(TRIM(gender)) IN ('totalt','alla','all') OR gender IS NULL THEN 'ALL'
    ELSE 'ALL'
  END AS gender
FROM unpivoted
