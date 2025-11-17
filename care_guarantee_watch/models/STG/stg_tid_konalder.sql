{{ config(materialized='view') }}

WITH src AS (
  SELECT
    "Year"                              AS year_raw,
    "Month"                             AS month_name_raw,
    "Metric"                            AS metric_raw,
    "Value"                             AS value_raw,
    "Region/landsting/akutmottagning"   AS region_raw,
    "Kön"                               AS gender_raw,
    "Ålder"                             AS age_raw,
    "PERIOD"                            AS period_raw
  FROM {{ source('RAW_DATA','STAT_KON_ALDER_RAW') }}
),

cleaned AS (
  SELECT
    year_raw                          AS year,
    INITCAP(TRIM(month_name_raw))                    AS month_name,
    TRIM(metric_raw)                                 AS metric_name,

    -- Value: handle commas, spaces
    TRY_TO_DOUBLE(REPLACE(REGEXP_REPLACE(value_raw::VARCHAR,'[^0-9.,-]',''), ',', '.')) AS value,

    TRIM(region_raw)                                 AS region_name,

    CASE
      WHEN LOWER(TRIM(gender_raw)) IN ('k','kvinna','kvinnor') THEN 'F'
      WHEN LOWER(TRIM(gender_raw)) IN ('m','man','män')        THEN 'M'
      ELSE 'ALL'
    END                                              AS gender,

    NULLIF(TRIM(age_raw), '')                        AS age_group,
    TO_DATE(period_raw) AS period_date,
    EXTRACT(MONTH FROM TO_DATE(period_raw)) AS month_number
  FROM src
  WHERE value_raw IS NOT NULL
)

SELECT
  year,
  month_number,
  month_name,
  period_date,
  'STAT_KON_ALDER'             AS source_system,
  metric_name                  AS indicator_name,
  LOWER(REGEXP_REPLACE(metric_name,'\\s+','_')) AS source_indicator_code,
  region_name,
  gender,
  age_group,
  value
FROM cleaned
WHERE year IS NOT NULL
ORDER BY region_name, year, month_number