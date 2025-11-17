{{ config(materialized='view') }}

WITH src AS (
  SELECT
    "Year"        AS year_raw,
    "Month"       AS month_name_raw,                     -- Swedish month text (optional)
    "Metric"      AS metric_raw,
    "Value"       AS value_raw,
    "Region"      AS region_raw,
    "Diagnosgrupp" AS diagnosgrupp_raw,
    "PERIOD"      AS period_raw                          -- ISO 'YYYY-MM-DD'
  FROM {{ source('RAW_DATA','STAT_DIAGNOS_RAW') }}
),

typed AS (
  SELECT
    year_raw            AS year,
    TO_DATE(period_raw)                AS period_date,     -- Snowflake can parse ISO directly
    EXTRACT(MONTH FROM TO_DATE(period_raw)) AS month_number,

    TRIM(month_name_raw)               AS month_name,
    TRIM(region_raw)                   AS region_name,
    TRIM(diagnosgrupp_raw)             AS diagnosgrupp,
    TRIM(metric_raw)                   AS indicator_name,

    /* Convert '--' / '-' / '' to NULL, then parse keeping decimals (comma → dot) */
    CASE
      WHEN TRIM(value_raw) IN ('--','-','') THEN NULL
      ELSE TRY_TO_DOUBLE(REPLACE(REGEXP_REPLACE(value_raw::VARCHAR, '[^0-9.,-]', ''), ',', '.'))
    END                                AS value
  FROM src
),

labeled AS (
  SELECT
    *,
    /* Harmonize fields to align with your other STG models */
    'MONTH'                                         AS period_granularity,
    TO_DATE(year || '-' || LPAD(month_number::TEXT,2,'0') || '-01') AS period_month,
    /* infer unit from metric text (optional but handy) */
    CASE
      WHEN LOWER(indicator_name) LIKE '%minut%' THEN 'minuter'
      WHEN LOWER(indicator_name) LIKE '%antal%' THEN 'antal'
      ELSE NULL
    END                                            AS unit,
    LOWER(REGEXP_REPLACE(indicator_name, '\s+', '_')) AS source_indicator_code
  FROM typed
)

SELECT
  region_name,
  year,
  period_month,                   -- real month date
  period_granularity,             -- 'MONTH'
  'STAT_DIAGNOS'   AS source_system,
  source_indicator_code,
  indicator_name,
  diagnosgrupp,
  value,
  /* keep schema consistent across facts */
  'ALL'            AS gender,
  NULL::VARCHAR    AS age_group,
  unit
FROM labeled
WHERE year IS NOT NULL
  /* drop national totals if present */
  AND COALESCE(LOWER(region_name),'') NOT IN ('riket','sverige')