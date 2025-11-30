{{ config(materialized='view') }}

WITH src AS (
  SELECT *
  FROM {{ source('RAW_DATA','OVERCROWDING_RAW') }}
),

-- 1) Parse Swedish month + year from Category
parsed AS (
  SELECT
    TRIM("CATEGORY") AS category,
    CASE
      WHEN LOWER("CATEGORY") LIKE 'januari %'   THEN 1
      WHEN LOWER("CATEGORY") LIKE 'februari %'  THEN 2
      WHEN LOWER("CATEGORY") LIKE 'mars %'      THEN 3
      WHEN LOWER("CATEGORY") LIKE 'april %'     THEN 4
      WHEN LOWER("CATEGORY") LIKE 'maj %'       THEN 5
      WHEN LOWER("CATEGORY") LIKE 'juni %'      THEN 6
      WHEN LOWER("CATEGORY") LIKE 'juli %'      THEN 7
      WHEN LOWER("CATEGORY") LIKE 'augusti %'   THEN 8
      WHEN LOWER("CATEGORY") LIKE 'september %' THEN 9
      WHEN LOWER("CATEGORY") LIKE 'oktober %'   THEN 10
      WHEN LOWER("CATEGORY") LIKE 'november %'  THEN 11
      WHEN LOWER("CATEGORY") LIKE 'december %'  THEN 12
      ELSE NULL
    END AS month_no,
    TRY_TO_NUMBER(REGEXP_SUBSTR("CATEGORY", '[0-9]{4}')) AS year,

    /* list only the region columns needed
    "Region Blekinge",
    "Region Dalarna",
    "Region Gotland",
    "Region Gävleborg",
    "Region Halland",
    "Region Jämtland Härjedalen",
    "Region Jönköpings län",
    "Region Kalmar län",
    "Region Kronoberg",
    "Region Norrbotten",
    "Region Skåne",
    "Region Stockholm",
    "Region Sörmland",
    "Region Uppsala",
    "Region Värmland",
    "Region Västerbotten",
    "Region Västernorrland",
    "Region Västmanland",
    "Region Örebro län",
    "Region Östergötland",
    "Västra Götalandsregionen"
  FROM src
),

-- 2) Proper month date
dated AS (
  SELECT
    year,
    month_no,
    TRY_TO_DATE(year || '-' || LPAD(month_no,2,'0') || '-01', 'YYYY-MM-DD') AS period_month,

    /* pass through the same region columns */
    "Region Blekinge",
    "Region Dalarna",
    "Region Gotland",
    "Region Gävleborg",
    "Region Halland",
    "Region Jämtland Härjedalen",
    "Region Jönköpings län",
    "Region Kalmar län",
    "Region Kronoberg",
    "Region Norrbotten",
    "Region Skåne",
    "Region Stockholm",
    "Region Sörmland",
    "Region Uppsala",
    "Region Värmland",
    "Region Västerbotten",
    "Region Västernorrland",
    "Region Västmanland",
    "Region Örebro län",
    "Region Östergötland",
    "Västra Götalandsregionen"
  FROM parsed
),

-- 3) Unpivot all region columns (exclude "Alla regioner")
u AS (
  SELECT
    period_month,
    year,
    month_no,
    TRIM(region_col) AS region_col,
    value
  FROM dated
  UNPIVOT INCLUDE NULLS (value FOR region_col IN (
    "Region Blekinge",
    "Region Dalarna",
    "Region Gotland",
    "Region Gävleborg",
    "Region Halland",
    "Region Jämtland Härjedalen",
    "Region Jönköpings län",
    "Region Kalmar län",
    "Region Kronoberg",
    "Region Norrbotten",
    "Region Skåne",
    "Region Stockholm",
    "Region Sörmland",
    "Region Uppsala",
    "Region Värmland",
    "Region Västerbotten",
    "Region Västernorrland",
    "Region Västmanland",
    "Region Örebro län",
    "Region Östergötland",
    "Västra Götalandsregionen"
  ))
),

-- 4) Clean region names
cleaned AS (
  SELECT
    year,
    month_no,
    period_month,
    CASE
      WHEN region_col = 'Västra Götalandsregionen' THEN 'Västra Götaland'
      ELSE REGEXP_REPLACE(
             REGEXP_REPLACE(region_col, '^Region\\s+', ''),
             '\\s+län$', ''
           )
    END AS region_name,
    CAST(value AS FLOAT) AS value      
  FROM u
)

SELECT
  year,
  month_no,
  period_month,
  region_name,
  value,
  'OVERCROWDING'        AS source_system,
  'displaced_patients'  AS source_indicator_code,
  'Overcrowding & displaced patients' AS indicator_name,
  'MONTH'               AS period_granularity,
  'ALL'                 AS gender
FROM cleaned
WHERE year IS NOT NULL
  AND month_no IS NOT NULL
  AND period_month IS NOT NULL
  AND region_name IS NOT NULL
  AND value IS NOT NULL