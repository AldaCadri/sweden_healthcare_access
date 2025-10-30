{{ config(materialized='view') }}

WITH src AS (
  SELECT
    "År"::INT                          AS year,
    TRIM("Grupp")                      AS profession_group,
    TRIM("Område")                     AS area_raw,
    TRIM("Kön")                        AS gender_raw,
    {{ adapter.quote('86 Hälso­ och sjukvård') }}::FLOAT AS value
  FROM {{ source('RAW_DATA','MEDICAL_PERSONNEL_RAW') }}
),

-- keep only län (drop Riket and sjukvårdsregioner)
filtered AS (
  SELECT *
  FROM src
  WHERE area_raw <> 'Riket'
    AND LOWER(area_raw) NOT LIKE '%sjukvårdsregion%'
),

-- normalize gender + region name
typed AS (
  SELECT
    year,
    TRIM(
  REGEXP_REPLACE(
    REGEXP_REPLACE(area_raw, '\\s+län\\s*$', '', 1, 0, 'i'),
    's$', '', 1, 0, 'i'
  )
) AS region_name,
    CASE
      WHEN LOWER(gender_raw) IN ('män','man')        THEN 'M'
      WHEN LOWER(gender_raw) IN ('kvinnor','kvinna') THEN 'F'
      ELSE 'ALL'
    END AS gender,
    profession_group AS indicator_name,
    LOWER(REGEXP_REPLACE(profession_group, '[^A-Za-z0-9]+', '_')) AS source_indicator_code,
    value,
    NULL::DATE AS period_month,
    'YEAR'     AS period_granularity
  FROM filtered
)

SELECT
  region_name,
  year,
  value,
  period_month,            -- stays NULL (no fake months)
  period_granularity,      -- 'YEAR'
  'MEDICAL_PERSONNEL'      AS source_system,
  source_indicator_code,
  indicator_name,
  gender
FROM typed
WHERE year IS NOT NULL
  AND region_name IS NOT NULL
  AND value IS NOT NULL