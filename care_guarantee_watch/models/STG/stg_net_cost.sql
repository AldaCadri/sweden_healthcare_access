{{ config(materialized='view') }}

WITH src AS (
  SELECT *
  FROM {{ source('RAW_DATA','NET_COST_RAW') }}
),

filtered AS (
  SELECT
    TRIM("Enhetsnamn")          AS region_name,            -- text
    /* If Regionskod is VARCHAR, keep TRY_TO_NUMBER; if it's NUMBER already, use CAST or keep as-is */
    TRY_TO_NUMBER("Regionskod") AS region_code,            -- safe if text; OK if numeric too
    TRIM("Enhetstyp")           AS unit_type,              -- expect 'Region'
    TRIM("Kön/Totalt")          AS gender_raw,
    TRIM("Ålder")               AS age_raw,

    /* 🔧 Don't TRY_ on numeric: Mätperiod is NUMBER in your preview */
    "Mätperiod"::INT            AS year,                   -- was TRY_TO_NUMBER("Mätperiod")

    TRIM("Måttenhet")           AS unit_raw,

    /* 🔧 Värde is NUMBER already; just cast (or even leave it) */
    "Värde"::FLOAT              AS value,                  -- was TRY or text handling

    COALESCE(NULLIF(TRIM("Titel"),''), TRIM("Diagramrubrik")) AS indicator_name_raw,
    TRIM("Register/källa")      AS register_kalla
  FROM src
  WHERE TRIM("Enhetstyp") = 'Region'
),

typed AS (
  SELECT
    region_code,
    region_name,
    year,
    value,
    NULL::DATE                    AS period_month,
    'YEAR'                        AS period_granularity,
    indicator_name_raw            AS indicator_name,
    LOWER(REGEXP_REPLACE(indicator_name_raw, '[^A-Za-z0-9]+', '_')) AS source_indicator_code,
    CASE
      WHEN LOWER(gender_raw) IN ('k','kvinna','kvinnor') THEN 'F'
      WHEN LOWER(gender_raw) IN ('m','man','män')        THEN 'M'
      ELSE 'ALL'
    END                           AS gender,
    NULLIF(TRIM(age_raw), '')     AS age_group,
    NULLIF(TRIM(unit_raw), '')    AS unit,
    NULLIF(TRIM(register_kalla), '') AS source_register
  FROM filtered
)

SELECT
  region_code,
  region_name,
  year,
  value,
  period_month,
  period_granularity,
  'NET_COST'              AS source_system,
  source_indicator_code,
  indicator_name,
  gender,
  age_group,
  unit
FROM typed
WHERE year IS NOT NULL
  AND region_code IS NOT NULL
  AND region_name IS NOT NULL
  AND value IS NOT NULL