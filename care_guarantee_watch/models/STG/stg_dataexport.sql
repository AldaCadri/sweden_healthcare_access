{{ config(materialized='view') }}

WITH base AS (
  SELECT
    "Enhetsnamn"         AS unit_name,
    "Enhetstyp"          AS unit_type,
    "Regionskod"         AS region_code,
    "Enhetskod"          AS unit_code,
    "Kön/Totalt"         AS gender_raw,
    "Indikator-Id"       AS indicator_code,
    "Titel"              AS indicator_name,
    "Måttenhet"          AS unit_raw,
    TRIM("Mätperiod")    AS period_raw,
    "Värde"              AS value_raw,
    "Täljare"            AS numerator_raw,
    "Nämnare/antal fall" AS denominator_raw
  FROM {{ source('RAW_DATA','DATAEXPORT_RAW') }}
  WHERE "Enhetstyp" = 'Region'        -- only Regions
),

period_parsed AS (
  SELECT
    b.*,
    CASE
      WHEN REGEXP_LIKE(period_raw, '^[0-9]{4}-[0-9]{2}$') THEN 'MONTH'
      WHEN REGEXP_LIKE(period_raw, '^[0-9]{4}$')          THEN 'YEAR'
      ELSE 'UNKNOWN'
    END AS period_granularity,
    TO_NUMBER(REGEXP_SUBSTR(period_raw, '^[0-9]{4}')) AS year,
    CASE
      WHEN REGEXP_LIKE(period_raw, '^[0-9]{4}-[0-9]{2}$')
        THEN TRY_TO_DATE(period_raw, 'YYYY-MM')
      ELSE NULL
    END AS period_month
  FROM base b
),

numeric_parsed AS (
  SELECT
    unit_name, unit_type, region_code, unit_code,
    gender_raw, indicator_code, indicator_name, unit_raw,
    period_raw, period_granularity, year, period_month,

    -- keep decimals robustly (handles %, spaces, decimal comma)
    TRY_TO_DOUBLE(
      REPLACE(REGEXP_REPLACE(value_raw::VARCHAR, '[^0-9.,-]', ''), ',', '.')
    ) AS value,

    TRY_TO_DOUBLE(REGEXP_REPLACE(numerator_raw::VARCHAR,   '[^0-9.-]', '')) AS numerator,
    TRY_TO_DOUBLE(REGEXP_REPLACE(denominator_raw::VARCHAR, '[^0-9.-]', '')) AS denominator
  FROM period_parsed
),

aug AS (
  SELECT
    n.*,
    -- does this region+indicator+year+gender have monthly rows?
    MAX(IFF(period_granularity = 'MONTH', 1, 0))
      OVER (PARTITION BY region_code, LOWER(TRIM(indicator_code)), year, 
                         UPPER(TRIM(gender_raw))) AS has_monthly,

    -- unified timeline field: month if monthly, else Jan-01 of year (for slicers/joins)
    IFF(period_granularity = 'MONTH', period_month, TO_DATE(year || '-01-01')) AS period_filter_date,

    -- readable label: YYYY-MM for months, YYYY for years
    IFF(period_granularity = 'MONTH', TO_VARCHAR(period_month, 'YYYY-MM'), TO_VARCHAR(year)) AS period_filter_label
  FROM numeric_parsed n
)

SELECT
  TRIM(unit_name)                         AS region_name,
  region_code,
  unit_code,
  year,
  period_month,                           -- NULL for YEAR rows (no fake months)
  period_granularity,                     -- 'MONTH' or 'YEAR'
  period_filter_date,                     -- month if monthly, else year-start
  period_filter_label,                    -- 'YYYY-MM' or 'YYYY'
  'DATAEXPORT'                            AS source_system,
  LOWER(TRIM(indicator_code))             AS source_indicator_code,
  TRIM(indicator_name)                    AS indicator_name,
  value,
  CASE
    WHEN LOWER(TRIM(gender_raw)) IN ('k','kvinna','kvinnor') THEN 'F'
    WHEN LOWER(TRIM(gender_raw)) IN ('m','man','män')        THEN 'M'
    WHEN gender_raw IS NULL OR gender_raw = '' OR LOWER(TRIM(gender_raw)) IN ('totalt','alla','all') THEN 'ALL'
    ELSE 'ALL'
  END                                     AS gender,
  NULLIF(TRIM(unit_raw), '')              AS unit,
  numerator,
  denominator
FROM aug
-- drop YEAR rows when monthly exists for same region+indicator+year+gender
WHERE NOT (period_granularity = 'YEAR' AND has_monthly = 1)
  AND year IS NOT NULL

