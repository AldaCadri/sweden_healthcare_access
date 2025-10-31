{{ config(materialized='view') }}

WITH src AS (
  SELECT
    "Year"                                                      AS year,
    "Totala hälso- och sjukvårdsutgifter, mnkr"  AS health_exp_mnkr,
   "BNP till marknadspris, mnkr"                AS gdp_mnkr,
    "BNP relationstal, procent"                 AS health_share_pct
  FROM {{ source('RAW_DATA','EXPENDITURE_GDP_RAW') }}
  WHERE "Year" IS NOT NULL
),

-- turn the three columns into rows (long format)
u AS (
  SELECT year, 'health_exp_mnkr' AS source_indicator_code,
         'Totala hälso- och sjukvårdsutgifter' AS indicator_name,
         health_exp_mnkr AS value, 'mnkr' AS unit
  FROM src
  UNION ALL
  SELECT year, 'gdp_mnkr',
         'BNP till marknadspris',
         gdp_mnkr, 'mnkr'
  FROM src
  UNION ALL
  SELECT year, 'health_exp_share_pct',
         'Hälso-/sjukvård som andel av BNP',
         health_share_pct, 'percent'
  FROM src
)

SELECT
  'Sverige'                 AS region_name,
  year,
  TO_DATE(year || '-01-01') AS period_date,
  'YEAR'                    AS period_granularity,
  'SCB_NATIONAL'            AS source_system,
  source_indicator_code,
  indicator_name,
  unit,
  value
FROM u
WHERE value IS NOT NULL
ORDER BY year, source_indicator_code