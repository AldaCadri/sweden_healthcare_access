-- models/STG/stg_population.sql
{{ config(materialized='view') }}

-- 0) Point to RAW JSON
WITH raw_doc AS (
  SELECT PAYLOAD AS j
  FROM RAW_DATA.SCB_POPULATION_RAW       
),

-- 1) Find the index of Region and Year in columns[] 
col_idx AS (
  SELECT
    MAX(IFF(c.value:code::string ILIKE 'Region', c.index, NULL)) AS idx_region,
    MAX(IFF(c.value:code::string ILIKE 'Tid'   , c.index, NULL)) AS idx_year
  FROM raw_doc r,
       LATERAL FLATTEN(input => r.j:columns) c
),

-- 2) Flatten data[] to rows, project keys by index, get the observation
data_rows AS (
  SELECT
    d.value:key                           AS key_arr,     -- ARRAY: [Region, Year]
    d.value:values[0]::string             AS obs_raw
  FROM raw_doc r,
       LATERAL FLATTEN(input => r.j:data) d
),

projected AS (
  SELECT
    key_arr[i.idx_region]::string         AS region_code_raw,   -- e.g., "01","03",...
    key_arr[i.idx_year]::string           AS year_raw,          -- e.g., "2024"
    obs_raw
  FROM data_rows, col_idx i
),

normalized AS (
  SELECT
    TRY_TO_NUMBER(region_code_raw)                        AS region_code,         -- 1,3,4,...
    LPAD(region_code_raw, 2, '0')                         AS region_code_str,     -- "01","03",...
    year_raw::NUMBER                                      AS year,
    TRY_TO_NUMBER(obs_raw)                                AS population,

    
    'ALL'                                                 AS gender,
    CAST(NULL AS VARCHAR)                                 AS age_group
  FROM projected
  WHERE obs_raw IS NOT NULL
)

SELECT
  region_code,
  region_code_str,
  year,
  population,
  gender,
  age_group
FROM normalized
WHERE year IS NOT NULL