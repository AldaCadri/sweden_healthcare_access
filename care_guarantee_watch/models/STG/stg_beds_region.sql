{{ config(materialized='view') }}

WITH base AS (
  SELECT
    TRIM("Enhetsnamn")                          AS region_name,
    "Enhetstyp"                                 AS unit_type,
    "Regionskod"                                AS region_code,
    "Indikator-Id"       AS indicator_code,
    "Titel"              AS indicator_name,
    "Diagramrubrik"    AS indicator_desc,
    "Mätperiod"              AS year,
    "Värde"                              AS value,
    TRIM("Register/källa")                  AS register_source
  FROM {{ source('RAW_DATA', 'BEDS_REGION_RAW') }}
),

clean AS (
  SELECT
    region_name,
    region_code,        
    year,
    value,                               
    indicator_code,
    indicator_name,
    indicator_desc,

  FROM base
  WHERE year IS NOT NULL
)

SELECT
  region_name,
    region_code,        
    year,
    value,                                
    indicator_code,
    indicator_name,
    indicator_desc,
FROM clean