{{ config(
  materialized = 'view',
  alias = 'stg_dim_region',
  unique_key = 'region_code_2'
) }}

with src as (
  select
      f.value:"properties" as props,
      f.value:"geometry"   as geom
  from {{ source('RAW_DATA', 'GEO_LAN_REGIONS_RAW') }} r,
       lateral flatten(input => r.PAYLOAD:"features") f
)

select
    try_to_number(props:"scb_lan_code_2"::string)        as region_code_int,
    lpad(props:"scb_lan_code_2"::string, 2, '0')         as region_code_2,

    -- keep original (genitive) as reference
    trim(props:"region_name_official"::string)           as region_name_raw,

    -- cleaned display name: drop ONE trailing "s" (no county’s base name ends with "s")
    regexp_replace(trim(props:"region_name_official"::string), 's$', '') as region_name,

    to_geography(geom)                                   as geom
from src
where props is not null