{{ config(materialized='table') }}

with src as (
  select
    region_code_int,
    region_code_2,
    region_name,
    region_name_raw,
    geom
  from {{ ref('stg_dim_region') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['region_code_int']) }} as region_key,
  region_code_int,
  region_code_2,
  region_name,

from src