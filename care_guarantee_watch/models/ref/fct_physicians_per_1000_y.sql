{{ config(materialized='table') }}

with src as (
  select * from {{ ref('ref_physicians_per_1000_y') }}
)

select
  region_key,
  date_key,
  indicator_key,
  year,
  value
from src
where region_key is not null
  and date_key   is not null
  and indicator_key is not null
