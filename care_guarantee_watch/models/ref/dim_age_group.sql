{{ config(materialized='table') }}

with src as (
  select distinct age_group
  from {{ ref('stg_tid_konalder') }}
  where age_group is not null
),
norm as (
  select
    trim(age_group) as age_group_label,
    try_to_number(regexp_substr(age_group, '^[0-9]+')) as sort_from
  from src
)
select
  {{ dbt_utils.generate_surrogate_key(['age_group_label']) }} as age_key,
  age_group_label,
  coalesce(sort_from, 999) as sort_key
from norm