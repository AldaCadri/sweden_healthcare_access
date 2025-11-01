{{ config(materialized='table') }}

with base as (
  select column1 as gender_code, column2 as gender_name from (
    values ('F','Kvinna'), ('M','Man'), ('ALL','Alla')
  )
)
select
  {{ dbt_utils.generate_surrogate_key(['gender_code']) }} as gender_key,
  gender_code,
  gender_name
from base