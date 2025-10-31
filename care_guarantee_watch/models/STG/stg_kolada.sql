-- models/stg/stg_kolada.sql
{{ config(materialized='view') }}

with prepared as (
  select
    "Område"                as region_name_raw,
    "Kön"                   as gender_raw,
    "Nyckeltalsid"          as indicator_code,
    "Nyckeltal"             as indicator_name,
    "Nyckeltalsbeskrivning" as indicator_description,
    cast("2019" as float) as y2019,
    cast("2020" as float) as y2020,
    cast("2021" as float) as y2021,
    cast("2022" as float) as y2022,
    cast("2023" as float) as y2023,
    cast("2024" as float) as y2024
  from {{ source('RAW_DATA','KOLADA_RAW') }}
),

unpivoted as (
  select
    region_name_raw,
    gender_raw,
    indicator_code,
    indicator_name,
    indicator_description,
    to_number(regexp_replace(year_col, '[^0-9]', '')) as year,
    val::float as value
  from prepared
  unpivot exclude nulls ( val for year_col in (y2019, y2020, y2021, y2022, y2023, y2024) )
),

cleaned as (
  select
    region_name_raw,

    /* 1) remove leading 'Region' or 'Regionen' (case-insensitive via parameters='i')
       2) trim spaces
       3) remove one trailing 's' (genitive) */
    case
      when region_name_raw ilike 'Alla regioner%' then null
      else
        regexp_replace(                                     -- step 3
          regexp_replace(                                   -- steps 1 & 2
            trim(region_name_raw),
            '^(region(en)?[[:space:]]+)',                   -- pattern
            '', 1, 1, 'i'                                   -- parameters='i' => case-insensitive
          ),
          's$',''                                           -- drop a single trailing s
        )
    end as region_name,

    gender_raw,
    indicator_code,
    indicator_name,
    indicator_description,
    year,
    value
  from unpivoted
)

select
  region_name_raw,
  region_name,
  year,
  'KOLADA' as source_system,
  lower(trim(indicator_code)) as source_indicator_code,
  trim(indicator_name)        as indicator_name,
  value,
  case
    when lower(trim(gender_raw)) in ('k','kvinna','kvinnor') then 'F'
    when lower(trim(gender_raw)) in ('m','man','män')        then 'M'
    when lower(trim(gender_raw)) in ('totalt','alla','all') or gender_raw is null then 'ALL'
    else 'ALL'
  end as gender
from cleaned
where region_name is not null
  
