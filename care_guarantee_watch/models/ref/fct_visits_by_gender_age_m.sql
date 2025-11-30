{{ config(materialized='table') }}

with src as (
  select
    trim(REGION_NAME)                   as region_name_raw,
    upper(trim(GENDER))                 as gender_code,          -- 'F','M','ALL'
    trim(AGE_GROUP)                     as age_group_label,
    SOURCE_SYSTEM                       as source_system,
    SOURCE_INDICATOR_CODE               as indicator_code,
    PERIOD_DATE                         as period_date,
    VALUE::float                        as value
  from {{ ref('stg_tid_konalder') }}
  where VALUE is not null
),

-- Normalize incoming region names (strip "Region " prefix and trailing " län", uppercase)
src_norm as (
  select
    upper(
      regexp_replace(
        regexp_replace(region_name_raw, '^\\s*REGION\\s+', ''),
        '\\s+LÄN\\s*$', ''
      )
    )                                     as name_norm,
    *
  from src
),


name_map as (
  select column1::string as bad_name_u, column2::string as good_name_u
  from values
    ('JÄMTLAND HÄRJEDALEN','JÄMTLAND')
),

-- Apply mapping
src_mapped as (
  select
    coalesce(m.good_name_u, s.name_norm)  as region_name_norm,
    s.gender_code,
    s.age_group_label,
    s.source_system,
    s.indicator_code,
    s.period_date,
    s.value
  from src_norm s
  left join name_map m
    on m.bad_name_u = s.name_norm
),

-- dim_region prepared with the same normalizer
dim_r as (
  select
    region_key,
    upper(
      regexp_replace(
        regexp_replace(trim(region_name), '^\\s*REGION\\s+', ''),
        '\\s+LÄN\\s*$', ''
      )
    ) as region_name_norm
  from {{ ref('dim_region') }}
)

select
  r.region_key,
  to_number(to_char(date_trunc('month', s.period_date), 'YYYYMMDD')) as date_key,
  g.gender_key,
  a.age_key,
  i.indicator_key,
  s.value,
  cast(null as varchar) as unit
from src_mapped s
left join dim_r r
  on r.region_name_norm = s.region_name_norm
left join {{ ref('dim_gender') }}      g  on g.gender_code      = s.gender_code
left join {{ ref('dim_age_group') }}   a  on a.age_group_label  = s.age_group_label
left join {{ ref('dim_indicator') }}   i
  on upper(trim(i.source_system))        = upper(trim(s.source_system))
 and upper(trim(i.source_indicator_code)) = upper(trim(s.indicator_code))
where r.region_key is not null