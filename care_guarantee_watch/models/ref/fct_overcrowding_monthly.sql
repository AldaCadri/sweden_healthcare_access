{{ config(materialized='table') }}

with src as (
  select *
  from {{ ref('stg_overcrowding') }}
),

-- explicit alias mappings (extend as you find more)
name_map as (
  select column1::varchar as bad_name, column2::varchar as good_name
  from values
    ('Sörmland', 'Södermanland'),
    ('Jönköpings', 'Jönköping'),
    ('Jämtland Härjedalen', 'Jämtland')
),

-- normalize source names and apply mapping
s_norm as (
  select
    -- apply explicit mapping first, else keep original
    coalesce(m.good_name, s.region_name)                                as region_name_mapped,
    -- strip common prefixes/suffixes & normalize spacing/case
    upper(
      regexp_replace(
        regexp_replace(
          regexp_replace(trim(coalesce(m.good_name, s.region_name)), '^Region\\s+', ''), -- drop "Region "
          '\\s+län$', ''                                                                  -- drop " län"
        ),
        '\\s+', ' '                                                                       -- collapse spaces
      )
    ) as region_name_norm,
    s.*
  from src s
  left join name_map m
    on upper(s.region_name) = upper(m.bad_name)
),

-- normalize dim names the same way
r_norm as (
  select
    upper(
      regexp_replace(
        regexp_replace(
          regexp_replace(trim(region_name), '^Region\\s+', ''),
          '\\s+län$', ''
        ),
        '\\s+', ' '
      )
    ) as region_name_norm,
    region_key
  from {{ ref('dim_region') }}
)

select
  r.region_key,
  to_number(to_char(date_trunc('month', s.period_month),'YYYYMMDD')) as date_key,
  i.indicator_key,
  s.value::float as value
from s_norm s
left join r_norm r
  on s.region_name_norm = r.region_name_norm
left join {{ ref('dim_indicator') }} i
  on i.source_system = s.source_system
 and i.source_indicator_code = s.source_indicator_code