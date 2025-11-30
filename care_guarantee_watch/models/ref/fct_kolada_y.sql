{{ config(materialized='table') }}

-- 1) Source slice (keep only non-null values)
with src as (
  select
    trim(region_name)               as region_name_raw,
    upper(trim(gender))             as gender_code,          -- 'F','M','ALL'
    source_system,
    source_indicator_code           as indicator_code,
    year::int                       as year,
    value::float                    as value
  from {{ ref('stg_kolada') }}
  where value is not null
),

-- 2) Normalize incoming names 
src_norm as (
  select
    upper(regexp_replace(regexp_replace(region_name_raw, '^\\s*REGION\\s+', ''), '\\s+LÄN\\s*$', '')) as name_norm,
    *
  from src
),

-- 3) Inline mapping table for problematic variants (UPPERCASE on both sides)
name_map as (
  select column1::string as bad_name_u, column2::string as good_name_u
  from values
    ('JÄMTLAND HÄRJEDALEN',   'JÄMTLAND'),
    ('JÖNKÖPINGS LÄN',        'JÖNKÖPING'),
    ('SÖRMLAND',              'SÖDERMANLAND'),
    ('VÄSTRA GÖTALANDSREGIONEN','VÄSTRA GÖTALAND'),
    ('STOCKHOLMS LÄN',        'STOCKHOLM'),
    ('UPPSALA LÄN',           'UPPSALA'),
    ('SÖDERMANLANDS LÄN',     'SÖDERMANLAND'),
    ('ÖSTERGÖTLANDS LÄN',     'ÖSTERGÖTLAND'),
    ('KRONOBERGS LÄN',        'KRONOBERG'),
    ('KALMAR LÄN',            'KALMAR'),
    ('GOTLANDS LÄN',          'GOTLAND'),
    ('BLEKINGE LÄN',          'BLEKINGE'),
    ('SKÅNE LÄN',             'SKÅNE'),
    ('HALLANDS LÄN',          'HALLAND'),
    ('VÄSTRA GÖTALANDS LÄN',  'VÄSTRA GÖTALAND'),
    ('VÄRMLANDS LÄN',         'VÄRMLAND'),
    ('ÖREBRO LÄN',            'ÖREBRO'),           
    ('VÄSTMANLANDS LÄN',      'VÄSTMANLAND'),
    ('DALARNAS LÄN',          'DALARNA'),
    ('GÄVLEBORGS LÄN',        'GÄVLEBORG'),
    ('VÄSTERNORRLANDS LÄN',   'VÄSTERNORRLAND'),
    ('JÄMTLANDS LÄN',         'JÄMTLAND'),
    ('VÄSTERBOTTENS LÄN',     'VÄSTERBOTTEN'),
    ('NORRBOTTENS LÄN',       'NORRBOTTEN')
),

-- 4) Apply mapping when present
src_mapped as (
  select
    coalesce(m.good_name_u, s.name_norm) as region_name_norm,
    s.gender_code,
    s.source_system,
    s.indicator_code,
    s.year,
    s.value
  from src_norm s
  left join name_map m
    on m.bad_name_u = s.name_norm
),

-- 5) Prepare dim_region with the same normalizer
dim_r as (
  select
    region_key,
    upper(regexp_replace(regexp_replace(trim(region_name), '^\\s*REGION\\s+', ''), '\\s+LÄN\\s*$', '')) as region_name_norm
  from {{ ref('dim_region') }}
),

-- 6) Resolve keys
joined as (
  select
    r.region_key,
    g.gender_key,
    i.indicator_key,
    s.year,
    s.value
  from src_mapped s
  left join dim_r r
    on r.region_name_norm = s.region_name_norm
  left join {{ ref('dim_gender') }} g
    on g.gender_code = s.gender_code
  left join {{ ref('dim_indicator') }} i
    on upper(trim(i.source_system)) = upper(trim(s.source_system))
   and upper(trim(i.source_indicator_code)) = upper(trim(s.indicator_code))
  where r.region_key is not null
),

-- 7) Dedupe exact grain collisions (keep first non-null value)
dedup as (
  select region_key, year, gender_key, indicator_key, value
  from (
    select
      region_key, year, gender_key, indicator_key, value,
      row_number() over (
        partition by region_key, year, gender_key, indicator_key
        order by case when value is null then 1 else 0 end, year desc
      ) as rn
    from joined
  )
  where rn = 1
)

select region_key, year, gender_key, indicator_key, value
from dedup