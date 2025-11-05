{{ config(materialized='view') }}

-- 1) Map each indicator to a 'role' we want to show in the Resources view.
--    We use stable identifiers where we have them (SCB POPULATION, BEDS_REGION),
--    and Swedish name patterns as a safety net.

with roles as (
  select
    i.indicator_key,
    upper(trim(i.topic))                                  as topic,
    upper(trim(i.source_system))                          as src,
    upper(trim(i.source_indicator_code))                  as code,
    i.indicator_name,
    case
      -- Population
      when src = 'SCB' and code = 'POPULATION' then 'POPULATION'

      -- Beds per 1000 (regional + intl + name fallback)
      when src in ('BEDS_REGION','OECD_HEALTH') then 'BEDS_PER_1000'
      when upper(i.topic) = 'CAPACITY'
           and regexp_like(lower(i.indicator_name), '(vårdplatser|beds)') then 'BEDS_PER_1000'

      -- Staff headcount: match common professions in Swedish
      when src = 'MEDICAL_PERSONNEL' then 'STAFF_HEADCOUNT'
      when upper(i.topic) = 'CAPACITY'
           and regexp_like(lower(i.indicator_name),
              '(läkare|sjukskötersk|röntgensjukskötersk|barnmorska|tandläkar|tandhygienist|biomedicinsk|arbetsterapeut|audionom|fysioterapeut|psykolog|logoped|apotekare|receptarie|sjukhusfysiker|kurator|kiropraktor|naprapat|optiker|ortopedingenjör|underskötersk|psykoterapeut)') then 'STAFF_HEADCOUNT'

      -- Total annual cost: prefer specific 'total' or 'net cost of health care' wordings
      when upper(i.topic) = 'COST'
           and regexp_like(lower(i.indicator_name),
              '(totala hälso-.*utgifter|nettokostnad.*hälso.*sjukvård)') then 'COST_TOTAL'

      -- Other cost metrics (kept if you want additional columns later)
      when upper(i.topic) = 'COST' then 'COST_OTHER'

      else null
    end as role
  from {{ ref('dim_indicator') }} i
),

-- 2) Bring yearly values from the unified capacity_yearly fact (it already includes
--    beds, staff headcount, population, and cost because we unioned those in REF).
cap as (
  select
    c.region_key,
    c.year,
    r.role,
    c.value
  from {{ ref('fct_capacity_yearly') }} c
  join roles r
    on r.indicator_key = c.indicator_key
  where r.role is not null
),

-- 3) Aggregate to one row per region/year.
agg as (
  select
    region_key,
    year,
    max(case when role = 'BEDS_PER_1000' then value end)           as beds_per_1000,
    sum(case when role = 'STAFF_HEADCOUNT' then value end)          as staff_headcount_total,
    max(case when role = 'POPULATION' then value end)               as population,
    max(case when role = 'COST_TOTAL' then value end)               as cost_total_sek,
    sum(case when role = 'COST_OTHER' then value end)               as cost_other_sek
  from cap
  group by 1,2
),

-- 4) Derive useful ratios.
final as (
  select
    region_key,
    year,
    beds_per_1000,
    staff_headcount_total,
    population,
    case when population > 0 then staff_headcount_total / population end as staff_per_capita,
    cost_total_sek as cost_per_capita
  from agg
)

select * from final