{{ config(materialized='view') }}

-- 1) Map indicators to roles (now splits doctors vs nurses)
with roles as (
  select
    i.indicator_key,
    upper(trim(i.topic))                 as topic,
    upper(trim(i.source_system))         as src,
    upper(trim(i.source_indicator_code)) as code,
    i.indicator_name,
    case
      -- Population
      when src = 'SCB' and code = 'POPULATION' then 'POPULATION'

      -- Beds per 1000 inhabitants
      when src in ('BEDS_REGION','OECD_HEALTH') then 'BEDS_PER_1000'
      when upper(i.topic) = 'CAPACITY'
        and regexp_like(lower(i.indicator_name), '(vårdplatser|beds)') then 'BEDS_PER_1000'

      -- 👇 Staff headcount (split by profession)
      when src = 'MEDICAL_PERSONNEL'
        and regexp_like(lower(i.indicator_name), '(läkare)') then 'STAFF_DOCTORS'
      when src = 'MEDICAL_PERSONNEL'
        and regexp_like(lower(i.indicator_name), '(sjuksköterska|sjukskoterska|röntgensjuksköterska|rontgensjukskoterska)') then 'STAFF_NURSES'
      when upper(i.topic) = 'CAPACITY'
        and regexp_like(lower(i.indicator_name), '(läkare)') then 'STAFF_DOCTORS'
      when upper(i.topic) = 'CAPACITY'
        and regexp_like(lower(i.indicator_name), '(sjuksköterska|sjukskoterska|röntgensjuksköterska|rontgensjukskoterska)') then 'STAFF_NURSES'

      -- (Optional) catch-all for other staff lines you might want later
      when src = 'MEDICAL_PERSONNEL' or upper(i.topic) = 'CAPACITY' then 'STAFF_OTHER'

      -- Cost
      when upper(i.topic) = 'COST'
        and src = 'NET_COST' then 'COST_PER_CAPITA'
      when upper(i.topic) = 'COST' then 'COST_OTHER'

      -- Ambulance (volume + time)
      when regexp_like(lower(i.indicator_name), 'responstid för ambulans vid prio 1-larm')
            then 'AMBULANCE_TIME_MIN'
      when regexp_like(lower(i.indicator_name), 'ambulanslarm per invånare – prio1-larm|ambulanslarm per invånare - prio1-larm')
            then 'AMBULANCE_COUNT'

      else null
    end as role
  from {{ ref('dim_indicator') }} i
),

-- 2) Yearly capacity/resource metrics
cap as (
  select
    c.region_key,
    c.year,
    r.role,
    c.value::float as value
  from {{ ref('fct_capacity_yearly') }} c
  join roles r on r.indicator_key = c.indicator_key
  where r.role is not null
),

-- 3) Ambulance yearly (if stored in performance_yearly)
amb_year as (
  select
    f.region_key,
    f.year,
    r.role,
    f.value::float as value
  from {{ ref('fct_performance_yearly') }} f
  join roles r on r.indicator_key = f.indicator_key
  where r.role in ('AMBULANCE_COUNT','AMBULANCE_TIME_MIN')
),

-- 4) Overcrowding yearly = sum monthly displaced patients per region/year
overc_year as (
  select
    m.region_key,
    d.year                            as year,
    'OVERCROWDING_Y'                  as role,
    sum(m.value)::float               as value
  from {{ ref('fct_overcrowding_monthly') }} m
  join {{ ref('dim_date') }} d on d.date_key = m.date_key
  group by 1,2
),

-- 5) Union all yearly roles
all_year as (
  select * from cap
  union all
  select * from amb_year
  union all
  select * from overc_year
),

-- 6) Aggregate by region/year + now separate doctors vs nurses
agg as (
  select
    region_key,
    year,
    max(case when role = 'BEDS_PER_1000'      then value end) as beds_per_1000,

    -- 👇 separate counts
    sum(case when role = 'STAFF_DOCTORS'      then value end) as staff_doctors,
    sum(case when role = 'STAFF_NURSES'       then value end) as staff_nurses,
    sum(case when role in ('STAFF_DOCTORS','STAFF_NURSES') then value end) as staff_headcount_total,

    max(case when role = 'POPULATION'         then value end) as population,
    max(case when role = 'COST_PER_CAPITA'    then value end) as cost_per_capita,
    max(case when role = 'COST_OTHER'         then value end) as cost_other,
    max(case when role = 'AMBULANCE_COUNT'    then value end) as ambulance_count,
    max(case when role = 'AMBULANCE_TIME_MIN' then value end) as ambulance_time_min,
    max(case when role = 'OVERCROWDING_Y'     then value end) as displaced_patients_year
  from all_year
  group by 1,2
),

final as (
  select
    region_key,
    year,
    cast(year * 10000 + 101 as number(8,0)) as date_key,
    beds_per_1000,

    -- 👇 exposed separately + total
    staff_doctors,
    staff_nurses,
    staff_headcount_total,

    population,
    cost_per_capita,
    cost_other,
    ambulance_count,
    ambulance_time_min,
    displaced_patients_year,

    -- Ratios per 1,000 inhabitants
    case when population > 0 then staff_headcount_total / population * 1000 end as staff_per_1000,
    case when population > 0 then staff_doctors         / population * 1000 end as doctors_per_1000,
    case when population > 0 then staff_nurses          / population * 1000 end as nurses_per_1000
  from agg
)

select * from final