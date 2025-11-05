{{ config(materialized='view') }}

with cap as (
  select
    c.region_key,
    c.year,
    d.topic,                -- CAPACITY/COST/etc.
    d.source_indicator_code as metric_code,
    d.indicator_name,
    c.value,
    c.unit
  from {{ ref('fct_capacity_yearly') }} c
  join {{ ref('dim_indicator') }} d on d.indicator_key = c.indicator_key
),
wide as (
  select
    region_key,
    year,
    max(case when metric_code='BEDS_PER_1000' then value end) as beds_per_1000,
    sum(case when topic='STAFF_HEADCOUNT' then value end)     as staff_headcount_total,  -- if you tagged headcount indicators under a specific code set
    max(case when metric_code='POPULATION' then value end)    as population,
    max(case when topic='COST' then value end)                as any_cost_value          -- optional; or pick specific codes
  from cap
  group by 1,2
),
derived as (
  select
    region_key,
    year,
    beds_per_1000,
    staff_headcount_total,
    population,
    any_cost_value,
    case when population > 0 then any_cost_value / population end as cost_per_capita
  from wide
)
select * from derived