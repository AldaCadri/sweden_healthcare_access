{{ config(materialized='table') }}

-- Add ALL monthly sources that have period_month here
with minmax as (
  select
    date_trunc('month', least(
      (select min(period_month) from {{ ref('stg_dataexport') }}     where period_month is not null),
      (select min(period_month) from {{ ref('stg_overcrowding') }}   where period_month is not null),
      (select min(period_date) from {{ ref('stg_tid_konalder') }}   where  period_date is not null),
      (select min(period_month) from {{ ref('stg_tid_diagnos') }}    where period_month is not null)
    )) as start_month,
    date_trunc('month', greatest(
      (select max(period_month) from {{ ref('stg_dataexport') }}     where period_month is not null),
      (select max(period_month) from {{ ref('stg_overcrowding') }}   where period_month is not null),
      (select max(period_date) from {{ ref('stg_tid_konalder') }}   where period_date is not null),
      (select max(period_month) from {{ ref('stg_tid_diagnos') }}    where period_month is not null)
    )) as end_month
),

-- Generate enough rows (1000 months ~= 83 years; bump if you need more)
gen as (
  select
    dateadd(month, seq4(), (select start_month from minmax)) as month_date
  from table(generator(rowcount => 1000))
),

spine as (
  select month_date
  from gen
  where month_date <= (select end_month from minmax)
)

select
  to_number(to_char(month_date, 'YYYYMMDD')) as date_key,  -- 1st day of month
  month_date,
  to_number(to_char(month_date,'YYYY')) as year,
  to_number(to_char(month_date,'MM'))   as month_no,
  to_char(month_date,'YYYY-MM')         as year_month_label
from spine
order by month_date