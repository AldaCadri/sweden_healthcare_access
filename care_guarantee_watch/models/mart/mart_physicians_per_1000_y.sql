{{ config(materialized='view') }}

-- 1️ Pull the core fact
with f as (
    select
        f.region_key,
        f.date_key,
        f.indicator_key,
        f.year,
        f.value
    from {{ ref('fct_physicians_per_1000_y') }} f
),

-- 2️ Join lookup dims for descriptive columns
joined as (
    select
        f.region_key,
        r.region_name,
        f.date_key,
        f.year,
        f.indicator_key,
        coalesce(i.indicator_name, 'Doctors per 1000 inhabitants') as indicator_name,
        coalesce(i.topic, 'CAPACITY')                             as topic,
        coalesce(i.source_system, 'OECD_HEALTH')                  as source_system,
        f.value,
        'Per 1000 inhabitants'                                    as unit
    from f
    left join {{ ref('dim_region') }}    r on r.region_key = f.region_key
    left join {{ ref('dim_date') }}      d on d.date_key   = f.date_key
    left join {{ ref('dim_indicator') }} i on i.indicator_key = f.indicator_key
)

select *
from joined