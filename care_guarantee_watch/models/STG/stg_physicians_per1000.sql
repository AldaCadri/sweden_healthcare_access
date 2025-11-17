{{ config(materialized='view') }}


    select 
        
        REGION_NAME as region_name,
        to_number(YEAR) as year,
        VALUE::float as value,
        'OECD_HEALTH' as source_system,
        'Doctors per 1000 inhabitants' as indicator_name,
        'Per 1000 inhabitants' as unit
    from {{ source('RAW_DATA', 'PHYSICIANSPER1000_RAW') }}


