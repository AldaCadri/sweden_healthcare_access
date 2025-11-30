{{ config( materialized = 'view') }}

-- This view converts the region geometries into GeoJSON text
-- for export to Power BI 

select
    region_key,
    lpad(cast(region_code_int as varchar), 2, '0') as region_code,

    region_name,
    st_asgeojson(geom) as geojson
from {{ ref('dim_region') }}