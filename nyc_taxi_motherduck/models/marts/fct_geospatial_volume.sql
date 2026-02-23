{% set current_year = var('year') | int %}
{% set current_month = var('month') | int %}

{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'service_type', 'revenue_year', 'revenue_month'],
    incremental_strategy = 'delete+insert',
    description = 'Gold layer: Geospatial trip volume for map visualizations.'
) }}

with trip_counts as (
    -- Reference Yellow Silver Layer
    select 
        pickup_location_id as location_id,
        count(*) as trip_count,
        'Yellow' as service_type
    from {{ ref('stg_yellow_trips') }} 
    where 1=1
    {% if is_incremental() %}
      -- Only look at the month we are currently processing
      and extract(year from pickup_datetime) = {{ current_year }}
      and extract(month from pickup_datetime) = {{ current_month }}
    {% endif %}
    group by 1
    
    union all
    
    -- Reference Green Silver Layer
    select 
        pickup_location_id as location_id,
        count(*) as trip_count,
        'Green' as service_type
    from {{ ref('stg_green_trips') }} 
    where 1=1
    {% if is_incremental() %}
      -- Only look at the month we are currently processing
      and extract(year from pickup_datetime) = {{ current_year }}
      and extract(month from pickup_datetime) = {{ current_month }}
    {% endif %}
    group by 1
),

zones as (
    -- Reference the Seed file instead of a staging table
    select 
        locationid as location_id,
        borough,
        zone
    from {{ ref('taxi_zone_lookup') }} 
)

select 
    z.location_id,
    z.borough,
    z.zone,
    t.service_type,
    {{ current_year }} as revenue_year,
    {{ current_month }} as revenue_month,
    sum(t.trip_count) as total_trips
from zones z
join trip_counts t on z.location_id = t.location_id
group by 1, 2, 3, 4, 5, 6
order by 4, 1