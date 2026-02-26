{% set current_year = var('year') | int %}
{% set current_month = var('month') | int %}

{{ config(
    materialized = 'incremental',
    unique_key = ['revenue_year', 'revenue_month', 'service_type', 'pickup_location_id'],
    incremental_strategy = 'delete+insert',
    description = 'Gold layer: Daily trip volume by service type.'
) }}

with trip_counts as (
    select 
        pickup_location_id as location_id,
        count(*) as total_trips,
        sum(total_amount) as total_amount,
        'Yellow' as service_type
    from {{ ref('stg_yellow_trips') }}
    group by 1
    
    union all
    
    select 
        pickup_location_id as location_id,
        count(*) as total_trips,
        sum(total_amount) as total_amount,
        'Green' as service_type
    from {{ ref('stg_green_trips') }}
    group by 1
)

select 
    *
from trip_counts