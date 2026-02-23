{% set current_year = var('year') | int %}
{% set current_month = var('month') | int %}

{{ config(
    materialized = 'incremental',
    unique_key = ['service_type', 'pickup_day'],
    incremental_strategy = 'delete+insert',
    description = 'Gold layer: Daily trip volume by service type.'
) }}

with trips_data as (
    -- Reference Silver models and filter for the specific variable month/year
    select 
        'Yellow' as service_type,
        -- Use the standardized pickup_datetime from your silver layer
        date_trunc('day', pickup_datetime) as pickup_day
    from {{ ref('stg_yellow_trips') }}
    where 1=1
    {% if is_incremental() %}
      -- Only look at the month we are currently processing
      and extract(year from pickup_datetime) = {{ current_year }}
      and extract(month from pickup_datetime) = {{ current_month }}
    {% endif %}
    
    union all
    
    select 
        'Green' as service_type,
        date_trunc('day', pickup_datetime) as pickup_day
    from {{ ref('stg_green_trips') }}
    where 1=1
    {% if is_incremental() %}
      -- Only look at the month we are currently processing
      and extract(year from pickup_datetime) = {{ current_year }}
      and extract(month from pickup_datetime) = {{ current_month }}
    {% endif %}
),

daily_counts as (
    select
        service_type,
        pickup_day,
        count(*) as total_trips
    from trips_data
    group by 1, 2
)

select
    *
from daily_counts
order by service_type, pickup_day