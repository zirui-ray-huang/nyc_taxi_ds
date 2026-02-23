{% set current_year = var('year') | int %}
{% set current_month = var('month') | int %}

{{ config(
    materialized = 'incremental',
    unique_key = ['service_type', 'pickup_year', 'pickup_month', 'pickup_hour'],
    incremental_strategy = 'delete+insert',
    description = 'Gold layer: Hourly taxi statistics by service type and year/month.'
) }}

with trips_data as (
    -- Reference Silver models and filter for the specific variable month/year
    select 
        'Yellow' as service_type,
        extract(year from pickup_datetime) as pickup_year,
        extract(month from pickup_datetime) as pickup_month,
        extract(hour from pickup_datetime) as pickup_hour,
        passenger_count,
        total_amount,
        tip_amount,
        trip_distance
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
        extract(year from pickup_datetime) as pickup_year,
        extract(month from pickup_datetime) as pickup_month,
        extract(hour from pickup_datetime) as pickup_hour,
        passenger_count,
        total_amount,
        tip_amount,
        trip_distance
    from {{ ref('stg_green_trips') }}
    where 1=1
    {% if is_incremental() %}
      -- Only look at the month we are currently processing
      and extract(year from pickup_datetime) = {{ current_year }}
      and extract(month from pickup_datetime) = {{ current_month }}
    {% endif %}
),

hourly_stats as (
    select
        service_type,
        pickup_year,
        pickup_month,
        pickup_hour,
        -- Volume metrics
        count(1) as total_trips,
        sum(passenger_count) as total_passengers,
        
        -- Financial metrics
        round(avg(total_amount), 2) as avg_fare,
        round(sum(total_amount), 2) as total_revenue,
        round(avg(tip_amount), 2) as avg_tip,
        
        -- Distance/Efficiency metrics
        round(avg(trip_distance), 2) as avg_distance_miles

    from trips_data
    group by 1, 2, 3, 4
)

select
    *,
    -- Time period labels
    case 
        when pickup_hour between 6 and 9 then 'Morning Rush'
        when pickup_hour between 16 and 19 then 'Evening Rush'
        when pickup_hour >= 22 or pickup_hour <= 3 then 'Late Night'
        else 'Off-Peak'
    end as time_period,
    
    -- Calculate % of volume for THIS specific month/year combination
    round(
        100.0 * total_trips / sum(total_trips) over(partition by service_type, pickup_year, pickup_month), 
        2
    ) as pct_of_monthly_service_volume

from hourly_stats
order by service_type, pickup_hour