{% set current_year = var('year') | int %}
{% set current_month = var('month') | int %}

{{ config(
    materialized = 'incremental',
    unique_key = ['revenue_year', 'revenue_month', 'service_type'],
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns',
    description = 'Gold layer: Monthly revenue metrics by service type for BI reporting.'
) }}

with trips_data as (
    -- Combine the two silver models
    select 
        'Yellow' as service_type,
        pickup_datetime,
        fare_amount,
        tip_amount,
        total_amount
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
        pickup_datetime,
        fare_amount,
        tip_amount,
        total_amount
    from {{ ref('stg_green_trips') }}
    where 1=1
    {% if is_incremental() %}
      -- Only look at the month we are currently processing
      and extract(year from pickup_datetime) = {{ current_year }}
      and extract(month from pickup_datetime) = {{ current_month }}
    {% endif %}
),

monthly_aggregates as (
    select 
        service_type,
        {{ current_year }} as revenue_year,
        {{ current_month }} as revenue_month,
        
        count(*) as total_trips,
        round(sum(fare_amount), 2) as total_fare_amount,
        round(sum(tip_amount), 2) as total_tip_amount,
        round(sum(total_amount), 2) as total_revenue,
        
        -- Calculation for tip percentage
        round(
            100.0 * sum(tip_amount) / nullif(sum(fare_amount), 0), 
            2
        ) as avg_tip_percentage

    from trips_data
    where fare_amount > 0
    group by 1, 2, 3
)

select
    *
from monthly_aggregates
