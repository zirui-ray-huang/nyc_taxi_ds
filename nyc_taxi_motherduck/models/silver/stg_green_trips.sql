{% set current_year = var('year') | int %}
{% set current_month = var('month') | int %}

{{ config(
    materialized = 'incremental',
    unique_key = 'trip_id',
    description = 'Silver layer: Cleaned Green Taxi trips with imputed missing values and standardized fields.',
    on_schema_change = 'append_new_columns'
) }}


with source as (
    select * from {{ source('s3_raw', 'green_trips') }}
    where VendorID is not null
    and (
        (extract(year from lpep_pickup_datetime) = {{ current_year }} 
         and extract(month from lpep_pickup_datetime) = {{ current_month }})
        or 
        (extract(year from lpep_dropoff_datetime) = {{ current_year }} 
         and extract(month from lpep_dropoff_datetime) = {{ current_month }})
    )

    qualify row_number() over (
        partition by VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, PULocationID, DOLocationID
        order by lpep_pickup_datetime
    ) = 1
),

renamed as (
    select
        -- Trip ID Generation using surrogate key
        {{ dbt_utils.generate_surrogate_key(['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'total_amount']) }} as trip_id,
        
        -- Vendor Mapping
        {{ get_vendor('VendorID') }} as vendor,

        -- Timestamps
        lpep_pickup_datetime as pickup_datetime,
        lpep_dropoff_datetime as dropoff_datetime,

        -- Trip Info
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,
        PULocationID AS pickup_location_id,
        DOLocationID AS dropoff_location_id,
        
        -- Rate Code, Trip Type, and Payment Type Mapping
        {{ get_rate_code('RatecodeID') }} as rate_code,
        {{ get_trip_type('trip_type') }} as trip_type,
        {{ get_payment_type('payment_type') }} as payment_type,

        -- Financials
        {{ clean_currency('fare_amount') }} as fare_amount,
        {{ clean_currency('extra') }} as extra,
        {{ clean_currency('mta_tax') }} as mta_tax,
        {{ clean_currency('tip_amount') }} as tip_amount,
        {{ clean_currency('tolls_amount') }} as tolls_amount,
        {{ clean_currency('improvement_surcharge') }} as improvement_surcharge,
        {{ clean_currency('total_amount') }} as total_amount,
        {{ clean_currency('congestion_surcharge') }} as congestion_surcharge,
        {{ clean_currency('cbd_congestion_fee') }} as cbd_congestion_fee

    from source
),

-- Calculate medians as a summary table
medians_route as (
    select 
        pickup_location_id,
        dropoff_location_id,
        median(trip_distance) as route_median_distance
    from renamed
    where trip_distance > 0
    group by 1, 2
),

median_global_passenger_count as (
    select 
        median(passenger_count) as global_median_passengers
    from renamed
    where passenger_count > 0
),

median_global_distance as (
    select 
        median(trip_distance) as global_median_distance
    from renamed
    where trip_distance > 0
),

final as (
    select
        r.*,
        -- Imputation Logic
        coalesce(
            nullif(r.passenger_count, 0), 
            g1.global_median_passengers
        ) as imputed_passenger_count,

        coalesce(
            nullif(r.trip_distance, 0), 
            m.route_median_distance, 
            g2.global_median_distance, 
            0
        ) as imputed_trip_distance

    from renamed r
    left join medians_route m 
        on r.pickup_location_id = m.pickup_location_id 
        and r.dropoff_location_id = m.dropoff_location_id
    cross join median_global_passenger_count g1
    cross join median_global_distance g2
)

select * from final