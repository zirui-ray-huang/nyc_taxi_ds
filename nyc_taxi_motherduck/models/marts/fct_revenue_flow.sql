{% set current_year = var('year') | int %}
{% set current_month = var('month') | int %}

{{ config(
    materialized = 'incremental',
    unique_key = ['source', 'target', 'revenue_year', 'revenue_month'],
    incremental_strategy = 'delete+insert',
    description = 'Gold layer: Revenue flow mapping for Sankey diagrams or breakdown charts.'
) }}

with yellow_silver as (
    select * from {{ ref('stg_yellow_trips') }}
    where 1=1
    {% if is_incremental() %}
      -- Only look at the month we are currently processing
      and extract(year from pickup_datetime) = {{ current_year }}
      and extract(month from pickup_datetime) = {{ current_month }}
    {% endif %}
),

green_silver as (
    select * from {{ ref('stg_green_trips') }}
    where 1=1
    {% if is_incremental() %}
      -- Only look at the month we are currently processing
      and extract(year from pickup_datetime) = {{ current_year }}
      and extract(month from pickup_datetime) = {{ current_month }}
    {% endif %}
),

raw_totals as (
    select 
        sum(total_amount) as revenue,
        'Yellow' as target 
    from yellow_silver
    
    union all
    
    select 
        sum(total_amount) as revenue,
        'Green' as target 
    from green_silver
),

combined_data as (
    select 
        sum(cast(fare_amount as decimal(18,2))) as fare_amount,
        sum(cast(extra as decimal(18,2))) as extra,
        sum(cast(mta_tax as decimal(18,2))) as mta_tax,
        sum(cast(tip_amount as decimal(18,2))) as tip_amount,
        sum(cast(tolls_amount as decimal(18,2))) as tolls_amount,
        sum(cast(improvement_surcharge as decimal(18,2))) as improvement_surcharge,
        sum(cast(congestion_surcharge as decimal(18,2))) as congestion_surcharge,
        sum(cast(cbd_congestion_fee as decimal(18,2))) as cbd_congestion_fee
    from (
        select 
            fare_amount, extra, mta_tax, tip_amount, 
            tolls_amount, improvement_surcharge, 
            congestion_surcharge, cbd_congestion_fee 
        from yellow_silver
        
        union all
        
        select 
            fare_amount, extra, mta_tax, tip_amount, 
            tolls_amount, improvement_surcharge, 
            congestion_surcharge, cbd_congestion_fee 
        from green_silver
    )
),

unpivoted_fees as (
    select
        case 
            when i = 1 then 'Fare Amount'
            when i = 2 then 'Extra'
            when i = 3 then 'MTA Tax'
            when i = 4 then 'Tip Amount'
            when i = 5 then 'Tolls Amount'
            when i = 6 then 'Improvement Surcharge'
            when i = 7 then 'Congestion Surcharge'
            when i = 8 then 'CBD Congestion Fee'
        end as source,
        case 
            when i = 1 then fare_amount
            when i = 2 then extra
            when i = 3 then mta_tax
            when i = 4 then tip_amount
            when i = 5 then tolls_amount
            when i = 6 then improvement_surcharge
            when i = 7 then congestion_surcharge
            when i = 8 then cbd_congestion_fee
        end as revenue,
        'All Revenue' as target
    from combined_data
    -- DuckDB specific unnest for pivoting
    cross join (select unnest(range(1, 9)) as i)
),

final_flow as (
    select 
        'All Revenue' as source, 
        revenue, 
        target,
        {{ current_year }} as revenue_year,
        {{ current_month }} as revenue_month
    from raw_totals

    union all

    select 
        source, 
        revenue, 
        'All Revenue' as target,
        {{ current_year }} as revenue_year,
        {{ current_month }} as revenue_month
    from unpivoted_fees
)

select * from final_flow