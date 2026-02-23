WITH base_data AS (
    SELECT 
        service_type,
        pickup_year,
        pickup_month,
        pickup_hour,
        total_trips,
        -- We calculate raw revenue/tips to ensure weighted averages are accurate in the aggregate
        (avg_fare * total_trips) as total_revenue,
        (avg_tip * total_trips) as total_tips
    FROM main_marts.fct_hourly_distribution
)

-- 1. The Granular Table (Monthly/Hourly)
SELECT 
    service_type,
    CAST(pickup_year AS VARCHAR) || '-' || LPAD(CAST(pickup_month AS VARCHAR), 2, '0') as month_id,
    pickup_hour,
    total_trips,
    avg_fare,
    avg_tip,
    'Monthly Trend' as grouping_level
FROM main_marts.fct_hourly_distribution

UNION ALL

-- 2. The Aggregate Table (All-Time Average per Hour)
SELECT 
    service_type,
    'All Time' as month_id,
    pickup_hour,
    SUM(total_trips) as total_trips,
    SUM(total_revenue) / NULLIF(SUM(total_trips), 0) as avg_fare,
    SUM(total_tips) / NULLIF(SUM(total_trips), 0) as avg_tip,
    'Total Aggregate' as grouping_level
FROM base_data
GROUP BY 1, 3
ORDER BY service_type, grouping_level, pickup_hour