select
    *,
    MAKE_DATE(revenue_year::bigint, revenue_month::bigint, 1) AS full_date,
    total_revenue - lag(total_revenue) over(partition by service_type order by revenue_year, revenue_month) as revenue_delta,
    (total_revenue - lag(total_revenue) over(partition by service_type order by revenue_year, revenue_month)) 
        / nullif(lag(total_revenue) over(partition by service_type order by revenue_year, revenue_month), 0) as revenue_pct_change,
    (total_trips - lag(total_trips) over(partition by service_type order by revenue_year, revenue_month)) 
        / nullif(lag(total_trips) over(partition by service_type order by revenue_year, revenue_month), 0) as trips_pct_change,
from main_marts.fct_monthly_revenue
order by revenue_year desc, revenue_month desc