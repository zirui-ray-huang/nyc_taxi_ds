select pickup_day, total_trips, service_type
from main_marts.fct_daily_distribution
where extract(year from pickup_day) != 2023
order by pickup_day