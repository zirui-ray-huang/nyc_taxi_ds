---
title: Temporal Trends
---

```sql daily_data
select *
from nyc_taxi.daily_data
```

```sql hourly_data
select 
    *
from nyc_taxi.hourly_data
where month_id = 'All Time'
```


# Demand Analysis

## Daily Heatmap
<CalendarHeatmap 
    data={daily_data}
    date=pickup_day
    value=total_trips
    title="Daily Trip Intensity"
/>



## Hourly Profile (Typical Day)
<BarChart
    data={hourly_data.filter(d => d.month_id === 'All Time')}
    x=pickup_hour
    y=total_trips
    series=service_type
    colorPalette={['#f9a825', '#2e7d32']}
/>

<Grid cols=2>
    <LineChart data={hourly_data.filter(d => d.month_id === 'All Time')} x=pickup_hour y=avg_fare title="Avg Fare" series=service_type colorPalette={['#f9a825', '#2e7d32']}/>
    <LineChart data={hourly_data.filter(d => d.month_id === 'All Time')} x=pickup_hour y=avg_tip title="Avg Tip" series=service_type colorPalette={['#f9a825', '#2e7d32']}/>
</Grid>

---

