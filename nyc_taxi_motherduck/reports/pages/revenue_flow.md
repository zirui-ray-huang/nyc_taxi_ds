---
title: Revenue Flow Analysis
---

```sql years
select revenue_year
from (
    select 'All' as revenue_year, 0 as sort_order
    union all
    select distinct cast(cast(revenue_year as int) as varchar), 1 as sort_order
    from nyc_taxi.revenue_sources_data
    where revenue_year is not null
)
order by sort_order asc, revenue_year desc
```

```sql months
select revenue_month
from (
    select 'All' as revenue_month, 0 as sort_order
    union all
    select distinct cast(cast(revenue_month as int) as varchar), 1 as sort_order
    from nyc_taxi.revenue_sources_data
    where revenue_month is not null
)
order by sort_order asc, revenue_month desc
```

<Grid cols=2>
  <Dropdown
    name=selected_year
    data={years}
    value=revenue_year
    title="Year"
  />
  <Dropdown
    name=selected_month
    data={months}
    value=revenue_month
    title="Month"
  />
</Grid>


```sql filtered_revenue
select * from nyc_taxi.revenue_sources_data
where 
  -- Filter by Year: If 'All' is selected, ignore. Otherwise, match the year.
  (
    '${inputs.selected_year.value}' = 'All' 
    or cast(cast(revenue_year as int) as varchar) = '${inputs.selected_year.value}'
  )
  and 
  -- Filter by Month: If 'All' is selected, ignore. Otherwise, match the month.
  (
    '${inputs.selected_month.value}' = 'All' 
    or cast(cast(revenue_month as int) as varchar) = '${inputs.selected_month.value}'
  )
```

# Revenue Origins

This diagram tracks how revenue flows from different sources.

<SankeyDiagram 
    data={filtered_revenue} 
    sourceCol=source
    targetCol=target
    valueCol=revenue
    title="Revenue Distribution"
/>