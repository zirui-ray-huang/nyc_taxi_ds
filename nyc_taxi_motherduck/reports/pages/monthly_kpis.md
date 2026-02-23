---
title: Monthly Performance
---

```sql latest_kpis
select *
from nyc_taxi.latest_kpis
```


# KPI Deep Dive

<Tabs>
  <Tab label="Yellow Taxi">
    <Grid cols=2> 
      <BigValue data={latest_kpis.filter(d => d.service_type === 'Yellow')} value=total_revenue sparkline=full_date comparison=revenue_pct_change comparisonFmt=pct1 title="Revenue Growth"/>
      <BigValue data={latest_kpis.filter(d => d.service_type === 'Yellow')} value=total_trips sparkline=full_date comparison=trips_pct_change comparisonFmt=pct1 title="Trip Growth"/>
    </Grid>
  </Tab>
  <Tab label="Green Taxi">
    <Grid cols=2> 
      <BigValue data={latest_kpis.filter(d => d.service_type === 'Green')} value=total_revenue sparkline=full_date comparison=revenue_pct_change comparisonFmt=pct1 title="Revenue Growth"/>
      <BigValue data={latest_kpis.filter(d => d.service_type === 'Green')} value=total_trips sparkline=full_date comparison=trips_pct_change comparisonFmt=pct1 title="Trip Growth"/>
    </Grid>
  </Tab>
</Tabs>
