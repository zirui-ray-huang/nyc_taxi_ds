---
title: 🚕 NYC Taxi Executive Dashboard
neverShowQueries: true
---




```sql latest_kpis
select *
from nyc_taxi.latest_kpis
```




<Alert status="info">
This report reflects operations up to 
    <strong>
        <Value data={latest_kpis} column=full_date fmt=mmm_yyyy />
    </strong>.
</Alert>



Welcome to the NYC Taxi Analysis portal. Select a report below to dive deeper into the data.

<Grid cols=3>
    <div style="padding: 15px; border: 1px solid #eee; border-radius: 8px;">
        <a href="/monthly_kpis" style="font-size: 1.2rem; font-weight: bold; text-decoration: none;">📊 Monthly KPIs</a>
        <p style="margin-top: 10px; color: #666;">Latest revenue, trip counts, and month-over-month growth patterns.</p>
    </div>

    <div style="padding: 15px; border: 1px solid #eee; border-radius: 8px;">
        <a href="/temporal_trends" style="font-size: 1.2rem; font-weight: bold; text-decoration: none;">🕒 Temporal Trends</a>
        <p style="margin-top: 10px; color: #666;">Hourly demand profiles and daily calendar intensity heatmaps.</p>
    </div>

    <div style="padding: 15px; border: 1px solid #eee; border-radius: 8px;">
        <a href="/revenue_flow" style="font-size: 1.2rem; font-weight: bold; text-decoration: none;">💸 Revenue Flow</a>
        <p style="margin-top: 10px; color: #666;">Sankey diagrams tracking payment types and revenue sources.</p>
    </div>
</Grid>

<br/>

### Market Overview

<Grid cols=2>
  <BigValue 
    data={latest_kpis.filter(d => d.service_type === 'Yellow')} 
    value=total_revenue
    title="Yellow Revenue (Latest)"
    row=last
  />
  <BigValue 
    data={latest_kpis.filter(d => d.service_type === 'Green')} 
    value=total_revenue
    title="Green Revenue (Latest)"
    row=last
  />
</Grid>
