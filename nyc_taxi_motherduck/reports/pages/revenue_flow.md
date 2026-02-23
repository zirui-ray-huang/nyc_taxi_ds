---
title: Revenue Flow Analysis
---

```sql raw_monthly_data
  select * 
  from nyc_taxi.revenue_sources_data
```

# Revenue Origins

This diagram tracks how revenue flows from different sources.

<SankeyDiagram 
    data={raw_monthly_data} 
    sourceCol=source
    targetCol=target
    valueCol=revenue
    title="Revenue Distribution"
/>