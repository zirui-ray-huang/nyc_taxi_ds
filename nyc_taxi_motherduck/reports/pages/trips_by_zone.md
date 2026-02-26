---
title: Origin Density Map
---

Visualizing the top 20 trip origin zones.

```sql yellow_trips_by_zone
select 
    location_id::bigint::string as LocationID,
    zone,
    sum(total_trips) as total_trips
from nyc_taxi.geo_data
WHERE service_type = 'Yellow'
group by 1, 2
ORDER BY 3 DESC
LIMIT 20
```

```sql green_trips_by_zone
select 
    location_id::bigint::string as LocationID,
    zone,
    sum(total_trips) as total_trips
from nyc_taxi.geo_data
WHERE service_type = 'Green'
group by 1, 2
ORDER BY 3 DESC
LIMIT 20
```


<Tabs>
  <Tab label="Yellow Taxi">
    <AreaMap 
        data={yellow_trips_by_zone}
        geoJsonUrl="../geo-json/taxi_zones.geojson"
        areaCol=LocationID
        value=total_trips
        geoId=LocationID
        title="Yellow Trip Density by Zone"
        colorPalette={['#4A8EBA', '#C65D47']}
        tooltipType=click
        tooltip={[
            {id: 'Zone', showColumnName: false, valueClass: 'text-xl font-semibold'},
            {id: 'LocationID', title: "Location ID", fmt: 'id', fieldClass: 'text-[grey]', valueClass: 'text-[white]'},
            {id: 'total_trips', fmt: 'num', fieldClass: 'text-[grey]', valueClass: 'text-[green]'},
        ]}
    />
  </Tab>
  <Tab label="Green Taxi">
    <AreaMap 
        data={green_trips_by_zone}
        geoJsonUrl="../geo-json/taxi_zones.geojson"
        areaCol=LocationID
        value=total_trips
        geoId=LocationID
        title="Green Trip Density by Zone"
        colorPalette={['#4A8EBA', '#C65D47']}
        tooltipType=click
        tooltip={[
            {id: 'Zone', showColumnName: false, valueClass: 'text-xl font-semibold'},
            {id: 'LocationID', title: "Location ID", fmt: 'id', fieldClass: 'text-[grey]', valueClass: 'text-[white]'},
            {id: 'total_trips', fmt: 'num', fieldClass: 'text-[grey]', valueClass: 'text-[green]'},
        ]}
    />
  </Tab>
</Tabs>