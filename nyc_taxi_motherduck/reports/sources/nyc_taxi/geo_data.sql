select geo.*, lookup.Zone
from main_marts.fct_geo_distribution as geo
LEFT JOIN main.taxi_zone_lookup as lookup
ON geo.location_id = lookup.LocationID