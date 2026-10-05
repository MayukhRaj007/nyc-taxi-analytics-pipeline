select
    cast("LocationID" as integer) as location_id,
    trim("Borough") as borough,
    trim("Zone") as zone, -- noqa: RF04
    trim(service_zone) as service_zone
from {{ ref('taxi_zone_lookup') }}
