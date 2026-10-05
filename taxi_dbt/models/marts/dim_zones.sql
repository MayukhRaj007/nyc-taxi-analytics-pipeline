select
    location_id,
    borough,
    zone,
    service_zone,
    -- Handy flags so BI users do not have to remember zone names or ids.
    service_zone in ('Airports', 'EWR') as is_airport,
    borough = 'Manhattan' as is_manhattan
from {{ ref('stg_taxi_zones') }}
