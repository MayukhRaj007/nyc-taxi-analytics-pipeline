-- Fails if time runs backwards on any trip.
select trip_id, pickup_ts, dropoff_ts
from {{ ref('fct_trips') }}
where dropoff_ts < pickup_ts
