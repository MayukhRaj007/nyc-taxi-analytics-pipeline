-- Fails if any trip lasts 24 hours or more (or has a negative duration).
select trip_id, pickup_ts, dropoff_ts, duration_minutes
from {{ ref('fct_trips') }}
where duration_minutes >= 24 * 60
   or duration_minutes < 0
