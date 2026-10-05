-- Fails (returns rows) if any trip has a negative fare, total or tip.
-- Negative amounts are refunds/disputes and are filtered out in staging.
select
    trip_id,
    fare_amount,
    total_amount,
    tip_amount
from {{ ref('fct_trips') }}
where
    fare_amount < 0
    or total_amount < 0
    or tip_amount < 0
