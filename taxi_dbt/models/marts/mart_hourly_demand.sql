select
    pickup_hour,
    count(*) as trip_count,
    count(distinct pickup_date) as days_observed,
    {{ safe_divide('count(*)', 'count(distinct pickup_date)') }} as avg_trips_per_day,
    avg(fare_amount) as avg_fare,
    avg(tip_pct) as avg_tip_pct,
    {{ weighted_tip_pct() }} as weighted_tip_pct,
    avg(avg_speed_mph) as avg_speed_mph
from {{ ref('fct_trips') }}
group by pickup_hour
