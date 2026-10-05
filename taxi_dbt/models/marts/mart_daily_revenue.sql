select
    pickup_date,
    count(*)                                                 as trip_count,
    sum(total_amount)                                        as total_revenue,
    sum(fare_amount)                                         as fare_revenue,
    sum(tip_amount)                                          as tip_revenue,
    avg(fare_amount)::double                                 as avg_fare,
    avg(trip_distance_miles)                                 as avg_trip_distance_miles,
    -- Card trips only: cash tips are not recorded, so including cash would bias this down.
    avg(tip_pct)                                             as avg_tip_pct,
    {{ weighted_tip_pct() }}                                 as weighted_tip_pct
from {{ ref('fct_trips') }}
group by pickup_date
