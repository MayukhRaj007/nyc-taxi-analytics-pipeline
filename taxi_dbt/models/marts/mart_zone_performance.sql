with by_zone as (

    select
        pickup_location_id                                   as location_id,
        count(*)                                             as trip_count,
        sum(total_amount)                                    as total_revenue,
        avg(fare_amount)::double                             as avg_fare,
        avg(trip_distance_miles)                             as avg_trip_distance_miles,
        avg(duration_minutes)                                as avg_duration_minutes,
        avg(tip_pct)                                         as avg_tip_pct,
        {{ weighted_tip_pct() }}                             as weighted_tip_pct
    from {{ ref('fct_trips') }}
    group by pickup_location_id

)

select
    by_zone.location_id,
    zones.borough,
    zones.zone,
    by_zone.trip_count,
    by_zone.total_revenue,
    by_zone.avg_fare,
    by_zone.avg_trip_distance_miles,
    by_zone.avg_duration_minutes,
    by_zone.avg_tip_pct,
    by_zone.weighted_tip_pct,
    rank() over (order by by_zone.trip_count desc)           as trip_count_rank,
    round(100.0 * by_zone.trip_count / sum(by_zone.trip_count) over (), 3) as pct_of_all_trips
from by_zone
inner join {{ ref('dim_zones') }} as zones
    on by_zone.location_id = zones.location_id
