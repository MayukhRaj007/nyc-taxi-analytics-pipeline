{#-
    Intermediate: business logic that several marts will need, in one place.
    Adds zone names and derived measures (duration, speed, tip %).
    A view: it is only read by fct_trips, so materialising it would just
    duplicate ~11M rows on disk.
-#}
with trips as (

    select * from {{ ref('stg_yellow_trips') }}

),

zones as (

    select * from {{ ref('stg_taxi_zones') }}

),

enriched as (

    select
        trips.*,
        pickup_zone.borough                                         as pickup_borough,
        pickup_zone.zone                                            as pickup_zone,
        dropoff_zone.borough                                        as dropoff_borough,
        dropoff_zone.zone                                           as dropoff_zone,
        date_diff('second', trips.pickup_ts, trips.dropoff_ts) / 60.0 as duration_minutes
    from trips
    left join zones as pickup_zone
        on trips.pickup_location_id = pickup_zone.location_id
    left join zones as dropoff_zone
        on trips.dropoff_location_id = dropoff_zone.location_id

),

derived as (

    select
        *,
        -- Speed is meaningless for trips under a minute or with no distance, and
        -- values above 100 mph are meter errors; leave those NULL rather than lie.
        case
            when duration_minutes >= 1
                 and trip_distance_miles > 0
                 and {{ safe_divide('trip_distance_miles', 'duration_minutes / 60.0') }} <= 100
            then {{ safe_divide('trip_distance_miles', 'duration_minutes / 60.0') }}
        end                                                         as avg_speed_mph,
        -- Tips are only recorded for card payments (type 1); cash trips would
        -- read as 0% and drag the average down, so they are excluded (NULL).
        case
            when payment_type_id = 1 and fare_amount > 0
            then {{ safe_divide('tip_amount * 100.0', 'fare_amount') }}
        end                                                         as tip_pct
    from enriched

)

select * from derived
