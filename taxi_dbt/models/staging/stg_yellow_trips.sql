{#-
    Staging: rename, cast, and drop rows that cannot be real trips.
    One-to-one with the source except for the filters below; no joins here.

    Rows are dropped (not fixed) because a refund, a negative meter, or a
    dropoff before pickup carries no trustworthy value to repair. The volume
    dropped is visible in the Airflow data_quality_check task (retention).
-#}
with source as (

    select * from {{ source('raw', 'yellow_trips') }}

),

renamed as (

    select
        -- Natural key: the TLC feed has no trip id, so derive a deterministic one.
        md5(concat_ws(
            '|',
            vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
            pulocationid, dolocationid, trip_distance, fare_amount, total_amount
        )) as trip_id,
        cast(vendorid as integer) as vendor_id,
        cast(tpep_pickup_datetime as timestamp) as pickup_ts,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_ts,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as double) as trip_distance_miles,
        cast(ratecodeid as integer) as rate_code_id,
        store_and_fwd_flag,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,
        cast(payment_type as integer) as payment_type_id,
        cast(fare_amount as decimal(10, 2)) as fare_amount,
        cast(coalesce(extra, 0) as decimal(10, 2)) as extra_amount,
        cast(coalesce(mta_tax, 0) as decimal(10, 2)) as mta_tax_amount,
        cast(tip_amount as decimal(10, 2)) as tip_amount,
        cast(coalesce(tolls_amount, 0) as decimal(10, 2)) as tolls_amount,
        cast(coalesce(improvement_surcharge, 0) as decimal(10, 2)) as improvement_surcharge_amount,
        cast(coalesce(congestion_surcharge, 0) as decimal(10, 2)) as congestion_surcharge_amount,
        cast(coalesce(airport_fee, 0) as decimal(10, 2)) as airport_fee_amount,
        cast(coalesce(cbd_congestion_fee, 0) as decimal(10, 2)) as cbd_congestion_fee_amount,
        cast(total_amount as decimal(10, 2)) as total_amount,
        source_month,
        loaded_at
    from source

),

valid as (

    select * from renamed
    where
        pickup_ts is not null
        and dropoff_ts is not null
        and pickup_location_id is not null
        and dropoff_location_id is not null
        and fare_amount is not null
        and total_amount is not null
        and tip_amount is not null
        -- Negative money = refunds/disputes; we model completed paid trips only.
        and fare_amount >= 0
        and total_amount >= 0
        and tip_amount >= 0
        and trip_distance_miles >= 0
        -- Physically impossible or absurd values (meter glitches).
        and fare_amount <= {{ var('max_fare_usd') }}
        and total_amount <= {{ var('max_fare_usd') }}
        and trip_distance_miles <= {{ var('max_trip_miles') }}
        -- Time must run forwards, and a single trip cannot span a day.
        and dropoff_ts >= pickup_ts
        and dropoff_ts < pickup_ts + interval 24 hour
        -- The feed contains stray rows dated outside the month of their file
        -- (e.g. 2008); keeping them would break month-based reloads.
        and cast(date_trunc('month', pickup_ts) as date) = source_month

)

select * from valid
