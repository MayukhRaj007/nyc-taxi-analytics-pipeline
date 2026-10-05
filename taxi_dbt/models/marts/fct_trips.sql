{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

{#-
    Incremental fact table, one row per trip.

    Why incremental: each Airflow run adds one month (~3.5M rows). Rebuilding
    the whole table every run would reprocess every earlier month for nothing.

    Which rows to (re)process, in order of preference:
      1. Airflow passes start_date / end_date for the month being loaded, so a
         rerun or backfill of ANY month rebuilds exactly that month.
      2. Otherwise (manual `dbt run`), reprocess the last N days of existing
         data plus anything newer, to catch late-arriving rows.
    delete+insert on unique_key=trip_id makes either path idempotent.
-#}
with trips as (

    select * from {{ ref('int_trips_enriched') }}

    {% if is_incremental() %}
        {% if var('start_date', none) is not none %}
    where pickup_ts >= cast('{{ var("start_date") }}' as date)
      and pickup_ts <  cast('{{ var("end_date") }}' as date)
        {% else %}
    where pickup_ts >= (select max(pickup_date) from {{ this }}) - interval '{{ var("incremental_lookback_days") }} days'
        {% endif %}
    {% endif %}

),

-- Defensive de-duplication: the TLC feed has no primary key, so identical
-- records are collapsed. Done here (after the date filter) rather than in
-- staging so the window function only sees the rows being processed.
deduplicated as (

    select *
    from trips
    qualify row_number() over (partition by trip_id order by loaded_at desc) = 1

)

select
    trip_id,
    vendor_id,
    pickup_ts,
    dropoff_ts,
    cast(pickup_ts as date)                                  as pickup_date,
    extract(hour from pickup_ts)::integer                    as pickup_hour,
    isodow(pickup_ts)::integer                               as pickup_day_of_week,
    isodow(pickup_ts) in (6, 7)                              as is_weekend,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance_miles,
    duration_minutes,
    avg_speed_mph,
    payment_type_id,
    {{ payment_type_name('payment_type_id') }}               as payment_type_name,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    tip_pct,
    source_month
from deduplicated
