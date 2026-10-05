-- Every trip belongs to exactly one pickup zone, so zone totals must add up
-- to the fact table. Returns a row only if they differ.
select
    (select count(*) from {{ ref('fct_trips') }}) as fct_trips,
    (select sum(trip_count) from {{ ref('mart_zone_performance') }}) as zone_trips
where (select count(*) from {{ ref('fct_trips') }})
   != (select sum(trip_count) from {{ ref('mart_zone_performance') }})
