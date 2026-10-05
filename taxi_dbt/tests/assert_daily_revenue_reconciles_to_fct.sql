-- Reconciliation: each day's revenue and trip count in the mart must equal the
-- fact table. Catches a stale mart or a broken aggregation. Returns offending days.
with fct as (

    select pickup_date, count(*) as trips, sum(total_amount) as revenue
    from {{ ref('fct_trips') }}
    group by pickup_date

)

select
    coalesce(fct.pickup_date, mart.pickup_date) as pickup_date,
    fct.trips as fct_trips,
    mart.trip_count as mart_trips,
    fct.revenue as fct_revenue,
    mart.total_revenue as mart_revenue
from fct
full outer join {{ ref('mart_daily_revenue') }} as mart
    on fct.pickup_date = mart.pickup_date
where fct.pickup_date is null
   or mart.pickup_date is null
   or fct.trips != mart.trip_count
   or abs(fct.revenue - mart.total_revenue) > 0.01
