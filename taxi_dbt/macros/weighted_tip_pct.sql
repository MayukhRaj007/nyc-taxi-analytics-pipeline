{#-
    Tip percentage as total tips / total fares over card-paid trips (the only
    trips whose tips are recorded). Unlike avg(tip_pct), this is not inflated by
    tiny fares (a $3 fare with a $2 tip is 67% but barely moves total money).
    Use inside an aggregate query over fct_trips.
-#}
{% macro weighted_tip_pct() -%}
    100.0 * {{ safe_divide(
        'sum(tip_amount) filter (where tip_pct is not null)',
        'sum(fare_amount) filter (where tip_pct is not null)'
    ) }}
{%- endmacro %}
