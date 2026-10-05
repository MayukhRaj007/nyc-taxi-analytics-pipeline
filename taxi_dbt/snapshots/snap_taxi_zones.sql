{#-
    SCD Type 2 history for the zone lookup. Zones change rarely (TLC redraws a
    boundary or renames a zone maybe once in years), so this is a small, honest
    example: if a re-downloaded lookup changes a borough/zone/service_zone, the
    old row is closed (dbt_valid_to) and a new row opened, so history is kept.
    dim_zones serves the current view; query the snapshot for point-in-time.
-#}
{% snapshot snap_taxi_zones %}

{{
    config(
        target_schema='snapshots',
        unique_key='location_id',
        strategy='check',
        check_cols=['borough', 'zone', 'service_zone']
    )
}}

select * from {{ ref('stg_taxi_zones') }}

{% endsnapshot %}
