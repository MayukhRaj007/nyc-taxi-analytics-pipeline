{#-
    Use the custom schema exactly as written (staging, intermediate, marts, raw)
    instead of dbt's default "<target_schema>_<custom_schema>" (e.g. main_marts).
    Keeps DuckDB schemas readable: `select * from marts.fct_trips`.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
