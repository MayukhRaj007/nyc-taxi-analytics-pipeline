{#-
    Division that returns NULL instead of raising or producing inf when the
    denominator is zero or NULL. Used for rates, averages and percentages.
-#}
{% macro safe_divide(numerator, denominator) -%}
    ({{ numerator }}) / nullif({{ denominator }}, 0)
{%- endmacro %}
