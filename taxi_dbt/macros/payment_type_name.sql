{#-
    Map TLC payment_type codes to labels (data dictionary for yellow trips).
    Keeping the mapping in one macro means the staging model, tests and docs
    all agree on the allowed values.
-#}
{% macro payment_type_name(column_name) -%}
    case {{ column_name }}
        when 0 then 'Flex fare'
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'Other'
    end
{%- endmacro %}
