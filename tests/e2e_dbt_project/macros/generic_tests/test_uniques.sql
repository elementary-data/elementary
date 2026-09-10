{# A test that always errors at runtime (used on error_model's missing column). #}
{%- test uniques(model, column_name) -%}
    select {{ column_name }} from {{ model }} group by {{ column_name }} having count(*) > 1
{%- endtest -%}
