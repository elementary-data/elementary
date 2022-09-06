{% macro get_filtered_columns_in_relation(from, except=[]) -%}
    {{ return(adapter.dispatch('get_filtered_columns_in_relation', 'dbt_utils')(from, except)) }}
{% endmacro %}

{% macro default__get_filtered_columns_in_relation(from, except=[]) -%}
    {%- do dbt_utils._is_relation(from, 'get_filtered_columns_in_relation') -%}
    {%- do dbt_utils._is_ephemeral(from, 'get_filtered_columns_in_relation') -%}

    {# -- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

    {%- set include_cols = [] %}
    {%- set cols = adapter.get_columns_in_relation(from) -%}
    {%- set except = except | map("lower") | list %}
    {%- for col in cols -%}
        {%- if col.column|lower not in except -%}
            {% do include_cols.append(col.column) %}
        {%- endif %}
    {%- endfor %}

    {{ return(include_cols) }}

{%- endmacro %}