{% set exclude_field = 'field_1' %}
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from= ref('data_filtered_columns_in_relation'), except=[exclude_field]) %}

with data as (

    select

        {% for column_name in column_names %}
            max({{ column_name }}) as {{ column_name }} {% if not loop.last %},{% endif %}
        {% endfor %}

    from {{ ref('data_filtered_columns_in_relation') }}

)

select * from data
