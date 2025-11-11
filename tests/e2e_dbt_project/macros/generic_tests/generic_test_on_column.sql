{%- test generic_test_on_column(model, column_name) -%}
    {% set query_with_rows %}
        with nothing as (select 1 as num)
        select * from nothing where num = 1
    {%- endset -%}
    {{ query_with_rows }}
{%- endtest -%}