{%- macro get_test_rows_sample(result_rows_agate, test_type, results_sample_limit=5) -%}
    {% set test_rows_sample = [] %}
    {% if result_rows_agate %}
        {% set result_row_column = result_rows_agate.columns["result_row"] %}
        {% if test_type == 'dbt_test' %}
          {% set result_row_column = result_row_column[:results_sample_limit] %}
        {% endif %}
        {% for result_row in result_row_column %}
          {% do test_rows_sample.append(fromjson(result_row)) %}
        {% endfor %}
    {% endif %}
    {{ return(test_rows_sample) }}
{%- endmacro -%}
