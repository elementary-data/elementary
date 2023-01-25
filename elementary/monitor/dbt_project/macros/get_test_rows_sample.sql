{%- macro get_test_rows_sample(test, test_result_rows_agate, test_type, results_sample_limit = 5) -%}
    {% set test_rows_sample = [] %}
    {% set test_execution_id = elementary.insensitive_get_dict_value(test, 'test_execution_id') %}
    {% set result_rows_agate = test_result_rows_agate.get(test_execution_id) %}
    {% if result_rows_agate %}
        {% set result_row_column = result_rows_agate.columns[0] %}
        {% if test_type == 'dbt_test' %}
          {% set result_row_column = result_row_column[:results_sample_limit] %}
        {% endif %}
        {% for result_row in result_row_column %}
          {% do test_rows_sample.append(fromjson(result_row)) %}
        {% endfor %}
    {% endif %}
    {{ return(test_rows_sample) }}
{%- endmacro -%}
