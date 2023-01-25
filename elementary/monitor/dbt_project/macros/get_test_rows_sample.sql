{%- macro get_test_rows_sample(test, elementary_test_result_id_key, test_result_rows_agate, test_type, results_sample_limit=5) -%}
    {% set test_rows_sample = [] %}
    {% set elementary_test_results_id = elementary.insensitive_get_dict_value(test, elementary_test_result_id_key) %}
    {% set result_rows_agate = test_result_rows_agate.get(elementary_test_results_id) %}
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
