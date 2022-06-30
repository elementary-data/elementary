{% macro get_metrics(tests, results_sample_limit) %}
    {% set tests_metrics = {} %}
    {%- for test in tests -%}
        {% set test_unique_id = elementary.insensitive_get_dict_value(test_result_dict, 'test_unique_id') %}
        {% set test_results_query = elementary.insensitive_get_dict_value(test_result_dict, 'test_results_query') %}
        {% set test_type = elementary.insensitive_get_dict_value(test_result_dict, 'test_type') %}
        {% set status = elementary.insensitive_get_dict_value(test_result_dict, 'status') | lower %}

        {% set test_rows_sample = none %}
        {%- if status != 'error'-%}
            {% set test_rows_sample = elementary_internal.get_test_rows_sample(test_results_query, test_type, results_sample_limit) %}
        {%- endif -%}
        {% do tests_metrics.update({test_unique_id: test_rows_sample}) %}
    {%- endfor -%}
    {% do elementary.edr_log(tojson(tests_metrics)) %}
{% endmacro %}
