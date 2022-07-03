{% macro get_tests_metrics(tests, metrics_sample_limit = 5) %}
    {% set tests_metrics = {} %}
    {%- for test in tests -%}
        {% set test_unique_id = elementary.insensitive_get_dict_value(test, 'test_unique_id') %}
        {% set test_results_query = elementary.insensitive_get_dict_value(test, 'test_results_query') %}
        {% set test_type = elementary.insensitive_get_dict_value(test, 'test_type') %}
        {% set status = elementary.insensitive_get_dict_value(test, 'status') | lower %}

        {% set test_rows_sample = none %}
        {%- if status != 'error'-%}
            {% set test_rows_sample = elementary_internal.get_test_rows_sample(test_results_query, test_type, metrics_sample_limit) %}
        {%- endif -%}
        {% set sub_test_unique_id = get_sub_test_unique_id(
            test_unique_id=elementary.insensitive_get_dict_value(test, 'test_unique_id'),
            test_type=elementary.insensitive_get_dict_value(test, 'test_type'),
            test_sub_type=elementary.insensitive_get_dict_value(test, 'test_sub_type'),
            table_name=elementary.insensitive_get_dict_value(test, 'table_name'),
            column_name=elementary.insensitive_get_dict_value(test, 'column_name'),
        ) %}
        {% do tests_metrics.update({sub_test_unique_id: test_rows_sample}) %}
    {%- endfor -%}
    {% do elementary.edr_log(tojson(tests_metrics)) %}
{% endmacro %}
