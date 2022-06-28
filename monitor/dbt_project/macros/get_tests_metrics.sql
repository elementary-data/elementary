{% macro get_tests_metrics(tests_dicts) %}
    {% set test_metrics_dicts = [] %}
    {%- for test_dict in tests_dicts -%}
        {% set test_unique_id = elementary.insensitive_get_dict_value(test_dict, 'test_unique_id') %}
        {% set test_results_query = elementary.insensitive_get_dict_value(test_dict, 'test_results_query') %}
        {% set test_type = elementary.insensitive_get_dict_value(test_dict, 'test_type') %}
        {% set status = elementary.insensitive_get_dict_value(test_dict, 'status') | lower %}

        {% set test_metrics = none %}
        {%- if status != 'error'-%}
            {% set test_metrics = elementary_internal.get_test_rows_sample(test_results_query, test_type) %}
            {% do elementary.edr_log(tojson(test_metrics)) %}
        {%- endif -%}
        {% do test_dict.update({'test_rows_sample': test_metrics}) %}
        {% do test_metrics_dicts.append(test_result_dict) %}
    {%- endfor -%}
    {% do elementary.edr_log(tojson(test_metrics_dicts)) %}
{% endmacro %}
