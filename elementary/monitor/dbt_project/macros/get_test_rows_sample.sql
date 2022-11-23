{%- macro get_test_rows_sample(test, test_results_query, test_type, results_sample_limit = 5) -%}
    {% set result_rows = test.result_rows %}
    {% if result_rows is defined and result_rows is not none %}
      {% do return(fromjson(result_rows)) %}
    {% endif %}

    {% set test_rows_sample = none %}
    {% if test_results_query %}
        {% set test_rows_sample_query = test_results_query %}
        {% if test_type == 'dbt_test' %}
            {% set test_rows_sample_query = test_rows_sample_query ~ ' limit ' ~ results_sample_limit %}
        {% endif %}

        {% set test_rows_sample_agate = run_query(test_rows_sample_query) %}
        {% set test_rows_sample = elementary.agate_to_dicts(test_rows_sample_agate) %}
    {% endif %}
    {{- return(test_rows_sample) -}}
{%- endmacro -%}