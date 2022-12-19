{% macro get_last_tests_invocation_id(invocation_time = none) %}
    {% set last_invocation_query %}
        with elementary_test_results as (
            select * from {{ ref('elementary', 'elementary_test_results') }}
        )

        select distinct invocation_id, detected_at
        from elementary_test_results
        {% if invocation_time %}
            where detected_at < {{"'" ~ invocation_time ~ "'" }}
        {% endif %}
        order by detected_at desc
        limit 1
    {% endset %}
    {% set result = elementary.agate_to_json(dbt.run_query(last_invocation_query)) %}
    {% do elementary.edr_log(result) %}
{% endmacro %}
