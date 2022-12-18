{% macro get_last_invocation_id(type, invocation_time = none) %}
    {% set last_invocation_query %}
        with dbt_run_results as (
            select * from {{ ref('elementary', 'dbt_run_results') }}
        )

        select distinct invocation_id, generated_at, resource_type
        from dbt_run_results
        where resource_type = {{"'" ~ type ~ "'" }}
        {% if invocation_time %}
            and generated_at < {{"'" ~ invocation_time ~ "'" }}
        {% endif %}
        order by generated_at desc
        limit 1
    {% endset %}
    {% set result = elementary.agate_to_json(dbt.run_query(last_invocation_query)) %}
    {% do elementary.edr_log(result) %}
{% endmacro %}
