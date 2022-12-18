{% macro get_test_unique_ids_from_invocation(invocation_id) %}
    {% set query %}
        with dbt_run_results as (
            select * from {{ ref('elementary', 'dbt_run_results') }}
        )

        select distinct unique_id
        from dbt_run_results
        where invocation_id = {{"'" ~ invocation_id ~ "'" }}
    {% endset %}
    {% set result = elementary.agate_to_json(dbt.run_query(query)) %}
    {% do elementary.edr_log(result) %}
{% endmacro %}