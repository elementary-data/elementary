{% macro get_test_last_invocation(invocation_id = none, invocation_max_time = none) %}
    {% set database, schema = elementary.target_database(), target.schema %}
    {% set invocations_relation = adapter.get_relation(database, schema, 'dbt_invocations') %}

    {% set last_invocation_query %}
        with elementary_test_results as (
            select * from {{ ref('elementary_test_results', package='elementary') }}
        ),

        test_invocation as (
            select distinct invocation_id, detected_at
            from elementary_test_results
            {% if invocation_id %}
                where invocation_id = {{ "'" ~ invocation_id ~ "'" }}
            {% elif invocation_max_time %}
                where detected_at < {{ "'" ~ invocation_max_time ~ "'" }}
            {% endif %}
            order by detected_at desc
            limit 1
        )

        {% if invocations_relation %}
            select 
                test_invocation.invocation_id, 
                test_invocation.detected_at,
                invocations.command,
                invocations.selected,
                invocations.full_refresh
            from test_invocation left join {{ ref('dbt_invocations', package='elementary') }} as invocations
            on test_invocation.invocation_id = invocations.invocation_id
        {% else %}
            select 
                invocation_id,
                detected_at,
                NULL as command,
                NULL as selected,
                NULL as full_refresh
            from test_invocation
        {% endif %}
    {% endset %}
    {% do return(elementary.agate_to_dicts(elementary.run_query(last_invocation_query))) %}
{% endmacro %}
