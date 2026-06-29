{% macro get_test_last_invocation(invocation_id = none, invocation_max_time = none) %}
    {% set database, schema = elementary.target_database(), target.schema %}
    {% set invocations_relation = adapter.get_relation(database, schema, 'dbt_invocations') %}

    {% set last_invocation_query %}
        with elementary_test_results as (
            select * from {{ ref('elementary', 'elementary_test_results') }}
        ),

        test_invocation as (
            select {% if elementary.is_tsql() %}top 1{% endif %} distinct invocation_id, detected_at
            from elementary_test_results
            {% if invocation_id %}
                where invocation_id = {{ "'" ~ invocation_id ~ "'" }}
            {% elif invocation_max_time %}
                where detected_at < {{ "'" ~ invocation_max_time ~ "'" }}
            {% endif %}
            order by detected_at desc
            {% if not elementary.is_tsql() %}limit 1{% endif %}
        )

        {% if invocations_relation %}
            {% set job_run_id_exists = elementary.column_exists_in_relation(invocations_relation, 'job_run_id') %}
            {% set job_run_url_exists = elementary.column_exists_in_relation(invocations_relation, 'job_run_url') %}
            select
                test_invocation.invocation_id,
                test_invocation.detected_at,
                invocations.command,
                invocations.selected,
                invocations.full_refresh,
                {% if job_run_id_exists %}invocations.job_run_id{% else %}NULL as job_run_id{% endif %},
                {% if job_run_url_exists %}invocations.job_run_url{% else %}NULL as job_run_url{% endif %}
            from test_invocation left join {{ ref('elementary', 'dbt_invocations') }} as invocations
            on test_invocation.invocation_id = invocations.invocation_id
        {% else %}
            select
                invocation_id,
                detected_at,
                NULL as command,
                NULL as selected,
                NULL as full_refresh,
                NULL as job_run_id,
                NULL as job_run_url
            from test_invocation
        {% endif %}
    {% endset %}
    {% do return(elementary.agate_to_dicts(elementary.run_query(last_invocation_query))) %}
{% endmacro %}
