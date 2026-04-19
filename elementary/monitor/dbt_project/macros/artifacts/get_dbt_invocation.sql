{%- macro get_dbt_invocation(invocation_id) -%}
    {% set relation = ref('elementary', 'dbt_invocations') %}
    {% if not elementary.relation_exists(relation) %}
        {% do return([]) %}
    {% endif %}

    {% set query %}
        select
            invocation_id,
            project_name,
            run_started_at,
            run_completed_at,
            dbt_version,
            command,
            orchestrator,
            job_id,
            job_name,
            job_url,
            job_run_id,
            job_run_url,
            target_name,
            target_database,
            target_schema,
            target_profile_name,
            threads,
            full_refresh,
            selected,
            invocation_vars
        from {{ relation }}
        where invocation_id = '{{ elementary_cli._sql_escape(invocation_id) }}'
        limit 1
    {% endset %}

    {% set results = elementary.run_query(query) %}
    {% do return(elementary.agate_to_dicts(results)) %}
{%- endmacro -%}
