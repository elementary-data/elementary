{%- macro get_dbt_invocations(
    command=none,
    project_name=none,
    orchestrator=none,
    job_id=none,
    job_run_id=none,
    invocation_ids=none,
    target_name=none,
    target_schema=none,
    target_profile_name=none,
    full_refresh=none,
    started_after=none,
    started_before=none,
    limit=200
) -%}
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
        where 1=1
        {{ elementary_cli._eq_ci_filter('command', command) }}
        {{ elementary_cli._eq_ci_filter('project_name', project_name) }}
        {{ elementary_cli._eq_ci_filter('orchestrator', orchestrator) }}
        {{ elementary_cli._eq_filter('job_id', job_id) }}
        {{ elementary_cli._eq_filter('job_run_id', job_run_id) }}
        {{ elementary_cli._in_filter('invocation_id', invocation_ids) }}
        {{ elementary_cli._eq_ci_filter('target_name', target_name) }}
        {{ elementary_cli._eq_ci_filter('target_schema', target_schema) }}
        {{ elementary_cli._eq_ci_filter('target_profile_name', target_profile_name) }}
        {{ elementary_cli._bool_filter('full_refresh', full_refresh) }}
        {{ elementary_cli._gte_filter('run_started_at', started_after) }}
        {{ elementary_cli._lte_filter('run_started_at', started_before) }}
        order by run_started_at desc
        limit {{ limit }}
    {% endset %}

    {% set results = elementary.run_query(query) %}
    {% do return(elementary.agate_to_dicts(results)) %}
{%- endmacro -%}
