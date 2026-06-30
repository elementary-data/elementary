{%- macro get_dbt_run_results(
    unique_id=none,
    invocation_id=none,
    status=none,
    resource_type=none,
    materialization=none,
    name=none,
    started_after=none,
    started_before=none,
    execution_time_gt=none,
    execution_time_lt=none,
    lightweight=true,
    limit=200
) -%}
    {% set relation = ref('elementary', 'dbt_run_results') %}
    {% if not elementary.relation_exists(relation) %}
        {% do return([]) %}
    {% endif %}

    {% set heavy_cols = 'compiled_code, adapter_response' %}
    {% set base_cols %}
        model_execution_id,
        unique_id,
        invocation_id,
        name,
        status,
        resource_type,
        materialization,
        execution_time,
        execute_started_at,
        execute_completed_at,
        compile_started_at,
        compile_completed_at,
        full_refresh,
        message,
        thread_id,
        query_id,
        created_at
    {% endset %}

    {% set projection = base_cols if lightweight else base_cols ~ ', ' ~ heavy_cols %}

    {% set query %}
        select {{ projection }}
        from {{ relation }}
        where 1=1
        {{ elementary_cli._eq_filter('unique_id', unique_id) }}
        {{ elementary_cli._eq_filter('invocation_id', invocation_id) }}
        {{ elementary_cli._eq_ci_filter('status', status) }}
        {{ elementary_cli._eq_ci_filter('resource_type', resource_type) }}
        {{ elementary_cli._eq_ci_filter('materialization', materialization) }}
        {{ elementary_cli._like_ci_filter('name', name) }}
        {{ elementary_cli._gte_filter('execute_started_at', started_after) }}
        {{ elementary_cli._lte_filter('execute_started_at', started_before) }}
        {{ elementary_cli._gt_number_filter('execution_time', execution_time_gt) }}
        {{ elementary_cli._lt_number_filter('execution_time', execution_time_lt) }}
        order by execute_started_at desc
        limit {{ limit }}
    {% endset %}

    {% set results = elementary.run_query(query) %}
    {% do return(elementary.agate_to_dicts(results)) %}
{%- endmacro -%}
