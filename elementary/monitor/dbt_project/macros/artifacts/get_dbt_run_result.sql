{%- macro get_dbt_run_result(model_execution_id, include_compiled_code=false) -%}
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

    {% set projection = base_cols ~ ', ' ~ heavy_cols if include_compiled_code else base_cols %}

    {% set query %}
        select {{ projection }}
        from {{ relation }}
        where model_execution_id = '{{ elementary_cli._sql_escape(model_execution_id) }}'
        limit 1
    {% endset %}

    {% set results = elementary.run_query(query) %}
    {% do return(elementary.agate_to_dicts(results)) %}
{%- endmacro -%}
