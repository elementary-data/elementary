{% macro populate_model_alerts_query() %}
    with model_alerts as (
        {{ elementary_cli.model_alerts_query() }}
    ),

    snapshot_alerts as (
        {{ elementary_cli.snapshot_alerts_query() }}
    ),

    all_alerts as ( 
        select * 
        from model_alerts
        union all
        select *
        from snapshot_alerts
    )

    select 
        model_execution_id as alert_id,
        unique_id,
        detected_at,
        database_name,
        materialization,
        path,
        original_path,
        schema_name,
        message,
        owner as owners,
        tags,
        alias,
        status,
        full_refresh
    from all_alerts
{% endmacro %}


{% macro model_alerts_query() %}
    with model_run_results as (
        select * from {{ ref('model_run_results') }}
    )

    select 
        model_execution_id,
        unique_id,
        invocation_id,
        name,
        generated_at,
        {{ elementary.edr_cast_as_timestamp("generated_at") }} as detected_at,
        status,
        full_refresh,
        message,
        execution_time,
        execute_started_at,
        execute_completed_at,
        compile_started_at,
        compile_completed_at,
        compiled_code,
        database_name,
        schema_name,
        materialization,
        tags,
        package_name,
        path,
        original_path,
        owner,
        alias 
    from model_run_results
    where lower(status) != 'success' 
{% endmacro %}


{% macro snapshot_alerts_query() %}
    with snapshot_run_results as (
        select * from {{ ref('snapshot_run_results') }}
    )

    select 
        model_execution_id,
        unique_id,
        invocation_id,
        name,
        generated_at,
        {{ elementary.edr_cast_as_timestamp("generated_at") }} as detected_at,
        status,
        full_refresh,
        message,
        execution_time,
        execute_started_at,
        execute_completed_at,
        compile_started_at,
        compile_completed_at,
        compiled_code,
        database_name,
        schema_name,
        materialization,
        tags,
        package_name,
        path,
        original_path,
        owner,
        alias 
    from snapshot_run_results
    where lower(status) != 'success' 
{% endmacro %}