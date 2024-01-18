{% macro populate_model_alerts() %}
    {% set model_alerts = [] %}
    {% set raw_model_alerts_agate = run_query(elementary_cli.populate_model_alerts_query()) %}
    {% set raw_model_alerts = elementary.agate_to_dicts(raw_model_alerts_agate) %}
    {% for raw_model_alert in raw_model_alerts %}
        {% set status = elementary.insensitive_get_dict_value(raw_model_alert, 'status') | lower %}

        {% set model_alert_data = {
            'id': elementary.insensitive_get_dict_value(raw_model_alert, 'alert_id'),
            'model_unique_id': elementary.insensitive_get_dict_value(raw_model_alert, 'unique_id'),
            'alert_class_id': elementary.insensitive_get_dict_value(raw_model_alert, 'alert_class_id'),
            'alias': elementary.insensitive_get_dict_value(raw_model_alert, 'alias'),
            'path': elementary.insensitive_get_dict_value(raw_model_alert, 'path'),
            'original_path': elementary.insensitive_get_dict_value(raw_model_alert, 'original_path'),
            'materialization': elementary.insensitive_get_dict_value(raw_model_alert, 'materialization'),
            'detected_at': elementary.insensitive_get_dict_value(raw_model_alert, 'detected_at'),
            'database_name': elementary.insensitive_get_dict_value(raw_model_alert, 'database_name'),
            'schema_name': elementary.insensitive_get_dict_value(raw_model_alert, 'schema_name'),
            'full_refresh': elementary.insensitive_get_dict_value(raw_model_alert, 'full_refresh'),
            'message': elementary.insensitive_get_dict_value(raw_model_alert, 'message'),
            'owners': elementary.insensitive_get_dict_value(raw_model_alert, 'owners'),
            'tags': elementary.insensitive_get_dict_value(raw_model_alert, 'tags'),
            'model_meta': elementary.insensitive_get_dict_value(raw_model_alert, 'model_meta'),
            'status': status
        } %}

        {% set model_alert = elementary_cli.generate_alert_object(
            elementary.insensitive_get_dict_value(raw_model_alert, 'alert_id'),
            elementary.insensitive_get_dict_value(raw_model_alert, 'alert_class_id'),
            'model',
            elementary.insensitive_get_dict_value(raw_model_alert, 'detected_at'),
            elementary.insensitive_get_dict_value(raw_model_alert, 'created_at'),
            model_alert_data,
        ) %}
        {% do model_alerts.append(model_alert) %}
    {% endfor %}
    {% do return(model_alerts) %}
{% endmacro %}


{% macro populate_model_alerts_query() %}
    with models as (
        select * from {{ ref('elementary', 'dbt_models') }}
    ),

    snapshots as (
        select * from {{ ref('elementary', 'dbt_snapshots') }}
    ),

    artifacts_meta as (
        select unique_id, meta from models
        union all
        select unique_id, meta from snapshots
    ),

    model_run_results as (
        select * from {{ ref('model_run_results') }}
    ),

    snapshot_run_results as (
        select * from {{ ref('snapshot_run_results') }}
    ),

    all_alerts as ( 
        select 
            model_execution_id,
            model_execution_id as alert_id,
            unique_id,
            invocation_id,
            name,
            generated_at,
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
        union all
        select 
            model_execution_id,
            model_execution_id as alert_id,
            unique_id,
            invocation_id,
            name,
            generated_at,
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
    )

    select 
        all_alerts.alert_id,
        all_alerts.unique_id,
        {# Currently alert_class_id equals to unique_id - might change in the future so we return both #}
        all_alerts.unique_id as alert_class_id,
        {{ elementary.edr_cast_as_timestamp("generated_at") }} as detected_at,
        {{ elementary.edr_current_timestamp() }} as created_at,
        all_alerts.database_name,
        all_alerts.materialization,
        all_alerts.path,
        all_alerts.original_path,
        all_alerts.schema_name,
        all_alerts.message,
        all_alerts.owner as owners,
        all_alerts.tags,
        all_alerts.alias,
        all_alerts.status,
        all_alerts.full_refresh,
        artifacts_meta.meta as model_meta
    from all_alerts
    left join artifacts_meta on all_alerts.unique_id = artifacts_meta.unique_id
    where all_alerts.alert_id not in (
        {# "this" is referring to "alerts_v2" - we are executing it using a post_hook over "alerts_v2" #}
        select alert_id from {{ this }}
    )
{% endmacro %}


