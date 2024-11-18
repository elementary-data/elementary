{% macro populate_model_alerts(days_back=1) %}
    {% set model_alerts = [] %}
    {% set raw_model_alerts_agate = run_query(elementary_cli.populate_model_alerts_query(days_back)) %}
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


{% macro populate_model_alerts_query(days_back=1) %}
    {# This macro is invoked as part of alerts_v2 post-hook, so "this" references to alerts_v2 -#}
    {% set seed_run_results_relation = elementary.get_elementary_relation('seed_run_results') -%}

    with models as (
        select * from {{ ref('elementary', 'dbt_models') }}
    ),

    snapshots as (
        select * from {{ ref('elementary', 'dbt_snapshots') }}
    ),

    seeds as (
        select * from {{ ref('elementary', 'dbt_seeds') }}
    ),

    artifacts_meta as (
        select unique_id, meta from models
        union all
        select unique_id, meta from snapshots
        union all
        select unique_id, meta from seeds
    ),

    model_run_results as (
        select * from {{ ref('model_run_results') }}
    ),

    snapshot_run_results as (
        select * from {{ ref('snapshot_run_results') }}
    ),

    {% if seed_run_results_relation -%}
    seed_run_results as (
        select * from {{ seed_run_results_relation }}
    ),
    {%- endif %}

    all_run_results as (
        {% set run_result_columns %}
            model_execution_id as alert_id,
            unique_id,
            generated_at,
            status,
            full_refresh,
            message,
            database_name,
            schema_name,
            materialization,
            tags,
            path,
            original_path,
            owner,
            alias
        {% endset -%}

        select {{ run_result_columns }} from model_run_results
        union all
        select {{ run_result_columns }} from snapshot_run_results
        {% if seed_run_results_relation -%}
        union all
        select {{ run_result_columns }} from seed_run_results
        {%- endif %}
    ),

    all_alerts as ( 
        select *
        from all_run_results
        where lower(status) != 'success'
        and {{ elementary.edr_cast_as_timestamp('generated_at') }} > {{ elementary.edr_timeadd('day', -1 * days_back, elementary.edr_current_timestamp()) }}
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


