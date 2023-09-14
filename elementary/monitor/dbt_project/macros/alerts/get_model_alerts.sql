{% macro get_pending_model_alerts(days_back) %}
    -- depends_on: {{ ref('alerts_models') }}
    {% set elementary_database, elementary_schema = elementary.target_database(), target.schema %}
    {% set snapshots_relation = adapter.get_relation(elementary_database, elementary_schema, 'dbt_snapshots') %}


    {% set select_pending_alerts_query %}
        with alerts_in_time_limit as (
            select *
            from {{ ref('alerts_models') }}
            where {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit(days_back) }}
        ),

        models as (
            select * from {{ ref('elementary', 'dbt_models') }}
        ),

        {% if snapshots_relation %}
            snapshots as (
                select * from {{ snapshots_relation }}
            ),
        {% endif %}

        artifacts_meta as (
            select unique_id, meta from models
            {% if snapshots_relation %}
                union all
                select unique_id, meta from {{ snapshots_relation }}
            {% endif %}
        ),

        extended_alerts as (
            select
                alerts_in_time_limit.alert_id,
                alerts_in_time_limit.unique_id,
                {# Currently alert_class_id equals to unique_id - might change in the future so we return both #}
                alerts_in_time_limit.unique_id as alert_class_id,
                alerts_in_time_limit.detected_at,
                alerts_in_time_limit.database_name,
                alerts_in_time_limit.materialization,
                alerts_in_time_limit.path,
                alerts_in_time_limit.original_path,
                alerts_in_time_limit.schema_name,
                alerts_in_time_limit.message,
                alerts_in_time_limit.owners,
                alerts_in_time_limit.tags,
                alerts_in_time_limit.alias,
                alerts_in_time_limit.status,
                alerts_in_time_limit.full_refresh,
                {# backwards compatibility #}
                case
                    when alerts_in_time_limit.suppression_status is NULL and alerts_in_time_limit.alert_sent = TRUE then 'sent'
                    when alerts_in_time_limit.suppression_status is NULL and alerts_in_time_limit.alert_sent = FALSE then 'pending'
                    else suppression_status
                end as suppression_status,
                alerts_in_time_limit.sent_at,
                artifacts_meta.meta as model_meta
            from alerts_in_time_limit
            left join models on alerts_in_time_limit.unique_id = models.unique_id
            left join artifacts_meta on alerts_in_time_limit.unique_id = artifacts_meta.unique_id
        )

        select *
        from extended_alerts
        where suppression_status = 'pending'
    {% endset %}

    {% set alerts_agate = run_query(select_pending_alerts_query) %}
    {% set model_result_alert_dicts = elementary.agate_to_dicts(alerts_agate) %}
    {% set pending_alerts = [] %}
    {% for model_result_alert_dict in model_result_alert_dicts %}
        {% set status = elementary.insensitive_get_dict_value(model_result_alert_dict, 'status') | lower %}
        {% set pending_alert_dict = {'id': elementary.insensitive_get_dict_value(model_result_alert_dict, 'alert_id'),
                                 'model_unique_id': elementary.insensitive_get_dict_value(model_result_alert_dict, 'unique_id'),
                                 'alert_class_id': elementary.insensitive_get_dict_value(model_result_alert_dict, 'alert_class_id'),
                                 'alias': elementary.insensitive_get_dict_value(model_result_alert_dict, 'alias'),
                                 'path': elementary.insensitive_get_dict_value(model_result_alert_dict, 'path'),
                                 'original_path': elementary.insensitive_get_dict_value(model_result_alert_dict, 'original_path'),
                                 'materialization': elementary.insensitive_get_dict_value(model_result_alert_dict, 'materialization'),
                                 'detected_at': elementary.insensitive_get_dict_value(model_result_alert_dict, 'detected_at'),
                                 'database_name': elementary.insensitive_get_dict_value(model_result_alert_dict, 'database_name'),
                                 'schema_name': elementary.insensitive_get_dict_value(model_result_alert_dict, 'schema_name'),
                                 'full_refresh': elementary.insensitive_get_dict_value(model_result_alert_dict, 'full_refresh'),
                                 'message': elementary.insensitive_get_dict_value(model_result_alert_dict, 'message'),
                                 'owners': elementary.insensitive_get_dict_value(model_result_alert_dict, 'owners'),
                                 'tags': elementary.insensitive_get_dict_value(model_result_alert_dict, 'tags'),
                                 'model_meta': elementary.insensitive_get_dict_value(model_result_alert_dict, 'model_meta'),
                                 'suppression_status': elementary.insensitive_get_dict_value(model_result_alert_dict, 'suppression_status'),
                                 'sent_at': elementary.insensitive_get_dict_value(model_result_alert_dict, 'sent_at'),
                                 'status': status} %}
        {% do pending_alerts.append(pending_alert_dict) %}
    {% endfor %}
    {% do return(pending_alerts) %}
{% endmacro %}


{% macro get_last_model_alert_sent_times(days_back) %}
    -- depends_on: {{ ref('alerts_models') }}
    {% set select_last_alert_sent_times_query %}
        with alerts_in_time_limit as (
            select
                unique_id as alert_class_id,
                case
                    when suppression_status is NULL and alert_sent = TRUE then 'sent'
                    when suppression_status is NULL and alert_sent = FALSE then 'pending'
                    else suppression_status
                end as suppression_status,
                sent_at
            from {{ ref('alerts_models') }}
            where {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit(days_back) }}
        )

        select 
            alert_class_id,
            max(sent_at) as last_sent_at
        from alerts_in_time_limit
        where suppression_status = 'sent'
        group by alert_class_id
    {% endset %}

    {% set alerts_agate = run_query(select_last_alert_sent_times_query) %}
    {% set last_alert_sent_time_result_dicts = elementary.agate_to_dicts(alerts_agate) %}
    {% set last_alert_times = {} %}
    {% for last_alert_sent_time_result_dict in last_alert_sent_time_result_dicts %}
        {% do last_alert_times.update({
            last_alert_sent_time_result_dict.get('alert_class_id'): last_alert_sent_time_result_dict.get('last_sent_at')
        }) %}
    {% endfor %}
    {% do return(last_alert_times) %}
{% endmacro %}
