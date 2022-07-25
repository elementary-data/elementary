{% macro get_new_model_alerts(days_back) %}
    -- depends_on: {{ ref('alerts_models') }}
    {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
    {% set snapshots_relation = adapter.get_relation(elementary_database, elementary_schema, 'dbt_snapshots') %}


    {% set select_new_alerts_query %}
        with alerts as (
            select * from {{ ref('alerts_models') }}
            where alert_sent = false and detected_at >= {{ get_alerts_time_limit(days_back) }}
        ),
        models as (
            select * from {{ ref('elementary', 'dbt_models') }}
        )
        {% if snapshots_relation %}
            ,snapshots as (
                select * from {{ snapshots_relation }}
            )
            select
                alerts.*,
                coalesce(models.meta, snapshots.meta) as model_meta
            from alerts
            left join models on alerts.unique_id = models.unique_id
            left join snapshots on alerts.unique_id = snapshots.unique_id
        {% else %}
            select
                alerts.*,
                models.meta as model_meta
            from alerts
            left join models on alerts.unique_id = models.unique_id
        {% endif %}
    {% endset %}

    {% set alerts_agate = run_query(select_new_alerts_query) %}
    {% set model_result_alert_dicts = elementary.agate_to_dicts(alerts_agate) %}
    {% set new_alerts = [] %}
    {% for model_result_alert_dict in model_result_alert_dicts %}
        {% set status = elementary.insensitive_get_dict_value(model_result_alert_dict, 'status') | lower %}
        {% set new_alert_dict = {'id': elementary.insensitive_get_dict_value(model_result_alert_dict, 'alert_id'),
                                 'unique_id': elementary.insensitive_get_dict_value(model_result_alert_dict, 'unique_id'),
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
                                 'status': status} %}
        {% do new_alerts.append(new_alert_dict) %}
    {% endfor %}
    {% do elementary.edr_log(tojson(new_alerts)) %}
{% endmacro %}

