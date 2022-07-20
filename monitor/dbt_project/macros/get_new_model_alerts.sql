{% macro get_new_model_alerts(days_back, results_sample_limit = 5) %}
    -- depends_on: {{ ref('alerts_models') }}
    {% set select_new_alerts_query %}
        SELECT * FROM {{ ref('alerts_models') }}
        WHERE alert_sent = FALSE and detected_at >= {{ get_alerts_time_limit(days_back) }}
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
                                 'status': status} %}
        {% do new_alerts.append(new_alert_dict) %}
    {% endfor %}
    {% do elementary.edr_log(tojson(new_alerts)) %}
{% endmacro %}

