{% macro get_new_alerts(days_back, results_sample_limit = 5) %}
    -- depends_on: {{ ref('alerts') }}
    {% set select_new_alerts_query %}
        SELECT alert_id, model_unique_id, test_unique_id, detected_at, database_name, schema_name, table_name, column_name, alert_type, sub_type,
               alert_description, owners, tags, alert_results_query, other, test_name, test_params, severity, status
        FROM {{ ref('alerts') }}
        WHERE alert_sent = FALSE and detected_at >= {{ get_alerts_time_limit(days_back) }}
    {% endset %}
    {% set alerts_agate = run_query(select_new_alerts_query) %}
    {% set test_result_alert_dicts = elementary.agate_to_dicts(alerts_agate) %}
    {% set new_alerts = [] %}
    {% for test_result_alert_dict in test_result_alert_dicts %}
        {% set alert_results_query = elementary.insensitive_get_dict_value(test_result_alert_dict, 'alert_results_query') %}
        {% set alert_type = elementary.insensitive_get_dict_value(test_result_alert_dict, 'alert_type') %}
        {% set status = elementary.insensitive_get_dict_value(test_result_alert_dict, 'status') | lower %}

        {% set test_rows_sample = none %}
        {%- if (alert_type == 'dbt_test' and status in ['fail', 'warn']) or (alert_type != 'dbt_test' and status != 'error') -%}
            {% set test_rows_sample = elementary_internal.get_test_rows_sample(alert_results_query, alert_type, results_sample_limit) %}
        {%- endif -%}

        {% set new_alert_dict = {'id': elementary.insensitive_get_dict_value(test_result_alert_dict, 'alert_id'),
                                 'model_unique_id': elementary.insensitive_get_dict_value(test_result_alert_dict, 'model_unique_id'),
                                 'test_unique_id': elementary.insensitive_get_dict_value(test_result_alert_dict, 'test_unique_id'),
                                 'detected_at': elementary.insensitive_get_dict_value(test_result_alert_dict, 'detected_at'),
                                 'database_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'database_name'),
                                 'schema_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'schema_name'),
                                 'table_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'table_name'),
                                 'column_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'column_name'),
                                 'test_type': alert_type,
                                 'test_sub_type': elementary.insensitive_get_dict_value(test_result_alert_dict, 'sub_type'),
                                 'test_results_description': elementary.insensitive_get_dict_value(test_result_alert_dict, 'alert_description'),
                                 'owners': elementary.insensitive_get_dict_value(test_result_alert_dict, 'owners'),
                                 'tags': elementary.insensitive_get_dict_value(test_result_alert_dict, 'tags'),
                                 'test_results_query': alert_results_query,
                                 'test_rows_sample': test_rows_sample,
                                 'other': elementary.insensitive_get_dict_value(test_result_alert_dict, 'other'),
                                 'test_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'test_name'),
                                 'test_params': elementary.insensitive_get_dict_value(test_result_alert_dict, 'test_params'),
                                 'severity': elementary.insensitive_get_dict_value(test_result_alert_dict, 'severity'),
                                 'status': status} %}
        {% do new_alerts.append(new_alert_dict) %}
    {% endfor %}
    {% do elementary.edr_log(tojson(new_alerts)) %}
{% endmacro %}

