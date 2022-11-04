{% macro get_new_source_freshness_alerts(days_back) %}
    -- depends_on: {{ ref('alerts_source_freshness') }}
    {% set select_new_alerts_query %}
        with new_alerts as (
            select * from {{ ref('alerts_source_freshness') }}
            where alert_sent = false and {{ elementary.cast_as_timestamp('detected_at') }} >= {{ get_alerts_time_limit(days_back) }}
        )
        select * from new_alerts
    {% endset %}

    {% set alerts_agate = run_query(select_new_alerts_query) %}
    {% set alerts_dicts = elementary.agate_to_dicts(alerts_agate) %}
    {% set new_alerts = [] %}
    {% for alert_dict in alerts_dicts %}
        {% set new_alert_dict = {'id': alert_dict.get('alert_id'),
                                 'unique_id': alert_dict.get('unique_id'),
                                 'detected_at': alert_dict.get('detected_at'),
                                 'snapshotted_at': alert_dict.get('snapshotted_at'),
                                 'max_loaded_at': alert_dict.get('max_loaded_at'),
                                 'max_loaded_at_time_ago_in_s': alert_dict.get('max_loaded_at_time_ago_in_s'),
                                 'database_name': alert_dict.get('database_name'),
                                 'schema_name': alert_dict.get('schema_name'),
                                 'source_name': alert_dict.get('source_name'),
                                 'identifier': alert_dict.get('identifier'),
                                 'freshness_error_after': alert_dict.get('freshness_error_after'),
                                 'freshness_warn_after': alert_dict.get('freshness_warn_after'),
                                 'freshness_filter': alert_dict.get('freshness_filter'),
                                 'status': alert_dict.get('status'),
                                 'owners': alert_dict.get('owner'),
                                 'path': alert_dict.get('path'),
                                 'error': alert_dict.get('error'),
                                 'tags': alert_dict.get('tags')
                                } %}
        {% do new_alerts.append(new_alert_dict) %}
    {% endfor %}
    {% do elementary.edr_log(tojson(new_alerts)) %}
{% endmacro %}
