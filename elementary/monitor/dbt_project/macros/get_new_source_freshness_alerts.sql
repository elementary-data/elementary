{% macro get_new_source_freshness_alerts(days_back) %}
    -- depends_on: {{ ref('alerts_source_freshness') }}
    {% set elementary_database, elementary_schema = elementary.target_database(), target.schema %}

    {% set select_new_alerts_query %}
        with new_alerts as (
            select * from {{ ref('alerts_source_freshness') }}
            where alert_sent = false and {{ elementary.cast_as_timestamp('detected_at') }} >= {{ get_alerts_time_limit(days_back) }}
        ),
        sources as (
            select * from {{ ref('elementary', 'dbt_sources') }}
        )

        select *
        from new_alerts
        join sources on new_alerts.unique_id = sources .unique_id
    {% endset %}

    {% set alerts_agate = run_query(select_new_alerts_query) %}
    {% set alerts_dicts = elementary.agate_to_dicts(alerts_agate) %}
    {% set new_alerts = [] %}
    {% for alert_dict in alerts_dicts %}
        {% set new_alert_dict = {'id': alert_dict['alert_id'],
                                 'unique_id': alert_dict['unique_id'],
                                 'detected_at': alert_dict['detected_at'],
                                 'max_loaded_at': alert_dict['max_loaded_at'],
                                 'status': alert_dict['status'],
                                 'owners': elementary.insensitive_get_dict_value(model_result_alert_dict, 'owners'),
                                 'tags': elementary.insensitive_get_dict_value(model_result_alert_dict, 'tags')
                                } %}
        {% do new_alerts.append(new_alert_dict) %}
    {% endfor %}
    {% do elementary.edr_log(tojson(new_alerts)) %}
{% endmacro %}

