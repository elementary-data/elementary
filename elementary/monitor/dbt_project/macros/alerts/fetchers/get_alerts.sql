{% macro get_pending_alerts(days_back, type=none) %}
    -- depends_on: {{ ref('alerts_v2') }}
    {% set select_pending_alerts_query %}
        with alerts_in_time_limit as (
            select *
            from {{ ref('elementary_cli', 'alerts_v2') }}
            where {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit(days_back) }}
            {% if type %}
                and lower(type) = {{ elementary.edr_quote(type | lower) }}
            {% endif %}
        )

        select 
            alert_id as id,
            alert_class_id,
            type,
            detected_at,
            created_at,
            updated_at,
            status,
            data,
            sent_at
        from alerts_in_time_limit
        where status = 'pending'
    {% endset %}

    {% set alerts_agate = elementary.run_query(select_pending_alerts_query) %}
    {% set alerts_dicts = elementary.agate_to_dicts(alerts_agate) %}
    {% do return(alerts_dicts) %}
{% endmacro %}


{% macro get_last_alert_sent_times(days_back, type=none) %}
    -- depends_on: {{ ref('alerts_v2') }}
    {% set select_last_alert_sent_times_query %}
        with alerts_in_time_limit as (
            select
                alert_class_id,
                status,
                sent_at
            from {{ ref('alerts_v2') }}
            where {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit(days_back) }}
            {% if type %}
                and lower(type) = {{ elementary.edr_quote(type | lower) }}
            {% endif %}
        )

        select 
            alert_class_id,
            max(sent_at) as last_sent_at
        from alerts_in_time_limit
        where status = 'sent'
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
