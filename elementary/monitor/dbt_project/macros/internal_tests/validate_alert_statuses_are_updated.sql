{% macro validate_alert_statuses_are_updated() %}
    {% set alerts_with_no_updated_status_query %}
        select alert_id
        from from {{ ref('alerts_v2') }}
        where status not in ('sent', 'skipped')
    {% endset %}
    {% set alerts_agate = run_query(alerts_with_no_updated_status_query) %}
    {% set alerts_with_no_updated_status = elementary.agate_to_dicts(alerts_agate) %}
    -- When using --group-by table, singular test alerts are not sent.
    {% set alerts_with_no_updated_status_without_singulars = [] %}
    {% for alert in alerts_with_no_updated_status %}
        {% set alert_data = fromjson(alert['data']) %}
        {% if alert_data.get('test_sub_type', '') != 'singular' %}
          {% do alerts_with_no_updated_status_without_singulars.append(alert) %}
    {% endif %}
    {% endfor %}
    {% if alerts_with_no_updated_status_without_singulars %}
        {% do exceptions.raise_compiler_error("Elementary couldn't update all of the alerts status") %}
    {% endif %}
{% endmacro %}
