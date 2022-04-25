{% macro update_sent_alerts(alert_ids) %}
    -- depends_on: {{ ref('alerts') }}
    {% if alert_ids %}
        {% set update_sent_alerts_query %}
            UPDATE {{ ref('alerts') }} set alert_sent = TRUE
            WHERE alert_id IN {{ elementary.strings_list_to_tuple(alert_ids) }} and alert_sent = FALSE and
                detected_at >= {{ get_alerts_time_limit() }}
        {% endset %}
        {% set results = dbt_utils.get_query_results_as_dict(update_sent_alerts_query) %}
        {% do elementary.edr_log(results) %}
    {% endif %}
{% endmacro %}