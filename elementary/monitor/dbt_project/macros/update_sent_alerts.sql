{% macro update_sent_alerts(alert_ids, table_name) %}
    {% if alert_ids %}
        {% set update_sent_alerts_query %}
            UPDATE {{ ref(table_name) }} set alert_sent = TRUE
            WHERE alert_id IN {{ elementary.strings_list_to_tuple(alert_ids) }} and alert_sent = FALSE and
                {{ elementary.cast_as_timestamp('detected_at') }} >= {{ get_alerts_time_limit() }}
        {% endset %}
        {% do dbt.run_query(update_sent_alerts_query) %}
    {% endif %}
{% endmacro %}