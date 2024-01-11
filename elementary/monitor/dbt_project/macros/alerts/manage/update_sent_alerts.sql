{% macro update_sent_alerts(alert_ids, sent_at, table_name) %}
    {% if execute %}
        {% if alert_ids %}
            {% set update_sent_alerts_query %}
                UPDATE {{ ref(table_name) }} set suppression_status = 'sent', sent_at = {{ "'{}'".format(sent_at) }}, alert_sent = TRUE
                WHERE alert_id IN {{ elementary.strings_list_to_tuple(alert_ids) }} and suppression_status = 'pending' and
                    {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit() }}
            {% endset %}
            {% do elementary.run_query(update_sent_alerts_query) %}
        {% endif %}
    {% endif %}
{% endmacro %}