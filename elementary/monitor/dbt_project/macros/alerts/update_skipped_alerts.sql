{% macro update_skipped_alerts(alert_ids, table_name) %}
    {% if execute %}
        {% if alert_ids %}
            {% set update_skipped_alerts_query %}
                UPDATE {{ ref(table_name) }} set suppression_status = 'skipped'
                WHERE alert_id IN {{ elementary.strings_list_to_tuple(alert_ids) }} and suppression_status = 'pending' and
                    {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit() }}
            {% endset %}
            {% do elementary.run_query(update_skipped_alerts_query) %}
        {% endif %}
    {% endif %}
{% endmacro %}