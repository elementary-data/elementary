{% macro update_sent_alerts(alert_ids, sent_at) %}
    {% if execute %}
        {% if alert_ids %}
            {% set update_sent_alerts_query %}
                update {{ ref('elementary_cli', 'alerts_v2') }}
                set status = 'sent',
                    sent_at = {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(sent_at)) }},
                    updated_at = {{ elementary.edr_current_timestamp() }}
                where alert_id in {{ elementary.strings_list_to_tuple(alert_ids) }}
                    and status = 'pending'
                    and {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit() }}
            {% endset %}
            {% do elementary.run_query(update_sent_alerts_query) %}
        {% endif %}
    {% endif %}
{% endmacro %}
