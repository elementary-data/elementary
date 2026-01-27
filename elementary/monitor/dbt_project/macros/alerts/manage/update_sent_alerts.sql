{% macro update_sent_alerts(alert_ids, sent_at) %}
    {% if execute %}
        {% if alert_ids %}
            {% set update_sent_alerts_query = elementary_cli.get_update_sent_alerts_query(alert_ids, sent_at) %}
            {% do elementary.run_query(update_sent_alerts_query) %}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro get_update_sent_alerts_query(alert_ids, sent_at) %}
    {% do return(adapter.dispatch("get_update_sent_alerts_query", "elementary_cli")(alert_ids, sent_at)) %}
{% endmacro %}

{% macro default__get_update_sent_alerts_query(alert_ids, sent_at) %}
    update {{ ref('elementary_cli', 'alerts_v2') }}
    set status = 'sent',
        sent_at = {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(sent_at)) }},
        updated_at = {{ elementary.edr_current_timestamp() }}
    where alert_id in {{ elementary.strings_list_to_tuple(alert_ids) }}
        and status = 'pending'
        and {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit() }}
{% endmacro %}

{% macro clickhouse__get_update_sent_alerts_query(alert_ids, sent_at) %}
    ALTER TABLE {{ ref('elementary_cli', 'alerts_v2') }}
    UPDATE status = 'sent',
        sent_at = {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(sent_at)) }},
        updated_at = {{ elementary.edr_current_timestamp() }}
    WHERE alert_id in {{ elementary.strings_list_to_tuple(alert_ids) }}
        and status = 'pending'
        and {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit() }}
{% endmacro %}
