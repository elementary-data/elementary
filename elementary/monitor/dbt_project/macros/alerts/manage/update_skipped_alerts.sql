{% macro update_skipped_alerts(alert_ids) %}
    {% if execute %}
        {% if alert_ids %}
            {% set update_skipped_alerts_query = elementary_cli.get_update_skipped_alerts_query(alert_ids) %}
            {% do elementary.run_query(update_skipped_alerts_query) %}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro get_update_skipped_alerts_query(alert_ids) %}
    {% do return(adapter.dispatch("get_update_skipped_alerts_query", "elementary_cli")(alert_ids)) %}
{% endmacro %}

{% macro default__get_update_skipped_alerts_query(alert_ids) %}
    UPDATE {{ ref('elementary_cli', 'alerts_v2') }} set status = 'skipped', updated_at = {{ elementary.edr_current_timestamp() }}
    WHERE alert_id IN {{ elementary.strings_list_to_tuple(alert_ids) }} and status = 'pending' and
        {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit() }}
{% endmacro %}

{% macro clickhouse__get_update_skipped_alerts_query(alert_ids) %}
    ALTER TABLE {{ ref('elementary_cli', 'alerts_v2') }} UPDATE status = 'skipped', updated_at = {{ elementary.edr_current_timestamp() }}
    WHERE alert_id IN {{ elementary.strings_list_to_tuple(alert_ids) }} and status = 'pending' and
        {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit() }}
{% endmacro %}
