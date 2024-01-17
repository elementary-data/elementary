{% macro update_skipped_alerts(alert_ids) %}
    {% if execute %}
        {% if alert_ids %}
            {% set update_skipped_alerts_query %}
                UPDATE {{ ref('elementary_cli', 'alerts_v2') }} set status = 'skipped', updated_at = {{ elementary.edr_current_timestamp() }}
                WHERE alert_id IN {{ elementary.strings_list_to_tuple(alert_ids) }} and status = 'pending' and
                    {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit() }}
            {% endset %}
            {% do elementary.run_query(update_skipped_alerts_query) %}
        {% endif %}
    {% endif %}
{% endmacro %}