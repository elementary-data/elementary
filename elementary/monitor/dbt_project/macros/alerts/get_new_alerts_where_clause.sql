{% macro get_new_alerts_where_clause(alerts_model_relation) %}

    {% set last_alert_time_with_backfill_query %}
        {% set backfill_detected_at = elementary.timeadd('day', '-2', 'max(detected_at)') %}
        select {{ backfill_detected_at }} from {{ alerts_model_relation }}
    {% endset %}

    {% set last_alert_time_with_backfill = elementary.result_value(last_alert_time_with_backfill_query) %}

    {%- if last_alert_time_with_backfill %}
            where detected_at > '{{ last_alert_time_with_backfill }}'
            and alert_id not in (
                select alert_id
                from {{ alerts_model_relation }}
                where detected_at > '{{ last_alert_time_with_backfill }}'
            )
    {%- endif %}
{% endmacro %}
