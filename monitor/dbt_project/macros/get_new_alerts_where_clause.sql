{% macro get_new_alerts_where_clause(alerts_model_relation) %}

    {% set last_alert_time_with_backfill_query %}
        {% set backfill_detected_at = elementary.timeadd('day', '-2', 'max(detected_at)') %}
        select {{ backfill_detected_at }} from {{ alerts_model_relation }}
    {% endset %}

    {% set last_alert_time_with_backfill = "'" ~ elementary.result_value(last_alert_time_with_backfill_query) ~ "'" %}

    {%- set row_count = elementary.get_row_count(alerts_model_relation) %}
    {%- if row_count > 0 %}
        {% set where_clause %}
            where detected_at > {{ last_alert_time_with_backfill }}
            and alert_id not in (
                select alert_id
                from {{ alerts_model_relation }}
                where detected_at > {{ last_alert_time_with_backfill }}
                and alert_sent = true
            )
        {% endset %}
        {{ return(where_clause) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}
