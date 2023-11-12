{% macro get_new_alerts_where_clause(alerts_model_relation) %}

    {% set last_alert_time_with_backfill_query %}
        {% set backfill_detected_at = elementary.edr_timeadd('day', '-2', 'max(detected_at)') %}
        select {{ backfill_detected_at }} from {{ alerts_model_relation }}
    {% endset %}

    {% set last_alert_time_with_backfill = elementary.result_value(last_alert_time_with_backfill_query) %}

    {%- if last_alert_time_with_backfill %}
        {% set detected_at_ts = elementary.edr_cast_as_timestamp('detected_at') %}
        {% set last_ts = elementary.edr_cast_as_timestamp(elementary.edr_quote(last_alert_time_with_backfill)) %}
        where {{ detected_at_ts }} > {{ last_ts }}
        and alert_id not in (
            select alert_id
            from {{ alerts_model_relation }}
            where {{ detected_at_ts }} > {{ last_ts }}
        )
    {%- endif %}
{% endmacro %}
