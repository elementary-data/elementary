{% macro get_alerts_time_limit(days_back=31) %}
    {% set today = elementary.date_trunc('day', elementary.current_timestamp()) %}
    {% set datetime_limit = elementary.dateadd('day', days_back * -1, today) %}
    {{ return(elementary.cast_as_timestamp(datetime_limit)) }}
{% endmacro %}
