{% macro get_alerts_time_limit(days_back=31) %}
    {% set today = elementary.edr_date_trunc('day', elementary.edr_current_timestamp()) %}
    {% set datetime_limit = elementary.edr_dateadd('day', days_back * -1, today) %}
    {{ return(elementary.edr_cast_as_timestamp(datetime_limit)) }}
{% endmacro %}
