{% macro get_alerts_time_limit(days_back=31) %}
    {% set today = dbt_utils.date_trunc('day', dbt_utils.current_timestamp()) %}
    {% set datetime_limit = dbt_utils.dateadd('day', days_back * -1, today) %}
    {{ return(elementary.cast_as_timestamp(datetime_limit)) }}
{% endmacro %}
