{% macro get_alerts_time_limit_hour(hours_back=24) %}
    {% set nowtime = elementary.edr_current_timestamp() %}
	{% set datetime_limit = elementary.edr_timeadd('hour', hours_back * -1, nowtime) %}
    {{ return(datetime_limit) }}
{% endmacro %}
