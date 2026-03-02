{#
  Override dbt-dremio's dateadd macro which calls interval.replace() on the
  interval parameter, failing when the interval is an integer instead of a string.

  See: dbt-dremio's macros/utils/date_spine.sql line 32:
    {% set interval = interval.replace('order by 1', '') %}

  This override casts interval to string before calling .replace(), then uses
  TIMESTAMPADD (same as the original dbt-dremio implementation).
#}

{% macro dremio__dateadd(datepart, interval, from_date_or_timestamp) %}
    {% set interval = interval | string %}
    {% set interval = interval.replace('order by 1', '') %}
    {% if datepart == 'year' %}
        select TIMESTAMPADD(SQL_TSI_YEAR, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'quarter' %}
        select TIMESTAMPADD(SQL_TSI_QUARTER, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'month' %}
        select TIMESTAMPADD(SQL_TSI_MONTH, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'week' %}
        select TIMESTAMPADD(SQL_TSI_WEEK, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'hour' %}
        select TIMESTAMPADD(SQL_TSI_HOUR, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'minute' %}
        select TIMESTAMPADD(SQL_TSI_MINUTE, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'second' %}
        select TIMESTAMPADD(SQL_TSI_SECOND, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% else %}
        select TIMESTAMPADD(SQL_TSI_DAY, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% endif %}
{% endmacro %}
