{#
  Override dbt-dremio's dateadd macro which has two bugs:
  1. Calls interval.replace() on the interval parameter, failing when interval is an integer
  2. Wraps result in "select TIMESTAMPADD(...)" which creates a scalar subquery when
     embedded in larger SQL expressions, causing $SCALAR_QUERY errors in Dremio

  This override:
  - Casts interval to string before calling .replace()
  - Outputs just TIMESTAMPADD(...) as an expression (no "select" prefix)
#}

{% macro dremio__dateadd(datepart, interval, from_date_or_timestamp) %}
    {% set interval = interval | string %}
    {% set interval = interval.replace('order by 1', '') %}
    {% if datepart == 'year' %}
        TIMESTAMPADD(SQL_TSI_YEAR, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'quarter' %}
        TIMESTAMPADD(SQL_TSI_QUARTER, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'month' %}
        TIMESTAMPADD(SQL_TSI_MONTH, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'week' %}
        TIMESTAMPADD(SQL_TSI_WEEK, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'hour' %}
        TIMESTAMPADD(SQL_TSI_HOUR, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'minute' %}
        TIMESTAMPADD(SQL_TSI_MINUTE, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'second' %}
        TIMESTAMPADD(SQL_TSI_SECOND, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% else %}
        TIMESTAMPADD(SQL_TSI_DAY, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% endif %}
{% endmacro %}
