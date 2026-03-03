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
    {% set datepart = datepart | lower %}
    {% set interval = interval | string %}
    {# dbt-dremio's original macro wraps the result in a scalar subquery
       ("select TIMESTAMPADD(...) order by 1"), so when we receive the
       interval from upstream it may carry a trailing "order by 1". #}
    {% set interval = interval.replace('order by 1', '') %}
    {% if datepart == 'year' %}
        TIMESTAMPADD(YEAR, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'quarter' %}
        TIMESTAMPADD(QUARTER, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'month' %}
        TIMESTAMPADD(MONTH, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'week' %}
        TIMESTAMPADD(WEEK, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'hour' %}
        TIMESTAMPADD(HOUR, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'minute' %}
        TIMESTAMPADD(MINUTE, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'second' %}
        TIMESTAMPADD(SECOND, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% elif datepart == 'day' %}
        TIMESTAMPADD(DAY, CAST({{interval}} as int), CAST({{from_date_or_timestamp}} as TIMESTAMP))
    {% else %}
        {{ exceptions.raise_compiler_error("dremio__dateadd: unrecognized datepart '" ~ datepart ~ "'. Supported: year, quarter, month, week, day, hour, minute, second.") }}
    {% endif %}
{% endmacro %}
