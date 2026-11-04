{#
  Filters `column` to the last `days_back` days.

  Pass `partition_column` when `column` is not the table's partition column, or is a string and so
  has to be cast: BigQuery prunes only on the raw partition column, and without this the query
  scans all history. Its bound carries a day of slack because the two columns are stamped from
  different clocks — the guarded ones by the dbt client, `created_at` by the warehouse on insert.
#}

{% macro days_back_filter(column, days_back, partition_column=none, column_is_string=false) %}
  {% do return(
    adapter.dispatch("days_back_filter", "elementary_cli")(
      column, days_back, partition_column, column_is_string
    )
  ) %}
{% endmacro %}

{% macro default__days_back_filter(
  column, days_back, partition_column=none, column_is_string=false
) %}
  {%- set days_diff = elementary.edr_datediff(
    elementary.edr_cast_as_timestamp(column), elementary.edr_current_timestamp(), "day"
  ) -%}
  {% do return(days_diff ~ " < " ~ days_back) %}
{% endmacro %}

{% macro bigquery__days_back_filter(
  column, days_back, partition_column=none, column_is_string=false
) %}
  {%- set filtered = elementary.edr_cast_as_timestamp(column) if column_is_string else column -%}
  {%- set conditions = [
    filtered ~ " > " ~ elementary.edr_timeadd(
      "day", -1 * (days_back | int), elementary.edr_current_timestamp()
    )
  ] -%}
  {%- if partition_column and partition_column != column -%}
    {%- do conditions.append(
      partition_column ~ " > " ~ elementary.edr_timeadd(
        "day", -1 * ((days_back | int) + 1), elementary.edr_current_timestamp()
      )
    ) -%}
  {%- endif -%}
  {% do return(conditions | join(" and ")) %}
{% endmacro %}
