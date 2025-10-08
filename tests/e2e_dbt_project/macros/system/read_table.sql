{% macro read_table(table, where=none, column_names=none) %}
  {% set query %}
    select
    {% if column_names %}
      {{ elementary.escape_select(column_names) }}
    {% else %}
      *
    {% endif %}
    from {{ ref(table) }}
    {% if where %}
      where {{ where }}
    {% endif %}
  {% endset %}

  {% set results = elementary.run_query(query) %}
  {% set results_json = elementary.agate_to_json(results) %}
  {% do elementary.edr_log(results_json) %}
{% endmacro %}
