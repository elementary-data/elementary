{% macro get_result_rows_agate(days_back, valid_ids_query = none) %}
  {% set query %}
  select
    elementary_test_results_id,
    result_row
  from {{ ref("test_result_rows", package="elementary") }}
  where {{ elementary_cli.days_back_filter('detected_at', days_back, partition_column='created_at') }}
  {% if valid_ids_query %}
    and elementary_test_results_id in ({{ valid_ids_query }})
  {% endif %}
  {% endset %}
  {% set res = elementary.run_query(query) %}
  {% if not res %}
    {% do return({}) %}
  {% endif %}
  {% do return(res.group_by("elementary_test_results_id")) %}
{% endmacro %}
