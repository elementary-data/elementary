{% macro get_result_rows_agate(days_back) %}
  {% set query %}
  select
    elementary_test_results_id,
    result_row
  from {{ ref("elementary", "test_result_rows") }}
  where {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('detected_at'), elementary.edr_current_timestamp(), 'day') }} < {{ days_back }}
  {% endset %}
  {% do return(elementary.run_query(query).group_by("elementary_test_results_id")) %}
{% endmacro %}
