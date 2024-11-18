{% macro get_result_rows_agate(days_back, valid_ids_query = none) %}
  {% set query %}
  select
    elementary_test_results_id,
    result_row
  from {{ ref("elementary", "test_result_rows") }}
  where {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('detected_at'), elementary.edr_current_timestamp(), 'day') }} < {{ days_back }}
  {% if valid_ids_query %}
    and elementary_test_results_id in ({{ valid_ids_query }})
  {% endif %}
  {% endset %}
  {% do return(elementary.run_query(query).group_by("elementary_test_results_id")) %}
{% endmacro %}
