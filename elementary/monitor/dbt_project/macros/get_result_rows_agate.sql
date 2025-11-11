{% macro get_result_rows_agate(days_back, valid_ids_query = none) %}
  {% do return(adapter.dispatch('get_result_rows_agate', 'elementary')(days_back, valid_ids_query)) %}
{% endmacro %}

{% macro default__get_result_rows_agate(days_back, valid_ids_query = none) %}
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
  {% set res = elementary.run_query(query) %}
  {% if not res %}
    {% do return({}) %}
  {% endif %}
  {% do return(res.group_by("elementary_test_results_id")) %}
{% endmacro %}

{% macro bigquery__get_result_rows_agate(days_back, valid_ids_query = none) %}
  {% set query %}
  select
    elementary_test_results_id,
    result_row
  from {{ ref("elementary", "test_result_rows") }}
  where detected_at >= {{ elementary.edr_timeadd('day', -1 * days_back, elementary.edr_current_timestamp()) }}
  {% if valid_ids_query %}
    and elementary_test_results_id in ({{ valid_ids_query }})
  {% endif %}
  {% endset %}
  {% do return(elementary.run_query(query).group_by("elementary_test_results_id")) %}
{% endmacro %}