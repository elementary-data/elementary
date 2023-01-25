{% macro get_test_result_rows_query(days_back) %}
select
  test_execution_id,
  result_row
from {{ ref("test_result_rows") }}
where {{ elementary.datediff(elementary.cast_as_timestamp('detected_at'), elementary.current_timestamp(), 'day') }} < {{ days_back }}
{% endmacro %}
