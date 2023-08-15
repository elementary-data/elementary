{% macro get_models_latest_invocations_data() %}
  {% set invocations_relation = ref("elementary", "dbt_invocations") %}
  {% set column_exists = elementary.column_exists_in_relation(invocations_relation, 'job_url') %}

  {% set query %}
    with ordered_run_results as (
      select
        *,
        ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY generated_at DESC) AS row_number
      from {{ ref("elementary", "dbt_run_results") }}
      where resource_type = 'model'
    ),

    latest_run_results as (
      select *
      from ordered_run_results
      where row_number = 1
    )

    select
      invocation_id,
      command,
      selected,
      full_refresh,
      {% if column_exists %}
        job_url,
      {% endif %}
      job_name,
      job_id,
      orchestrator
    from {{ invocations_relation }}
  {% endset %}
  {% set result = elementary.run_query(query) %}
  {% do return(elementary.agate_to_dicts(result)) %}
{% endmacro %}
