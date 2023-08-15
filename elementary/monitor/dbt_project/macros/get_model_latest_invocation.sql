{% macro get_model_latest_invocation() %}
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

    select unique_id, invocation_id from latest_run_results
  {% endset %}
  {% set run_invocations_agate = run_query(query) %}
  {% do return(elementary.agate_to_dicts(run_invocations_agate)) %}
{% endmacro %}
