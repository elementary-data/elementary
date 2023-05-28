{% macro get_resources_latest_invocation() %}
  {% set dbt_run_results = ref('dbt_run_results') %}
  {% set get_resources_latest_invocation_query %}
    with ordered_run_results as (
      select
        *,
        ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY generated_at DESC) AS row_number
      from {{ dbt_run_results }}
    ),

    latest_run_results as (
      select *
      from ordered_run_results
      where row_number = 1
    )

    select unique_id, invocation_id from latest_run_results
  {% endset %}
  {% set run_invocations_agate = run_query(get_resources_latest_invocation_query) %}
  {% do return(elementary.agate_to_dicts(run_invocations_agate)) %}
{% endmacro %}
