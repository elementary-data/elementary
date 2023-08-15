{% macro get_models_latest_invocation() %}
  {% set query %}
    with ordered_run_results as (
      select
        *,
        row_number() over (partition by unique_id order by run_results.generated_at desc) as row_number
      from {{ ref("elementary", "dbt_run_results") }} run_results
      join {{ ref("elementary", "dbt_models") }} using (unique_id)
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
