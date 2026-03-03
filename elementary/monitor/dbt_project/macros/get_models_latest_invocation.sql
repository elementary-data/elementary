{% macro get_models_latest_invocation() %}
  {% set query %}
    with ordered_run_results as (
      select
        run_results.unique_id,
        run_results.invocation_id,
        row_number() over (partition by run_results.unique_id order by run_results.generated_at desc) as {{ elementary.escape_reserved_keywords('row_number') }}
      from {{ ref("dbt_run_results", package="elementary") }} run_results
      join {{ ref("dbt_models", package="elementary") }} models on run_results.unique_id = models.unique_id
    ),

    latest_run_results as (
      select unique_id, invocation_id
      from ordered_run_results
      where {{ elementary.escape_reserved_keywords('row_number') }} = 1
    )

    select unique_id, invocation_id from latest_run_results
  {% endset %}
  {% set run_invocations_agate = run_query(query) %}
  {% do return(elementary.agate_to_dicts(run_invocations_agate)) %}
{% endmacro %}
