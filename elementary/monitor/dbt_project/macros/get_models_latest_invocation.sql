{% macro get_models_latest_invocation() %}
  {% set query %}
    with ordered_run_results as (
      select
        *,
        row_number() over (partition by unique_id order by run_results.generated_at desc) as {{ elementary.escape_reserved_keywords('row_number') }}
      from {{ ref("dbt_run_results", package="elementary") }} run_results
      join {{ ref("dbt_models", package="elementary") }} using (unique_id)
    ),

    latest_run_results as (
      select *
      from ordered_run_results
      where {{ elementary.escape_reserved_keywords('row_number') }} = 1
    )

    select unique_id, invocation_id from latest_run_results
  {% endset %}
  {% set run_invocations_agate = run_query(query) %}
  {% do return(elementary.agate_to_dicts(run_invocations_agate)) %}
{% endmacro %}
