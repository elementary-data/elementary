{% macro get_models_latest_invocations_data() %}
  {% set invocations_relation = ref("dbt_invocations", package="elementary") %}

  {% set query %}
    with ordered_run_results as (
      select
        run_results.invocation_id,
        row_number() over (partition by run_results.unique_id order by run_results.generated_at desc) as {{ elementary.escape_reserved_keywords('row_number') }}
      from {{ ref("dbt_run_results", package="elementary") }} run_results
      join {{ ref("dbt_models", package="elementary") }} models on run_results.unique_id = models.unique_id
    ),

    latest_models_invocations as (
      select distinct invocation_id
      from ordered_run_results
      where {{ elementary.escape_reserved_keywords('row_number') }} = 1
    )

    select
      invocations.invocation_id,
      invocations.command,
      invocations.selected,
      invocations.full_refresh,
      invocations.job_url,
      invocations.job_name,
      invocations.job_id,
      invocations.job_run_id,
      invocations.job_run_url,
      invocations.orchestrator
    from {{ invocations_relation }} invocations
    join latest_models_invocations on invocations.invocation_id = latest_models_invocations.invocation_id
  {% endset %}
  {% set result = elementary.run_query(query) %}
  {% do return(elementary.agate_to_dicts(result)) %}
{% endmacro %}
