{%- macro get_models_runs(days_back = 7) -%}
    {% set models_runs_query %}
        with models as (
            select * from {{ ref('elementary', 'dbt_models') }}
        ),

        dbt_run_results as (
            select * from {{ ref('elementary', 'dbt_run_results') }}
        ),

        current_models_runs_results as (
            select run_results.*
            from dbt_run_results run_results
            join models on run_results.unique_id = models.unique_id
            where resource_type = 'model'
            and {{ elementary.datediff(elementary.cast_as_timestamp('run_results.generated_at'), elementary.current_timestamp(), 'day') }} < {{ days_back }}
        )

        select
            unique_id, 
            invocation_id,
            name,
            status,
            execution_time,
            full_refresh,
            compiled_sql,
            generated_at
        from current_models_runs_results
        order by generated_at
    {% endset %}
    {% set models_runs_agate = run_query(models_runs_query) %}
    {% set models_runs_results = elementary.agate_to_json(models_runs_agate) %}
    {% do elementary.edr_log(models_runs_results) %}
{%- endmacro -%}