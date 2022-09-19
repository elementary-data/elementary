{%- macro get_aggregated_models_runs(days_back = 7) -%}
    {% set aggregated_models_runs_query %}
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
        ),


        aggreagated_models_runs as (
            select
                *,
                median(execution_time) over (partition by unique_id) as median_execution_time,
                row_number() over (partition by unique_id order by generated_at) as row_number
            from current_models_runs_results
        )

        select 
            unique_id,
            name,
            status,
            round(execution_time, 3) as last_exec_time,
            compiled_sql,
            round(median_execution_time, 3) as median_exec_time,
            round(((execution_time / median_execution_time) - 1) * 100, 3) as exec_time_change_rate,
            generated_at
        from aggreagated_models_runs
        where  row_number = 1
    {%- endset -%}
    {% set aggregated_models_runs_agate = run_query(aggregated_models_runs_query) %}
    {% set aggregated_models_runs_json = elementary.agate_to_json(aggregated_models_runs_agate) %}
    {% do elementary.edr_log(aggregated_models_runs_json) %}
{%- endmacro -%}

