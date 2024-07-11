{%- macro get_models_runs(days_back = 7, exclude_elementary=false) -%}
    {% set models_runs_query %}
        with model_runs as (
            select 
                *,
                rank() over (partition by unique_id order by generated_at desc) as invocations_rank_index
            from {{ ref('elementary', 'model_run_results') }}
        )

        select
            unique_id, 
            invocation_id,
            name,
            schema_name as schema,
            status,
            case
                when status != 'success' then 0
                else round({{ elementary.edr_cast_as_numeric('execution_time') }}, 1)
            end as execution_time,
            full_refresh,
            materialization,
            case when invocations_rank_index = 1 then compiled_code else NULL end as compiled_code,
            generated_at
        from model_runs
        where {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('generated_at'), elementary.edr_current_timestamp(), 'day') }} < {{ days_back }}
        {% if exclude_elementary %}
          and unique_id not like 'model.elementary.%'
        {% endif %}
        order by generated_at
    {% endset %}
    {% set models_runs_agate = run_query(models_runs_query) %}
    {% do return(elementary.agate_to_dicts(models_runs_agate)) %}
{%- endmacro -%}
