{%- macro get_models_runs(days_back = 7, exclude_elementary=false) -%}
    {% set models_runs_query %}
        with model_runs as (
            select
                *,
                row_number() over (partition by unique_id order by generated_at desc) as invocations_rank_index
            from {{ ref('elementary', 'model_run_results') }}
            where {{ elementary_cli.days_back_filter('generated_at', days_back, partition_column='created_at', column_is_string=true) }}
        )

        select
            unique_id,
            invocation_id,
            name,
            schema_name as {{ elementary_cli.edr_quote_identifier('schema') }},
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
        {% if exclude_elementary %}
          where unique_id not like 'model.elementary.%'
        {% endif %}
        order by generated_at
    {% endset %}
    {% set models_runs_agate = run_query(models_runs_query) %}
    {% do return(elementary.agate_to_dicts(models_runs_agate)) %}
{%- endmacro -%}
