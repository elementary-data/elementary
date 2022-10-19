{%- macro get_models_runs(days_back = 7, exclude_elementary=false) -%}
    {% set models_runs_query %}
        with model_runs as (
            select * from {{ ref('elementary', 'model_run_results') }}
        )

        select
            unique_id, 
            invocation_id,
            name,
            schema_name as schema,
            status,
            case
                when status != 'success' then 0
                else round(execution_time, 1)
            end as execution_time,
            full_refresh,
            materialization,
            generated_at
        from model_runs
        where {{ elementary.datediff(elementary.cast_as_timestamp('generated_at'), elementary.current_timestamp(), 'day') }} < {{ days_back }}
        {% if exclude_elementary %}
          and unique_id not like 'model.elementary.%'
        {% endif %}
        order by generated_at
    {% endset %}
    {% set models_runs_agate = run_query(models_runs_query) %}
    {% set models_runs_results = elementary.agate_to_json(models_runs_agate) %}
    {% do elementary.edr_log(models_runs_results) %}
{%- endmacro -%}
