{% macro get_tests() %}
    {% set tests_query %}
        with dbt_tests as (
            select * from {{ ref('elementary', 'dbt_tests') }}
        ),
        
        dbt_models as (
            select * from {{ ref('elementary', 'dbt_models') }}
        ),

        dbt_sources as (
            select * from {{ ref('elementary', 'dbt_sources') }}
        ),

        dbt_resources as (
            select 
                unique_id,
                name,
                NULL as source_name
            from dbt_models
            union all
            select
                unique_id,
                name,
                source_name
            from dbt_sources
        )

        select
            dbt_tests.unique_id as id,
            dbt_tests.schema_name as schema,
            dbt_resources.name as table,
            dbt_resources.source_name as source_name,
            dbt_tests.test_column_name as column,
            dbt_tests.test_namespace as test_package,
            case
                when dbt_tests.short_name is not NULL then dbt_tests.short_name
                else dbt_tests.name
            end as test_name,
            dbt_tests.test_params as test_params,
            dbt_tests.severity as severity,
            dbt_tests.model_owners as model_owners,
            dbt_tests.model_tags as model_tags,
            dbt_tests.tags as tags,
            dbt_tests.generated_at as generated_at,
            dbt_tests.meta as meta,
            case 
                when dbt_tests.type = 'singular' then TRUE
                else FALSE
            end as is_singular
        from dbt_tests left join dbt_resources on dbt_tests.parent_model_unique_id = dbt_resources.unique_id
    {% endset %}
    {% set tests_agate = run_query(tests_query) %}
    {% do return(elementary.agate_to_dicts(tests_agate)) %}
{% endmacro %}
