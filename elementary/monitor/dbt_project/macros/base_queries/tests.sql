{% macro get_tests() %}
    {% set tests_query %}
        with dbt_tests as (
            select * from {{ ref('elementary', 'dbt_tests') }}
        ),
        
        dbt_models as (
            select * from {{ ref('elementary', 'dbt_models') }}
        )

        select
            dbt_tests.unique_id as id,
            dbt_tests.schema_name as schema,
            dbt_models.name as table,
            dbt_tests.test_column_name as column,
            dbt_tests.test_namespace as test_package,
            dbt_tests.short_name as test_name,
            dbt_tests.test_params as test_params,
            dbt_tests.severity as severity,
            dbt_tests.model_owners as model_owners,
            dbt_tests.model_tags as model_tags,
            dbt_tests.tags as tags,
            dbt_tests.generated_at as generated_at
        from dbt_tests left join dbt_models on dbt_tests.parent_model_unique_id = dbt_models.unique_id
    {% endset %}
    {% set tests_agate = run_query(tests_query) %}
    {% do return(elementary.agate_to_dicts(tests_agate)) %}
{% endmacro %}
