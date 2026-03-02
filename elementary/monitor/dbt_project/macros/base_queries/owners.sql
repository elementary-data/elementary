{% macro get_project_owners() %}
    {% set project_owners_query %}
        with dbt_models as (
            select * from {{ ref('dbt_models', package='elementary') }}
        ),

        dbt_sources as (
            select * from {{ ref('dbt_sources', package='elementary') }}
        ),

        dbt_seeds as (
            select * from {{ ref('dbt_seeds', package='elementary') }}
        ),

        dbt_tests as (
            select * from {{ ref('dbt_tests', package='elementary') }}
        )

        select model_owners as owner from dbt_tests
        union
        select owner from dbt_models
        union
        select owner from dbt_sources
        union
        select owner from dbt_seeds  
    {% endset %}
    {% set owners_agate = run_query(project_owners_query) %}
    {% do return(elementary.agate_to_dicts(owners_agate)) %}
{% endmacro %}
