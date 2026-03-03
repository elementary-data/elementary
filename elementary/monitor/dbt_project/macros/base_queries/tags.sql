{% macro get_project_tags() %}
    {% set project_tags_query %}
        with dbt_models as (
            select * from {{ ref('elementary', 'dbt_models') }}
        ),

        dbt_sources as (
            select * from {{ ref('elementary', 'dbt_sources') }}
        ),

        dbt_tests as (
            select * from {{ ref('elementary', 'dbt_tests') }}
        )

        select tags from dbt_models
        union
        select tags from dbt_sources
        union
        select tags from dbt_tests 
    {% endset %}
    {% set tags_agate = run_query(project_tags_query) %}
    {% do return(elementary.agate_to_dicts(tags_agate)) %}
{% endmacro %}
