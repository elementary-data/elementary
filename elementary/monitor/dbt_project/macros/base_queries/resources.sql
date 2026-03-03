{% macro get_model_resources(exclude_elementary=true) %}
    {% set model_resources_query %}
        with dbt_models as (
            select * from {{ ref('elementary', 'dbt_models') }}
        )

        select
            unique_id,
            name,
            schema_name as schema,
            tags,
            owner as owners,
            database_name as database
        from dbt_models
        {% if exclude_elementary %}
            where package_name != 'elementary'
        {% endif %}
    {% endset %}
    {% set models_agate = run_query(model_resources_query) %}
    {% do return(elementary.agate_to_dicts(models_agate)) %}
{% endmacro %}


{% macro get_source_resources(exclude_elementary=true) %}
    {% set source_resources_query %}
        with dbt_sources as (
            select * from {{ ref('elementary', 'dbt_sources') }}
        )

        select
            unique_id,
            name,
            source_name,
            schema_name AS schema,
            tags,
            owner AS owners,
            database_name as database
        from dbt_sources
        {% if exclude_elementary %}
            where package_name != 'elementary'
        {% endif %}
    {% endset %}
    {% set sources_agate = run_query(source_resources_query) %}
    {% do return(elementary.agate_to_dicts(sources_agate)) %}
{% endmacro %}


{% macro get_all_resources(exclude_elementary=true) %}
    {% set models = model_resources(exclude_elementary) %}
    {% set sources = source_resources(exclude_elementary) %}
    
    {% set resources = [] %}
    {% do resources.extend(sources) %}
    {% for model in models %}
        {% do model.update({"source_name": none}) %}
        {% do resources.append(model) %}
    {% endfor %}
    {% do return(resources) %}
{% endmacro %}


{% macro get_resources_meta() %}
    {% set resources_meta_query %}
        with dbt_models as (
            select * from {{ ref('elementary', 'dbt_models') }}
        ),

        dbt_sources as (
            select * from {{ ref('elementary', 'dbt_sources') }}
        ),

        dbt_seeds as (
            select * from {{ ref('elementary', 'dbt_seeds') }}
        ),

        dbt_tests as (
            select * from {{ ref('elementary', 'dbt_tests') }}
        )

        select meta from dbt_tests
        union
        select meta from dbt_models
        union
        select meta from dbt_sources
        union
        select meta from dbt_seeds  
    {% endset %}
    {% set resources_meta_agate = run_query(resources_meta_query) %}
    {% do return(elementary.agate_to_dicts(resources_meta_agate)) %}
{% endmacro %}
