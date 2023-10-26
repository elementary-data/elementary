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


{% macro get_resources_columns() %}
    {% set columns_relation = ref('elementary', 'enriched_columns') %}
    {% set column_name_field = 'name' %}
    {% if not elementary.relation_exists(columns_relation) %}
        {% set columns_relation = ref('elementary', 'dbt_columns') %}
        {% set column_name_field = 'column_name' %}
    {% endif %}

    {% set resources_columns_query %}
        select
            full_table_name,
            database_name,
            schema_name,
            table_name,
            {{ column_name_field }} as column_name,
            data_type
        from {{ columns_relation }}
    {% endset %}
    {% set columns_agate = run_query(resources_columns_query) %}
    {% set columns = elementary.agate_to_dicts(columns_agate) %}
    {% set resources_columns_map = {} %}
    {% for column in columns %}
        {% set resource = column.get('full_table_name') %}
        {% set resource_columns = resources_columns_map.get(resource, []) %}
        {% do resource_columns.append({'column': column.get('column_name'), 'type': column.get('data_type')}) %}
        {% do resources_columns_map.update({resource: resource_columns}) %} 
    {% endfor %}
    {% do return(resources_columns_map) %}
{% endmacro %}
