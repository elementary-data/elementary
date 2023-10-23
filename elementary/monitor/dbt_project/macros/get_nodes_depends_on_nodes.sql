{% macro get_nodes_depends_on_nodes(exclude_elementary=false) %}
    {% set exposures_relation = ref('elementary_cli', 'enriched_exposures') %}
    {% if not elementary.relation_exists(exposures_relation) %}
        {% set exposures_relation = ref('elementary', 'dbt_exposures') %}
    {% endif %}
    {% set models_depends_on_nodes_query %}
        with dbt_models as (
            select * from {{ ref('elementary', 'dbt_models') }}
            {% if exclude_elementary %}
              where package_name != 'elementary'
            {% endif %}
        ),

        dbt_sources as (
            select * from {{ ref('elementary', 'dbt_sources') }}
        ),

        dbt_exposures as (
            select * from {{ exposures_relation }}
        )

        select
            unique_id,
            depends_on_nodes,
            'model' as type
        from dbt_models
        union all
        select
            unique_id,
            null as depends_on_nodes,
            'source' as type
        from dbt_sources
        union all
        select
            unique_id,
            depends_on_nodes,
            'exposure' as type
        from dbt_exposures
    {% endset %}
    {% set models_depends_on_agate = run_query(models_depends_on_nodes_query) %}
    {% do return(elementary.agate_to_dicts(models_depends_on_agate)) %}
{% endmacro %}
