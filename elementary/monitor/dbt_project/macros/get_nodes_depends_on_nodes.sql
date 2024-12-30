{% macro get_nodes_depends_on_nodes(exclude_elementary=false) %}
    {% set exposures_relation = ref('elementary_cli', 'enriched_exposures') %}
    {% if not elementary.relation_exists(exposures_relation) %}
        {% set exposures_relation = ref('elementary', 'dbt_exposures') %}
    {% endif %}

    {% set models_depends_on_nodes_query %}
        select
            unique_id,
            null as depends_on_nodes,
            null as materialization,
            'seed' as type
        from {{ ref('elementary', 'dbt_seeds') }}
        union all
        select
            unique_id,
            depends_on_nodes,
            materialization,
            'snapshot' as type
        from {{ ref('elementary', 'dbt_snapshots') }}
        union all
        select
            unique_id,
            depends_on_nodes,
            materialization,
            'model' as type
        from {{ ref('elementary', 'dbt_models') }}
        {% if exclude_elementary %}
            where package_name != 'elementary'
        {% endif %}
        union all
        select
            unique_id,
            null as depends_on_nodes,
            null as materialization,
            'source' as type
        from {{ ref('elementary', 'dbt_sources') }}
        union all
        select
            unique_id,
            depends_on_nodes,
            null as materialization,
            'exposure' as type
        from {{ exposures_relation }}
    {% endset %}

    {% set models_depends_on_agate = run_query(models_depends_on_nodes_query) %}
    {% do return(elementary.agate_to_dicts(models_depends_on_agate)) %}
{% endmacro %}
