{% macro get_models_depends_on_nodes() %}
    {% set models_depends_on_nodes_query %}
        with dbt_models as (
            select * from {{ref('elementary', 'dbt_models')}}
        )

        select
            unique_id,
            depends_on_nodes
        from dbt_models      
    {% endset %}
    {% set models_depends_on_agate = run_query(models_depends_on_nodes_query) %}
    {% set models_depends_on_json = elementary.agate_to_json(models_depends_on_agate) %}
    {% do elementary.edr_log(models_depends_on_json) %}
{% endmacro %}
