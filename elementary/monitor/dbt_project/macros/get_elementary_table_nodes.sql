{% macro get_elementary_table_nodes() %}
    {% set table_materializations = ["table", "incremental"] %}
    {% set table_nodes = graph.nodes.values() | selectattr('package_name', '==', 'elementary') | selectattr('config.materialized', 'in', table_materializations) %}
    {% do return(table_nodes | list) %}
{% endmacro %}
