{% macro get_elementary_table_nodes() %}
    {% set table_materializations = ["table", "incremental"] %}
    {% set table_nodes = graph.nodes.values() | selectattr('config.materialized', 'in', table_materializations) %}
    {% do elementary.edr_log(tojson(table_nodes | list)) %}
{% endmacro %}
