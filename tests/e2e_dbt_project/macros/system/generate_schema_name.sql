{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {% if not custom_schema_name %}
      {% do return(default_schema) %}
    {% endif %}

    {% if node.resource_type == "seed" %}
      {% do return(custom_schema_name) %}
    {% endif %}

    {#- For Dremio with enterprise_catalog_namespace, delegate to the adapter's
        generate_schema_name for non-seed nodes (views/tables) so that
        root_path is correctly applied. Seeds use flat schema (e.g. test_seeds)
        to avoid nested Nessie namespaces that Dremio can't create folders for. -#}
    {% if target.type == 'dremio' %}
      {{ return(dremio__generate_schema_name(custom_schema_name, node)) }}
    {% endif %}

    {% do return("{}_{}".format(default_schema, custom_schema_name)) %}
{%- endmacro %}
