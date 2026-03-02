{% macro generate_schema_name(custom_schema_name, node) -%}
    {#- For Dremio, delegate entirely to the adapter's dremio__generate_schema_name
        which correctly uses target.root_path for datalake nodes (seeds/tables)
        and target.schema for non-datalake nodes (views). -#}
    {% if target.type == 'dremio' %}
      {{ return(dremio__generate_schema_name(custom_schema_name, node)) }}
    {% endif %}

    {%- set default_schema = target.schema -%}
    {% if not custom_schema_name %}
      {% do return(default_schema) %}
    {% endif %}

    {% if node.resource_type == "seed" %}
      {% do return(custom_schema_name) %}
    {% endif %}

    {% do return("{}_{}" .format(default_schema, custom_schema_name)) %}
{%- endmacro %}
