{% macro generate_schema_name(custom_schema_name, node) -%}
    {#- For Dremio, delegate to the adapter's dremio__generate_schema_name which
        prefixes root_path for datalake nodes (seeds/tables) so that dot-separated
        folder paths render correctly via DremioRelation.quoted_by_component. -#}
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
