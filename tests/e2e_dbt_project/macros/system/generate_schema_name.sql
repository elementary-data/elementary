{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {% if not custom_schema_name %}
      {% do return(default_schema) %}
    {% endif %}

    {#- For Dremio with enterprise_catalog_namespace, delegate to the adapter's
        generate_schema_name so that root_path is correctly applied. -#}
    {% if target.type == 'dremio' %}
      {{ return(dremio__generate_schema_name(custom_schema_name, node)) }}
    {% endif %}

    {% do return("{}_{}".format(default_schema, custom_schema_name)) %}
{%- endmacro %}
