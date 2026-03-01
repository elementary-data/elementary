{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {% if not custom_schema_name %}
      {% do return(default_schema) %}
    {% endif %}

    {% if node.resource_type == "seed" %}
      {#- Dremio (Nessie/Iceberg): keep seeds in the default schema alongside
          models to avoid cross-schema reference issues.  Dremio's Nessie source
          cannot resolve multi-part schema paths in view SQL reliably. -#}
      {% if target.type == 'dremio' %}
        {% do return(default_schema) %}
      {% endif %}
      {% do return(custom_schema_name) %}
    {% endif %}

    {% do return("{}_{}" .format(default_schema, custom_schema_name)) %}
{%- endmacro %}
