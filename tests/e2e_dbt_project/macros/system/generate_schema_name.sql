{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {% if not custom_schema_name %}
      {% do return(default_schema) %}
    {% endif %}

    {% if node.resource_type == "seed" %}
      {% do return(custom_schema_name) %}
    {% endif %}

    {% do return("{}_{}".format(default_schema, custom_schema_name)) %}
{%- endmacro %}
