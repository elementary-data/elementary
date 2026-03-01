{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {% if not custom_schema_name %}
      {% do return(default_schema) %}
    {% endif %}

    {% if node.resource_type == "seed" %}
      {#- Dremio (Nessie/Iceberg): seeds are datalake nodes whose path is
          root_path + custom_schema.  Views that ref() seeds resolve database
          to target.datalake but schema via this macro.  We must return the
          full root_path-qualified schema so the relation renders correctly
          as  <datalake>.<root_path>.<custom_schema>.<identifier>. -#}
      {% if target.type == 'dremio' %}
        {% do return("{}.{}".format(target.root_path, custom_schema_name)) %}
      {% endif %}
      {% do return(custom_schema_name) %}
    {% endif %}

    {% do return("{}_{}" .format(default_schema, custom_schema_name)) %}
{%- endmacro %}
