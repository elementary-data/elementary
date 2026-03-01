{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {% if not custom_schema_name %}
      {% do return(default_schema) %}
    {% endif %}

    {% if node.resource_type == "seed" %}
      {#- Dremio (Nessie/Iceberg): seeds live under the datalake root at
          <database>.<schema>.<custom_schema>.<identifier>.  The database
          is already set to the datalake name (NessieSource), so the schema
          returned here must be <default_schema>.<custom_schema> to avoid
          duplicating the datalake prefix. -#}
      {% if target.type == 'dremio' %}
        {% do return("{}.{}".format(default_schema, custom_schema_name)) %}
      {% endif %}
      {% do return(custom_schema_name) %}
    {% endif %}

    {% do return("{}_{}" .format(default_schema, custom_schema_name)) %}
{%- endmacro %}
