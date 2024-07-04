{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set schema_name = target.schema -%}
    {% if custom_schema_name %}
        {% set schema_name = "{}_{}".format(schema_name, custom_schema_name) %}
    {% endif %}

    {% set schema_name_suffix_by_var = var('schema_name_suffix', '') %}
    {% if schema_name_suffix_by_var %}
        {% set schema_name = schema_name + schema_name_suffix_by_var %}
    {% endif %}

    {% do return(schema_name) %}
{%- endmacro %}
