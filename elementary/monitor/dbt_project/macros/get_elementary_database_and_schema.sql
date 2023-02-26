{% macro get_elementary_database_and_schema() %}
    {% set database, schema = elementary.target_database(), target.schema %}
    {% if database %}
        {% do return(database ~ '.' ~ schema) %}
    {% else %}
        {% do return(schema) %}
    {% endif %}
{% endmacro %}
