{% macro get_elementary_database_and_schema() %}
    {# Use ref() to get the actual schema where elementary tables are created,
       accounting for any custom schema configuration in the user's dbt_project.yml #}
    {% set elementary_relation = ref('elementary', 'dbt_models') %}
    {% set database = elementary_relation.database %}
    {% set schema = elementary_relation.schema %}
    {% if database %}
        {% do return(database ~ '.' ~ schema) %}
    {% else %}
        {% do return(schema) %}
    {% endif %}
{% endmacro %}
