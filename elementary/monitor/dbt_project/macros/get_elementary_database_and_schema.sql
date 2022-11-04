{% macro get_elementary_database_and_schema() %}
    {% set database, schema = elementary.target_database(), target.schema %}
    {% if database %}
        {{ elementary.edr_log(database ~ '.' ~ schema) }}
    {% else %}
        {{ elementary.edr_log(schema) }}
    {% endif %}
{% endmacro %}
