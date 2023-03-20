{% macro test_conn() %}
    {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
    {% set elementary_model_relation = adapter.get_relation(elementary_database, elementary_schema, "dbt_models") %}
    {% if not elementary_model_relation %}
        {% do exceptions.raise_compiler_error("Elementary model not found in schema.") %}
    {% endif %}
{% endmacro %}
