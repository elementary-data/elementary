{% macro clear_env() %}
    {% do elementary_tests.edr_drop_schema(elementary.target_database(), generate_schema_name()) %}
    {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
    {% do elementary_tests.edr_drop_schema(database_name, schema_name) %}
{% endmacro %}

{% macro edr_drop_schema(database_name, schema_name) %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name) %}
    {% do dbt.drop_schema(schema_relation) %}
    {% do adapter.commit() %}
{% endmacro %}
