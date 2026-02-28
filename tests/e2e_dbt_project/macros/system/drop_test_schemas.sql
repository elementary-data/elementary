{% macro drop_test_schemas() %}
    {# Drop both the main test schema and the elementary schema used by the CLI.
       The schema names are derived from the profile's target schema. #}
    {% set main_schema = target.schema %}
    {% set elementary_schema = main_schema ~ '_elementary' %}

    {% do elementary_integration_tests.edr_drop_schema(elementary_schema) %}
    {% do elementary_integration_tests.edr_drop_schema(main_schema) %}
    {% do log("Dropped schemas: " ~ main_schema ~ ", " ~ elementary_schema, info=true) %}
{% endmacro %}

{% macro edr_drop_schema(schema_name) %}
    {% do return(adapter.dispatch('edr_drop_schema', 'elementary_integration_tests')(schema_name)) %}
{% endmacro %}

{% macro default__edr_drop_schema(schema_name) %}
    {% set schema_relation = api.Relation.create(database=target.database, schema=schema_name) %}
    {% do dbt.drop_schema(schema_relation) %}
    {% do adapter.commit() %}
{% endmacro %}

{% macro bigquery__edr_drop_schema(schema_name) %}
    {% set schema_relation = api.Relation.create(database=target.database, schema=schema_name) %}
    {% do dbt.drop_schema(schema_relation) %}
{% endmacro %}

{% macro clickhouse__edr_drop_schema(schema_name) %}
    {% do run_query("DROP DATABASE IF EXISTS " ~ schema_name) %}
    {% do adapter.commit() %}
{% endmacro %}

{% macro athena__edr_drop_schema(schema_name) %}
    {% set schema_relation = api.Relation.create(database=target.database, schema=schema_name) %}
    {% do dbt.drop_schema(schema_relation) %}
{% endmacro %}
