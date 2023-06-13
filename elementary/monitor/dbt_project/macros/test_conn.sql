{% macro test_conn() %}
    {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
    {% set elementary_model_relation = api.Relation.create(elementary_database, elementary_schema, "dbt_models") %}
    {% set query %}
        select 1 from {{ elementary_model_relation }}
    {% endset %}
    {% do elementary.run_query(query) %}
{% endmacro %}
