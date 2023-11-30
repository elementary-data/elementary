{% macro test_conn() %}
    {% do return([target.type, adapter.dispatch('test_conn', 'elementary_cli')()]) %}
{%- endmacro %}

{% macro default__test_conn() %}
    {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
    {% set elementary_model_relation = api.Relation.create(elementary_database, elementary_schema, "dbt_models") %}
    {% set query %}
        select * from {{ elementary_model_relation }} limit 10
    {% endset %}
    {% do elementary.run_query(query) %}
{% endmacro %}

{% macro snowflake__test_conn() %}
    {% set current_warehouse_query %}
        select current_warehouse() as current_warehouse
    {% endset %}
    {% set current_warehouse = elementary.agate_to_dicts(elementary.run_query(current_warehouse_query))[0].get("current_warehouse") %}
    {% if not current_warehouse %}
        {{ exceptions.raise_compiler_error("Could not access current warehouse. Permission is missing.") }}
    {% endif %}
    {% do elementary_cli.default__test_conn() %}
{% endmacro %}
