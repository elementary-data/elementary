{% macro get_columns() %}
    {% set query %}
        with columns as (
            select distinct
                lower(database_name) as database_name,
                lower(schema_name) as schema_name,
                lower(table_name) as table_name,
                lower(column_name) as column_name,
                data_type
            from {{ ref("elementary", "dbt_columns") }}
        ),

        sources as (
            select
                unique_id,
                lower(database_name) as database_name,
                lower(schema_name) as schema_name,
                lower(name) as table_name
            from {{ ref("elementary", "dbt_sources") }}
        ),

        models as (
            select
                unique_id,
                lower(database_name) as database_name,
                lower(schema_name) as schema_name,
                lower(name) as table_name
            from {{ ref("elementary", "dbt_models") }}
        ),

        tables as (
            select unique_id, database_name, schema_name, table_name
            from models
            union all
            select unique_id, database_name, schema_name, table_name
            from sources
        )

        select
            unique_id,
            column_name,
            data_type
        from columns
        join tables using (database_name, schema_name, table_name)
    {% endset %}

    {% set result = elementary.run_query(query) %}
    {% do return(elementary.agate_to_dicts(result)) %}
{% endmacro %}
