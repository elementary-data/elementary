{% set timestamp_column_names = [
    "updated_at",
    "created_at",
    "_fivetran_synced",
    "_airbyte_emitted_at",
    "create_date",
    "created",
    "db_insert_time",
    "last_modified_datetime",
    "create_ts",
    "created_ts",
    "update_ts",
    "updated_ts",
    "load_ts",
    "loaded_at",
    "date_created",
    "dbt_updated_at",
    "update_datetime",
    "event_time",
    "event_date",
    "event_created_at",
    "event_updated_at",
    "event_event_time",
    "_etl_loaded_at",
    "__etl_loaded_at",
    "_etl_inserted_at",
] %}
{% set timestamp_like_column_names = [
    "%_updated_at",
    "%_created_at",
] %}
{% set timestamp_column_sql_expr_filter = "('{}')".format(
    "', '".join(timestamp_column_names)
) %}

{% set fqn_sql_expr = "database_name, schema_name, table_name" %}

with
    tables_criticality as (
        select *
        from {{ ref("tables_criticality") }}
    ),

    columns as (
        select distinct *
        from {{ ref("elementary", "dbt_columns") }}
    ),

    timestamp_columns as (
        select *
        from columns
        where
            lower(column_name) in {{ timestamp_column_sql_expr_filter }}
            {% for timestamp_like_column_name in timestamp_like_column_names %}
                or lower(column_name) like '{{ timestamp_like_column_name }}'
            {% endfor %}
            -- Sources in which the user already provided the the timestamp column.
            or ({{ fqn_sql_expr }}) in (
                select {{ fqn_sql_expr }}
                from {{ ref("elementary", "dbt_sources") }}
                where loaded_at_field is not null
            )
    )


select
    {{ fqn_sql_expr }},
    resource_name,
    source_name,
    depends_on_count,
    dependant_on_count,
    exposure_count,
    string_agg(column_name, ', ') as timestamp_columns
from timestamp_columns
join tables_criticality using ({{ fqn_sql_expr }})
group by
    {{ fqn_sql_expr }},
    resource_name,
    source_name,
    depends_on_count,
    dependant_on_count,
    exposure_count
