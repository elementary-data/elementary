{% set timestamp_column_names = [
    "updated_at",
    "created_at",
    "_fivetran_synced",
    "_airbyte_emitted_at",
    "create_date",
    "created",
    "db_insert_time",
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
    "_ingestion_time",
    "last_modified_datetime",
] %}
{% set joined_timestamp_column_names = "'{}'".format(
    "', '".join(timestamp_column_names)
) %}


with
    columns as (
        select distinct
            lower(database_name) as database_name,
            lower(schema_name) as schema_name,
            lower(table_name) as table_name,
            lower(column_name) as column_name
        from {{ ref("elementary", "dbt_columns") }}
    ),

    inferred_timestamp_columns as (
        select
            database_name,
            schema_name,
            table_name,
            column_name,
            timestamp_column_names.confidence
        from columns
        join
            (
                values
                    {% for timestamp_column_name in timestamp_column_names %}
                        ('{{ timestamp_column_name }}', {{ loop.index }})
                        {% if not loop.last %},{% endif %}
                    {% endfor %}
            ) as timestamp_column_names(column_name, confidence) using (column_name)
    ),

    source_provided_timestamp_columns as (
        select
            lower(database_name) as database_name,
            lower(schema_name) as schema_name,
            lower(name) as table_name,
            lower(loaded_at_field) as column_name
        from {{ ref("elementary", "dbt_sources") }}
        where loaded_at_field is not null
    ),

    absolute_rated_timestamp_columns as (
        -- Combine inferred and source-provided timestamp columns,
        -- giving priority to source-provided ones.
        select
            database_name,
            schema_name,
            table_name,
            column_name,
            case
                when source.column_name is not null then 0 else inferred.confidence
            end as absolute_confidence
        from inferred_timestamp_columns inferred
        full outer join
            source_provided_timestamp_columns source using (
                database_name, schema_name, table_name, column_name
            )
    ),

    relative_rated_timestamp_columns as (
        select
            database_name,
            schema_name,
            table_name,
            column_name,
            row_number() over (
                partition by database_name, schema_name, table_name
                order by absolute_confidence
            ) as relative_confidence
        from absolute_rated_timestamp_columns
    ),

    best_rated_timestamp_columns as (
        select database_name, schema_name, table_name, column_name
        from relative_rated_timestamp_columns
        where relative_confidence = 1
    )

select database_name, schema_name, table_name, column_name as timestamp_column
from best_rated_timestamp_columns
