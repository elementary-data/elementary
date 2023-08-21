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

    -- Inferring the timestamp column based on their names and assigning a confidence score.
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

    -- Users can provide the timestamp columns for their sources,
    -- if provided, we assign a confidence score of 0 (certain).
    source_provided_timestamp_columns as (
        select
            lower(database_name) as database_name,
            lower(schema_name) as schema_name,
            lower(name) as table_name,
            lower(loaded_at_field) as column_name
        from {{ ref("elementary", "dbt_sources") }}
        where loaded_at_field is not null
    ),

    -- Combining the inferred and source provided timestamp columns.
    absolute_rated_timestamp_columns as (
        select
            database_name,
            schema_name,
            table_name,
            column_name,
            inferred.confidence as absolute_confidence
        from inferred_timestamp_columns inferred
        union all
        select
            database_name,
            schema_name,
            table_name,
            column_name,
            0 as absolute_confidence
        from source_provided_timestamp_columns
    ),

    -- Sort the timestamp columns by confidence and assign a rank.
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

    -- Select the timestamp columns with the highest confidence.
    best_rated_timestamp_columns as (
        select database_name, schema_name, table_name, column_name
        from relative_rated_timestamp_columns
        where relative_confidence = 1
    )

select database_name, schema_name, table_name, column_name as timestamp_column
from best_rated_timestamp_columns
