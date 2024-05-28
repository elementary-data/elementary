{# Prioritization: 1. insertion time, 2. update time. #}
{% set timestamp_column_names = [
    "created_at",
    "created_at_utc",
    "inserted_at",
    "inserted_at_utc",
    "create_date",
    "created",
    "db_insert_time",
    "create_ts",
    "created_ts",
    "load_ts",
    "loaded_at",
    "date_created",
    "_etl_loaded_at",
    "__etl_loaded_at",
    "_etl_inserted_at",
    "_ingestion_time",
    "_fivetran_synced",
    "_airbyte_emitted_at",

    "updated_at",
    "updated_at_utc",
    "update_ts",
    "updated_ts",
    "dbt_updated_at",
    "update_datetime",
    "event_updated_at",
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
        from {{ elementary.get_elementary_relation('information_schema_columns') }}
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

    -- Users can provide the timestamp columns for their models,
    -- if provided, we assign a confidence score of 0 (certain).
    model_provided_timestamp_columns as (
        select
            lower(database_name) as database_name,
            lower(schema_name) as schema_name,
            lower(name) as table_name,
            bigquery_partition_by::json ->> 'field' as column_name
        from {{ ref("elementary", "dbt_models") }}
        where bigquery_partition_by::json ->> 'data_type' != 'int64'
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
        union all
        select
            database_name,
            schema_name,
            table_name,
            column_name,
            0 as absolute_confidence
        from model_provided_timestamp_columns
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
