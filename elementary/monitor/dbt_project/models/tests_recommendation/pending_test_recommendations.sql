{% set recommended_tests = ["volume_anomalies", "freshness_anomalies"] %}

with
    tables_criticality as (
        select
            id,
            lower(database_name) as database_name,
            lower(schema_name) as schema_name,
            lower(table_name) as table_name,
            resource_name,
            source_name,
            tags,
            owner,
            depends_on_count,
            dependant_on_count,
            exposure_count,
            table_type
        from {{ ref("tables_criticality") }}
    ),

    potentinal_recommended_tests as (
        select id, database_name, schema_name, table_name, short_name
        from tables_criticality
        -- This is probably not warehouse agnostic.
        cross join
            (
                select
                    unnest(array['{{ recommended_tests|join("', '") }}']) as short_name
            ) rt
    ),

    existing_recommended_tests as (
        select parent_model_unique_id, short_name
        from {{ ref("elementary", "dbt_tests") }}
        where test_namespace = 'elementary'
    ),

    pending_recommended_tests as (
        select id, short_name
        from potentinal_recommended_tests
        where
            (id, short_name) not in (
                select parent_model_unique_id, short_name
                from existing_recommended_tests
            )
    ),

    timestamp_columns as (
        select database_name, schema_name, table_name, timestamp_column
        from {{ ref("table_timestamp_columns") }}
    ),

    pending_tests_with_table_info as (
        select
            resource_name,
            source_name,
            short_name as test_name,
            timestamp_column,
            tags,
            owner,
            depends_on_count,
            dependant_on_count,
            exposure_count,
            table_type
        from pending_recommended_tests
        join tables_criticality using (id)
        left join timestamp_columns using (database_name, schema_name, table_name)
    )

select *
from pending_tests_with_table_info
