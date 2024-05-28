{# Object structure is [test_namespace, test_name] #}
{% set recommended_tests = [
    ("elementary", "volume_anomalies"),
    ("elementary", "freshness_anomalies"),
    ("elementary", "schema_changes_from_baseline"),
] %}

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

    potential_recommended_tests as (
        select id, test_namespace, short_name
        from tables_criticality
        cross join
            (
                {% for recommended_test in recommended_tests %}
                    select
                        '{{ recommended_test[0] }}' as test_namespace,
                        '{{ recommended_test[1] }}' as short_name
                    {% if not loop.last %}
                        union all
                    {% endif %}
                {% endfor %}
            ) rt
    ),

    existing_recommended_tests as (
        select parent_model_unique_id, test_namespace, short_name
        from {{ ref("elementary", "dbt_tests") }}
    ),

    pending_recommended_tests as (
        select id, test_namespace, short_name
        from potential_recommended_tests
        where
            (id, test_namespace, short_name) not in (
                select parent_model_unique_id, test_namespace, short_name
                from existing_recommended_tests
            )
    ),

    timestamp_columns as (
        select database_name, schema_name, table_name, timestamp_column
        from {{ ref("table_timestamp_columns") }}
    ),

    table_columns as (
        select
            lower(database_name) as database_name,
            lower(schema_name) as schema_name,
            lower(table_name) as table_name,
            json_agg(json_build_object('name', lower(column_name), 'data_type', lower(data_type))) as columns
        from {{ elementary.get_elementary_relation('information_schema_columns') }}
        group by 1, 2, 3
    ),

    pending_tests_with_table_info as (
        select
            resource_name,
            source_name,
            test_namespace,
            short_name as test_name,
            tags,
            owner,
            depends_on_count,
            dependant_on_count,
            exposure_count,
            table_type,
            case
                when short_name in ('volume_anomalies', 'freshness_anomalies') and timestamp_column is not null
                then jsonb_build_object('timestamp_column', timestamp_column)
            end as test_args,
            case
                when short_name = 'schema_changes_from_baseline'
                then jsonb_build_object('columns', table_columns.columns)
            end as table_args
        from pending_recommended_tests
        join tables_criticality using (id)
        left join timestamp_columns using (database_name, schema_name, table_name)
        left join table_columns using (database_name, schema_name, table_name)
        where
        short_name = 'volume_anomalies'
        or
        (short_name = 'freshness_anomalies' and timestamp_column is not null)
        or
        (short_name = 'schema_changes_from_baseline' and table_columns.columns is not null)
    )

select *
from pending_tests_with_table_info
