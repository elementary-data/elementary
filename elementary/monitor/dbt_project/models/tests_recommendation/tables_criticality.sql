with
    dbt_models_data as (
        select
            unique_id as id,
            database_name,
            schema_name,
            alias as table_name,
            name as resource_name,
            null as source_name,
            'model' as table_type,
            tags,
            owner,
            cast(depends_on_nodes as json) as depends_on
        from {{ ref("elementary", "dbt_models") }}
    ),

    dbt_sources_data as (
        select
            unique_id as id,
            database_name,
            schema_name,
            name as table_name,
            name as resource_name,
            source_name,
            'source' as table_type,
            tags,
            owner,
            cast('[]' as json) as depends_on
        from {{ ref("elementary", "dbt_sources") }}
    ),

    tables_information as (
        select *
        from dbt_models_data
        union all
        select *
        from dbt_sources_data
    ),

    dependant_on_counts as (
        select t1.id, count(*) as dependant_on_count
        from tables_information as t1
        join
            tables_information as t2
            on exists (
                select 1
                from json_array_elements(t2.depends_on) as t2_depends_on
                where t2_depends_on::text = concat('"', t1.id, '"')
            )
        group by t1.id
    ),

    exposure_counts as (
        select t.id, count(*) as exposure_count
        from tables_information as t
        join
            {{ ref("elementary", "dbt_exposures") }} as e
            on exists (
                select 1
                from
                    json_array_elements(e.depends_on_nodes::json) as exposure_depends_on
                where exposure_depends_on::text = concat('"', t.id, '"')
            )
        group by t.id
    ),

    tables as (
        select
            tables_information.*,
            json_array_length(tables_information.depends_on) as depends_on_count,
            coalesce(dependant_on_counts.dependant_on_count, 0) as dependant_on_count,
            coalesce(exposure_counts.exposure_count, 0) as exposure_count
        from tables_information
        left join dependant_on_counts on tables_information.id = dependant_on_counts.id
        left join exposure_counts on tables_information.id = exposure_counts.id
    )

select *
from tables
