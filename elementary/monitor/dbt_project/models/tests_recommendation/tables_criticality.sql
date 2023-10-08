{% set exposures_relation = ref('elementary_cli', 'enriched_exposures') %}
{% if not exposures_relation %}
    {% set exposures_relation = ref("elementary", "dbt_exposures") %}
{% endif %}

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
            cast(tags as jsonb) as tags,
            cast(owner as jsonb) as owner,
            cast(depends_on_nodes as jsonb) as depends_on
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
            cast(tags as jsonb) as tags,
            cast(owner as jsonb) as owner,
            cast('[]' as jsonb) as depends_on
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
        from tables_information t1
        join tables_information t2 on t2.depends_on ? t1.id
        group by t1.id
    ),

    exposure_counts as (
        select t.id, count(*) as exposure_count
        from tables_information t
        join
            {{ exposures_relation }} e
            on e.depends_on_nodes::jsonb ? t.id
        group by t.id
    ),

    tables as (
        select
            tables_information.*,
            jsonb_array_length(tables_information.depends_on) as depends_on_count,
            coalesce(dependant_on_counts.dependant_on_count, 0) as dependant_on_count,
            coalesce(exposure_counts.exposure_count, 0) as exposure_count
        from tables_information
        left join dependant_on_counts on tables_information.id = dependant_on_counts.id
        left join exposure_counts on tables_information.id = exposure_counts.id
    )

select *
from tables
