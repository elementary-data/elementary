{% macro dbt_tests_with_new_unique_id_query() %}
    with dbt_tests as (
        select * from {{ ref('elementary', 'dbt_tests') }}
    ),
    
    dbt_tests_with_elementary_unique_id as (
        select 
            case 
                when (alias = name and alias is not null and test_column_name is not null and short_name is not null) then test_column_name || '.' || alias || '.' || short_name
                when (alias = name and alias is not null and short_name is not null) then alias || '.' || short_name
                else unique_id
            end as elementary_unique_id,
            unique_id,
            database_name,
            schema_name,
            name,
            short_name,
            alias,
            test_column_name,
            severity,
            warn_if,
            error_if,
            test_params,
            test_namespace,
            tags,
            model_tags,
            model_owners,
            meta,
            depends_on_macros,
            depends_on_nodes,
            parent_model_unique_id,
            description,
            package_name,
            type,
            original_path,
            compiled_sql,
            path,
            generated_at   
        from dbt_tests
    ),
    
    dbt_tests_with_same_name_count as (
        select elementary_unique_id, count(*) as tests_name_count
        from dbt_tests_with_elementary_unique_id
        group by elementary_unique_id
    )

    select 
        case
            when counter.tests_name_count = 1 then tests.elementary_unique_id
            else tests.unique_id
        end as unique_id,
        tests.database_name,
        tests.schema_name,
        tests.name,
        tests.short_name,
        tests.alias,
        tests.test_column_name,
        tests.severity,
        tests.warn_if,
        tests.error_if,
        tests.test_params,
        tests.test_namespace,
        tests.tags,
        tests.model_tags,
        tests.model_owners,
        tests.meta,
        tests.depends_on_macros,
        tests.depends_on_nodes,
        tests.parent_model_unique_id,
        tests.description,
        tests.package_name,
        tests.type,
        tests.original_path,
        tests.compiled_sql,
        tests.path,
        tests.generated_at
    from dbt_tests_with_elementary_unique_id tests
    join dbt_tests_with_same_name_count counter on tests.elementary_unique_id = counter.elementary_unique_id
{% endmacro %}