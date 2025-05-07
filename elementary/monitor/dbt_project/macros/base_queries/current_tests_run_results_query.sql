{% macro current_tests_run_results_query(days_back = none, invocation_id = none) %}
    with elementary_test_results as (
        select * from {{ ref('elementary', 'elementary_test_results') }}
        {% if days_back %}
            where {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('detected_at'), elementary.edr_current_timestamp(), 'day') }} < {{ days_back }}
        {% endif %}
    ),

    dbt_run_results as (
        select * from {{ ref('elementary', 'dbt_run_results') }}
        {% if days_back %}
            where {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('execute_completed_at'), elementary.edr_current_timestamp(), 'day') }} < {{ days_back }}
        {% endif %}
    ),

    dbt_tests as (
        select * from {{ ref('elementary', 'dbt_tests') }}
    ),

    dbt_models as (
        select * from {{ ref('elementary', 'dbt_models') }}
    ),

    dbt_sources as (
        select * from {{ ref('elementary', 'dbt_sources') }}
    ),

    dbt_artifacts as (
        select unique_id, meta, tags, owner from dbt_models
        union all
        select unique_id, meta, tags, owner from dbt_sources
    ),

    first_time_test_occurred as (
        select 
            min(detected_at) as first_time_occurred,
            test_unique_id
        from elementary_test_results
        group by test_unique_id
    )

    select
        elementary_test_results.id,
        {#
            Due to inconsistency test sub type and column name in some of elementary's tests
            we need to calculate different elementary unique id which is used to identify between different tests.
        #}
        case
            when elementary_test_results.test_type = 'schema_change' then elementary_test_results.test_unique_id
            {# In old versions of elementary, elementary_test_results doesn't contain test_short_name, so we use dbt_test short_name. #}
            when dbt_tests.short_name = 'dimension_anomalies' then elementary_test_results.test_unique_id
            else coalesce(elementary_test_results.test_unique_id, 'None') || '.' || coalesce(nullif(elementary_test_results.column_name, ''), 'None') || '.' || coalesce(elementary_test_results.test_sub_type, 'None')
        end as elementary_unique_id,
        elementary_test_results.invocation_id,
        elementary_test_results.data_issue_id,
        elementary_test_results.test_execution_id,
        elementary_test_results.test_unique_id,
        elementary_test_results.model_unique_id,
        elementary_test_results.detected_at,
        elementary_test_results.database_name,
        elementary_test_results.schema_name,
        elementary_test_results.table_name,
        elementary_test_results.column_name,
        elementary_test_results.test_type,
        elementary_test_results.test_sub_type,
        elementary_test_results.test_results_description,
        elementary_test_results.owners,
        elementary_test_results.tags,
        elementary_test_results.test_results_query,
        elementary_test_results.other,
        case
            when dbt_tests.short_name is not null then dbt_tests.short_name
            else elementary_test_results.test_name
        end as test_name,
        elementary_test_results.test_params,
        elementary_test_results.severity,
        elementary_test_results.status,
        {# In old versions of elementary, elementary_test_results doesn't contain test_short_name, so we use dbt_test short_name. #}
        dbt_tests.short_name,
        elementary_test_results.test_alias,
        elementary_test_results.failures,
        elementary_test_results.result_rows,
        dbt_tests.original_path,
        dbt_tests.meta,
        dbt_tests.description as test_description,
        dbt_tests.package_name,
        dbt_tests.tags as test_tags,
        dbt_artifacts.meta as model_meta,
        dbt_artifacts.tags as model_tags,
        dbt_artifacts.owner as model_owner,
        first_occurred.first_time_occurred as test_created_at,
        dbt_run_results.execution_time as execution_time
    from elementary_test_results
    join dbt_tests on elementary_test_results.test_unique_id = dbt_tests.unique_id
    left join first_time_test_occurred first_occurred on elementary_test_results.test_unique_id = first_occurred.test_unique_id
    left join dbt_artifacts on elementary_test_results.model_unique_id = dbt_artifacts.unique_id
    left join dbt_run_results on elementary_test_results.test_execution_id = dbt_run_results.model_execution_id
{% endmacro %}
