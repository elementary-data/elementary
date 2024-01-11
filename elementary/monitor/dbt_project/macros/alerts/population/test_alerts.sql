{% macro populate_test_alerts_query() %}
    with elementary_test_results as (
        select * from {{ ref('elementary_test_results') }}
    ),

    failed_tests as (
        select id as alert_id,
               data_issue_id,
               test_execution_id,
               test_unique_id,
               model_unique_id,
               detected_at,
               database_name,
               schema_name,
               table_name,
               column_name,
               test_type as alert_type,
               test_sub_type as sub_type,
               test_results_description as alert_description,
               owners,
               tags,
               test_results_query as alert_results_query,
               other,
               test_name,
               test_short_name,
               test_params,
               severity,
               status,
               result_rows
            from elementary_test_results
            where lower(status) != 'pass' and test_type = 'dbt_test'
    )

    select
        alert_id,
        data_issue_id,
        test_execution_id,
        test_unique_id,
        model_unique_id,
        database_name,
        detected_at,
        schema_name,
        table_name,
        column_name,
        test_type as alert_type,
        test_sub_type as sub_type,
        test_results_description as alert_description,
        owners,
        tags,
        test_results_query as alert_results_query,
        other,
        test_name,
        test_short_name,
        test_params,
        severity,
        status,
        result_rows
    from failed_tests
{% endmacro %}
