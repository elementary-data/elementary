{%- macro get_tests_invocations(invocations_per_test = 720, days_back = 7) -%}
    {% set tests_invocations_query %}
        with test_results as (
            {{ elementary_internal.current_tests_run_results_query(days_back=days_back) }}
        ),

        test_latest_detected_at_time as (
            select
                test_unique_id,
                max(detected_at) as last_detected_at
            from test_results
            group by test_unique_id
        ),

        test_results_in_last_chosen_days as (
            select *,
                row_number() over (partition by model_unique_id, test_unique_id, test_sub_type, column_name order by detected_at desc) as row_number
            from test_results
        )

        select
            tests.model_unique_id, 
            tests.test_unique_id,
            tests.test_sub_type,
            tests.column_name,
            tests.detected_at,
            tests.test_results_description,
            tests.test_execution_id,
            tests.status
        from test_results_in_last_chosen_days tests
        join test_latest_detected_at_time latest_runs
        on tests.test_unique_id = latest_runs.test_unique_id and tests.detected_at = latest_runs.last_detected_at
        where tests.row_number <= {{ invocations_per_test }}
        order by tests.detected_at
    {% endset %}
    {% set tests_invocations_agate = run_query(tests_invocations_query) %}
    {% set tests_invocations_results = elementary.agate_to_json(tests_invocations_agate) %}
    {% do elementary.edr_log(tests_invocations_results) %}
{%- endmacro -%}