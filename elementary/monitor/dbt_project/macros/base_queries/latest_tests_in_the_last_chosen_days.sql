{% macro latest_tests_in_the_last_chosen_days(days_back = none, invocation_id = none) %}
    with test_results as (
        {{ elementary_internal.current_tests_run_results_query(days_back=days_back, invocation_id=invocation_id) }}
    ),

    test_latest_detected_at_time as (
        select
            {# current_tests_run_results_query sets elementary_unique_id to be test_unique_id #}
            test_unique_id,
            max(detected_at) as last_detected_at
        from test_results
        group by test_unique_id
    ),

    tests_in_last_chosen_days as (
        select
            *,
            {{ elementary.datediff(elementary.cast_as_timestamp('detected_at'), elementary.current_timestamp(), 'day') }} as days_diff
        from test_results
    )

    select tests.* 
    from tests_in_last_chosen_days tests
    join test_latest_detected_at_time latest_runs
    {#
        Elementary tests has different test_sub_type and column_name depends on the status of the test runs,
        which causing duplicate rows on the UI due to different keys in the partition at "tests_in_last_chosen_days".
        We join between the "test_unqiue_id" to the latest test run to make sure we only query the real latest test runs for each test.
    #}
    on tests.test_unique_id = latest_runs.test_unique_id and tests.detected_at = latest_runs.last_detected_at
{% endmacro %}