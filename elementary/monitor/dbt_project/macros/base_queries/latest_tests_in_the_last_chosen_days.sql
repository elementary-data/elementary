{% macro latest_tests_in_the_last_chosen_days(days_back = none, invocation_id = none) %}
    with test_results as (
        {{ elementary_internal.current_tests_run_results_query(days_back=days_back, invocation_id=invocation_id) }}
    ),

    test_latest_invocations as (
        select 
            test_unique_id,
            invocation_id
        from (
            select
                {# current_tests_run_results_query sets elementary_unique_id to be test_unique_id #}
                test_unique_id,
                invocation_id,
                row_number() over (partition by test_unique_id order by detected_at desc) as row_number
            from test_results
        )
        where row_number = 1
    )

    select 
        tests.*,
        {{ elementary.datediff(elementary.cast_as_timestamp('tests.detected_at'), elementary.current_timestamp(), 'day') }} as days_diff
    from test_results tests
    join test_latest_invocations latest_invocations
    {#
        Elementary tests has different test_sub_type and column_name depends on the status of the test runs,
        which causing duplicate rows on the UI due to different keys in the partition at "tests_in_last_chosen_days".
        We join between the "test_unqiue_id" to the latest test run invocation to make sure we only query the real latest test runs for each test.
    #}
    on tests.test_unique_id = latest_invocations.test_unique_id and tests.invocation_id = latest_invocations.invocation_id
{% endmacro %}