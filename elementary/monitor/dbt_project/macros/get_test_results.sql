{# 
    In the future we will want to merge between "get_test_results" and "get_tests_sample_data"
    So we currently have a small duplication in "latest_tests_in_the_last_chosen_days" for simplicity 
#}
{%- macro get_test_results(days_back = 7, results_sample_limit = 5, invocation_id = none) -%}
    {% set select_test_results %}
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
        ),

        latest_tests_in_the_last_chosen_days as (
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
        )

        select 
            id,
            model_unique_id,
            test_unique_id,
            elementary_unique_id,
            detected_at,
            database_name,
            schema_name,
            table_name,
            column_name,
            test_type,
            test_sub_type,
            test_results_description,
            owners,
            tags,
            meta,
            test_results_query,
            other,
            test_name,
            test_params,
            severity,
            status,
            days_diff
        from latest_tests_in_the_last_chosen_days
    {%- endset -%}
    {% set test_results_agate = run_query(select_test_results) %}
    {% set test_result_dicts_json = elementary.agate_to_json(test_results_agate) %}
    {% do elementary.edr_log(test_result_dicts_json) %}
{%- endmacro -%}

